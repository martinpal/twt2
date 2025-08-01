package twt2

import (
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"

	twtproto "palecci.cz/twtproto"
)

const proxyID = 0
const chunkSize = 1280

var app *App

type Handler func(http.ResponseWriter, *http.Request)

type PoolConnection struct {
	Conn     net.Conn
	SendChan chan *twtproto.ProxyComm
	ID       uint64
	InUse    bool
	LastUsed time.Time
}

type Connection struct {
	Connection   net.Conn
	LastSeqIn    uint64                        // last sequence number we have used for the last incoming chunk
	NextSeqOut   uint64                        // next sequence number to be sent out
	MessageQueue map[uint64]twtproto.ProxyComm // queue of messages with too high sequence numbers
}

type App struct {
	ListenPort            int
	PeerHost              string
	PeerPort              int
	Ping                  bool
	DefaultRoute          Handler
	PoolConnections       []*PoolConnection
	PoolMutex             sync.Mutex
	PoolSize              int
	LocalConnectionMutex  sync.Mutex
	LastLocalConnection   uint64
	LocalConnections      map[uint64]Connection
	RemoteConnectionMutex sync.Mutex
	RemoteConnections     map[uint64]Connection
}

func GetApp() *App {
	return app
}

func NewApp(f Handler, listenPort int, peerHost string, peerPort int, poolInit int, poolCap int, ping bool, isClient bool) *App {
	app := &App{
		ListenPort:        listenPort,
		PeerHost:          peerHost,
		PeerPort:          peerPort,
		Ping:              ping,
		DefaultRoute:      f,
		PoolConnections:   make([]*PoolConnection, 0, poolCap),
		PoolSize:          poolCap,
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}

	// Only create pool connections on client side
	if isClient && peerHost != "" {
		log.Debug("Creating bidirectional connection pool (client side)")

		// Initialize pool connections
		for i := 0; i < poolInit; i++ {
			poolConn := createPoolConnection(uint64(i), app.PeerHost, app.PeerPort, ping)
			if poolConn != nil {
				app.PoolConnections = append(app.PoolConnections, poolConn)
			}
		}

		log.Debugf("Created %d pool connections", len(app.PoolConnections))
	} else {
		log.Debug("Server side - no pool connections created")
	}

	return app
}

func createPoolConnection(id uint64, host string, port int, ping bool) *PoolConnection {
	log.Tracef("Creating pool connection %d to %s:%d", id, host, port)
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		log.Errorf("Error creating pool connection %d to %s:%d: %v", id, host, port, err)
		return nil
	}

	poolConn := &PoolConnection{
		Conn:     conn,
		SendChan: make(chan *twtproto.ProxyComm, 100),
		ID:       id,
		InUse:    false,
		LastUsed: time.Now(),
	}

	// Start goroutines for this connection
	go poolConnectionSender(poolConn)
	go poolConnectionReceiver(poolConn)

	// Send initial ping if enabled
	if ping {
		pingMessage := &twtproto.ProxyComm{
			Mt:    twtproto.ProxyComm_PING,
			Proxy: proxyID,
		}
		poolConn.SendChan <- pingMessage
	}

	return poolConn
}

func poolConnectionSender(poolConn *PoolConnection) {
	log.Tracef("Starting sender goroutine for pool connection %d", poolConn.ID)
	for message := range poolConn.SendChan {
		sendProtobufToConn(poolConn.Conn, message)
	}
	log.Tracef("Sender goroutine for pool connection %d ended", poolConn.ID)
}

func poolConnectionReceiver(poolConn *PoolConnection) {
	log.Tracef("Starting receiver goroutine for pool connection %d", poolConn.ID)
	handleConnection(poolConn.Conn)
	log.Tracef("Receiver goroutine for pool connection %d ended", poolConn.ID)
}

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.DefaultRoute(w, r)
}

func Hijack(w http.ResponseWriter, r *http.Request) {
	parsedHost := strings.Split(r.Host, ":")
	host := parsedHost[0]
	port, _ := strconv.Atoi(parsedHost[1])
	log.Infof("Method %s, Host %s:%d\n", r.Method, host, port)
	w.WriteHeader(http.StatusOK)
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "webserver doesn't support hijacking", http.StatusInternalServerError)
		return
	}
	conn, bufrw, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	app.LocalConnectionMutex.Lock()
	thisConnection := app.LastLocalConnection
	app.LastLocalConnection++
	app.LocalConnections[thisConnection] = Connection{Connection: conn, LastSeqIn: 0, MessageQueue: make(map[uint64]twtproto.ProxyComm)}
	app.LocalConnectionMutex.Unlock()

	connectMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_OPEN_CONN,
		Proxy:      proxyID,
		Connection: thisConnection,
		Seq:        0,
		Address:    host,
		Port:       uint32(port),
	}
	sendProtobuf(connectMessage)

	for {
		b := make([]byte, chunkSize)
		n, err := bufrw.Read(b)
		if err != nil {
			log.Infof("Error reading local connection: %v, going to send CLOSE_CONN_S message to remote end", err)
			app.LocalConnectionMutex.Lock()
			connRecord := app.LocalConnections[thisConnection]
			connRecord.LastSeqIn++
			app.LocalConnections[thisConnection] = connRecord
			delete(app.LocalConnections, thisConnection)
			app.LocalConnectionMutex.Unlock()
			closeMessage := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_CLOSE_CONN_S,
				Proxy:      proxyID,
				Connection: thisConnection,
				Seq:        connRecord.LastSeqIn,
			}
			sendProtobuf(closeMessage)
			return
		}
		if n > 0 {
			log.Tracef("Read %d bytes, hexdump", n)
			//      log.Tracef("%s", hex.Dump(b[:n]))
			app.LocalConnectionMutex.Lock()
			connRecord := app.LocalConnections[thisConnection]
			connRecord.LastSeqIn++
			app.LocalConnections[thisConnection] = connRecord
			app.LocalConnectionMutex.Unlock()
			dataMessage := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_DATA_UP,
				Proxy:      proxyID,
				Connection: thisConnection,
				Seq:        connRecord.LastSeqIn,
				Data:       b[:n],
			}
			sendProtobuf(dataMessage)
		}
	}
	log.Infof("Close hijacked connection %4d", thisConnection)
}

func sendProtobufToConn(conn net.Conn, message *twtproto.ProxyComm) {
	log.Tracef("Marshalling message %v", message)
	data, err := proto.Marshal(message)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	length := len(data)
	size := make([]byte, 2)
	size[0] = byte(length & 255)
	size[1] = byte((length & 65535) >> 8)
	log.Tracef("Length %d 0x%x, bytes 0x%x 0x%x", length, length, size[0], size[1])
	//  log.Tracef("%s", hex.Dump(data))
	B := make([]byte, 0, 2+length)
	B = append(B, size...)
	B = append(B, data...)
	conn.Write(B)
}

func sendProtobuf(message *twtproto.ProxyComm) {
	app.PoolMutex.Lock()
	defer app.PoolMutex.Unlock()

	// Find an available pool connection
	var selectedConn *PoolConnection
	for _, poolConn := range app.PoolConnections {
		if !poolConn.InUse {
			selectedConn = poolConn
			break
		}
	}

	// If no connection available, use the least recently used one
	if selectedConn == nil && len(app.PoolConnections) > 0 {
		selectedConn = app.PoolConnections[0]
		for _, poolConn := range app.PoolConnections {
			if poolConn.LastUsed.Before(selectedConn.LastUsed) {
				selectedConn = poolConn
			}
		}
	}

	if selectedConn == nil {
		log.Errorf("No pool connections available")
		return
	}

	log.Tracef("Sending message via pool connection %d", selectedConn.ID)
	selectedConn.LastUsed = time.Now()

	// Send message through the channel (non-blocking)
	select {
	case selectedConn.SendChan <- message:
		log.Tracef("Message queued for pool connection %d", selectedConn.ID)
	default:
		log.Warnf("Send channel full for pool connection %d", selectedConn.ID)
	}
}

// protobuf server

func handleConnection(conn net.Conn) {
	for {
		l := make([]byte, 2)
		n, err := conn.Read(l)
		if err != nil {
			log.Infof("Error reading frame length: %v", err)
			return
		}
		length := int(l[1])*256 + int(l[0])
		log.Tracef("Expecting protobuf message long %d bytes", length)
		B := make([]byte, 0, length)
		b := make([]byte, length)
		for len(B) != length {
			n, err = conn.Read(b)
			if err != nil {
				log.Infof("Error reading data: %v", err)
				if err != io.EOF {
					fmt.Println("read error:", err)
				}
				break
			}
			B = append(B, b[:n]...)
			b = make([]byte, cap(B)-len(B))
		}
		if len(B) == length {
			log.Tracef("Protobuf read %d bytes, hexdump", len(B))
			//      log.Tracef("%s", hex.Dump(B))
			message := &twtproto.ProxyComm{}
			if err := proto.Unmarshal(B, message); err != nil {
				log.Warnf("Erroneous message hexdump length(%d): %s", length, hex.Dump(B))
				log.Errorf("Failed to parse message length(%d): %v", length, err)
				conn.Close()
				return
			}
			handleProxycommMessage(message)
		} else {
			log.Errorf("Error receiving protobuf message, expected %4d, got %4d", length, len(B))
		}
	}
}

func handleProxycommMessage(message *twtproto.ProxyComm) {
	log.Tracef("Received message: %v", message)
	if message.Mt == twtproto.ProxyComm_PING {
		log.Debugf("Received PING message from proxy %d", message.Proxy)
		return
	}
	log.Tracef("Processing queueing logic for connection %d", message.Connection)
	var mutex *sync.Mutex
	var connections *map[uint64]Connection
	switch message.Mt {
	case twtproto.ProxyComm_OPEN_CONN:
		mutex = &app.RemoteConnectionMutex
		connections = &app.RemoteConnections
		mutex.Lock()
	case twtproto.ProxyComm_CLOSE_CONN_C, twtproto.ProxyComm_DATA_DOWN:
		mutex = &app.LocalConnectionMutex
		connections = &app.LocalConnections
		mutex.Lock()
		thisConnection, ok := app.LocalConnections[message.Connection]
		if !ok {
			mutex.Unlock()
			log.Tracef("No such connection %d, seq %d", message.Connection, message.Seq)
			return
		}
		log.Tracef("Seq DOWN %d %d", message.Seq, thisConnection.NextSeqOut)
		if message.Seq != thisConnection.NextSeqOut {
			log.Tracef("Queueing message UP conn %d seq %d", message.Connection, message.Seq)
			thisConnection.MessageQueue[message.Seq] = *message
			mutex.Unlock()
			return
		}
	case twtproto.ProxyComm_CLOSE_CONN_S, twtproto.ProxyComm_DATA_UP:
		mutex = &app.RemoteConnectionMutex
		connections = &app.RemoteConnections
		mutex.Lock()
		thisConnection, ok := app.RemoteConnections[message.Connection]
		if !ok {
			log.Tracef("No such connection %d, seq %d, adding", message.Connection, message.Seq)
			app.RemoteConnections[message.Connection] = Connection{Connection: nil, LastSeqIn: 0, NextSeqOut: 0, MessageQueue: make(map[uint64]twtproto.ProxyComm)}
			thisConnection, _ = app.RemoteConnections[message.Connection]
		}
		log.Tracef("Seq UP %d %d", message.Seq, thisConnection.NextSeqOut)
		if message.Seq != thisConnection.NextSeqOut {
			if message.Mt == twtproto.ProxyComm_CLOSE_CONN_S {
				log.Tracef("Out of order processing of %v message for connection %d seq %d", message.Mt, message.Connection, message.Seq)
				closeConnectionRemote(message)
				mutex.Unlock()
				return
			}
			log.Tracef("Queueing message UP conn %d seq %d", message.Connection, message.Seq)
			thisConnection.MessageQueue[message.Seq] = *message
			mutex.Unlock()
			return
		}
	}
	log.Tracef("Handling current message for connection %d", message.Connection)
	switch message.Mt {
	case twtproto.ProxyComm_OPEN_CONN:
		newConnection(message)
	case twtproto.ProxyComm_CLOSE_CONN_C:
		closeConnectionLocal(message)
	case twtproto.ProxyComm_DATA_DOWN:
		backwardDataChunk(message)
	case twtproto.ProxyComm_CLOSE_CONN_S:
		closeConnectionRemote(message)
	case twtproto.ProxyComm_DATA_UP:
		forwardDataChunk(message)
	}
	log.Debugf("Processing message queue for connection %d", message.Connection)
	thisConnection := (*connections)[message.Connection]
	seq := thisConnection.NextSeqOut
	log.Tracef("Next seq: %d", seq)
	//  log.Tracef("Queue %v", (*connections)[message.Connection].MessageQueue)
	for queueMessage, ok := thisConnection.MessageQueue[seq]; ok; {
		thisConnection = (*connections)[queueMessage.Connection]
		log.Debugf("Processing message queue for connection %d, seq %d", queueMessage.Connection, seq)
		//    log.Tracef("Message: %v", queueMessage)
		switch queueMessage.Mt {
		case twtproto.ProxyComm_CLOSE_CONN_C:
			closeConnectionLocal(&queueMessage)
		case twtproto.ProxyComm_DATA_DOWN:
			backwardDataChunk(&queueMessage)
		case twtproto.ProxyComm_CLOSE_CONN_S:
			closeConnectionRemote(&queueMessage)
		case twtproto.ProxyComm_DATA_UP:
			forwardDataChunk(&queueMessage)
		}
		delete((*connections)[queueMessage.Connection].MessageQueue, seq)
		seq++
		queueMessage, ok = thisConnection.MessageQueue[seq]
	}
	mutex.Unlock()
}

func newConnection(message *twtproto.ProxyComm) {
	log.Infof("Openning connection %4d to %s:%d", message.Connection, message.Address, message.Port)
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", message.Address, message.Port))
	if err != nil {
		log.Fatal(err)
		return
	}
	thisConnection, ok := app.RemoteConnections[message.Connection]
	if !ok {
		log.Tracef("Connection %4d not known, creating record", message.Connection)
		app.RemoteConnections[message.Connection] = Connection{Connection: conn, LastSeqIn: 0, NextSeqOut: 1, MessageQueue: make(map[uint64]twtproto.ProxyComm)}
	} else {
		log.Tracef("Connection %4d record already exists, setting conn field to newly dialed connection", message.Connection)
		thisConnection.Connection = conn
		thisConnection.NextSeqOut = 1
		app.RemoteConnections[message.Connection] = thisConnection
	}
	go handleRemoteSideConnection(conn, message.Connection)
}

func closeConnectionRemote(message *twtproto.ProxyComm) {
	log.Debugf("Closing remote connection %4d", message.Connection)
	connRecord, ok := app.RemoteConnections[message.Connection]
	if ok {
		conn := connRecord.Connection
		delete(app.RemoteConnections, message.Connection)
		time.AfterFunc(1*time.Second, func() {
			if conn != nil {
				conn.Close()
			}
		})
	}
}

func closeConnectionLocal(message *twtproto.ProxyComm) {
	log.Debugf("Closing local connection %4d", message.Connection)
	connRecord, ok := app.LocalConnections[message.Connection]
	if ok {
		conn := connRecord.Connection
		delete(app.LocalConnections, message.Connection)
		time.AfterFunc(1*time.Second, func() { conn.Close() })
	}
}

func backwardDataChunk(message *twtproto.ProxyComm) {
	//  log.Tracef("DATA_DOWN %v", message)
	thisConnection := app.LocalConnections[message.Connection]
	thisConnection.NextSeqOut++
	app.LocalConnections[message.Connection] = thisConnection
	n, err := thisConnection.Connection.Write(message.Data)
	if err != nil {
		log.Debugf("Error forwarding data chunk downward for connection %4d, seq %8d, length %5d, %v", message.Connection, message.Seq, len(message.Data), err)
		return
	}
	log.Debugf("Succesfully forwarded data chunk downward for connection %4d, seq %8d, length %5d, sent %5d", message.Connection, message.Seq, len(message.Data), n)
}

func forwardDataChunk(message *twtproto.ProxyComm) {
	thisConnection := app.RemoteConnections[message.Connection]
	thisConnection.NextSeqOut++
	app.RemoteConnections[message.Connection] = thisConnection
	n, err := thisConnection.Connection.Write(message.Data)
	if err != nil {
		log.Debugf("Error forwarding data chunk   upward for connection %4d, seq %8d, length %5d, %v", message.Connection, message.Seq, len(message.Data), err)
		return
	}
	log.Debugf("Succesfully forwarded data chunk   upward for connection %4d, seq %8d, length %5d, sent %5d", message.Connection, message.Seq, len(message.Data), n)
}

func handleRemoteSideConnection(conn net.Conn, connID uint64) {
	log.Infof("Starting remote side connection handler for connection %d", connID)
	b := make([]byte, chunkSize)
	for {
		n, err := conn.Read(b)
		if err != nil {
			if err == io.EOF {
				log.Infof("Error reading remote connection %d: %v", connID, err)
				app.RemoteConnectionMutex.Lock()
				connRecord, ok := app.RemoteConnections[connID]
				if !ok {
					log.Tracef("Connection %d was already closed and removed earlier, exiting goroutine", connID)
					app.RemoteConnectionMutex.Unlock()
					return
				}
				seq := connRecord.LastSeqIn
				delete(app.RemoteConnections, connID)
				app.RemoteConnectionMutex.Unlock()
				closeMessage := &twtproto.ProxyComm{
					Mt:         twtproto.ProxyComm_CLOSE_CONN_C,
					Proxy:      proxyID,
					Connection: connID,
					Seq:        seq,
				}
				sendProtobuf(closeMessage)
				return
			}
			log.Tracef("Error reading remote connection %d: %v, exiting goroutine", connID, err)
			app.RemoteConnectionMutex.Lock()
			delete(app.RemoteConnections, connID)
			app.RemoteConnectionMutex.Unlock()
			conn.Close()
			return
		}
		log.Tracef("Sending data from remote connection %4d downward, length %5d", connID, n)
		app.RemoteConnectionMutex.Lock()
		connRecord := app.RemoteConnections[connID]
		seq := connRecord.LastSeqIn
		connRecord.LastSeqIn++
		app.RemoteConnections[connID] = connRecord
		app.RemoteConnectionMutex.Unlock()
		//    log.Tracef("%s", hex.Dump(b[:n]))
		dataMessage := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Proxy:      proxyID,
			Connection: connID,
			Seq:        seq,
			Data:       b[:n],
		}
		sendProtobuf(dataMessage)
	}
}

func ProtobufServer(listenPort int) {
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", listenPort))
	if err != nil {
		log.Fatalf("Error listening: %s", err.Error())
	}
	defer l.Close()
	log.Infof("Listening on 0.0.0.0:%d", listenPort)
	for {
		log.Trace("Listening for an incoming connection")
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

// FIXME Delete this!
func tf(a int) int {
	return 2 * a
}
