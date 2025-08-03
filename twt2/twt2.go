package twt2

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	"google.golang.org/protobuf/proto"
	"palecci.cz/twtproto"
)

var log = logrus.StandardLogger()

const proxyID = 0
const chunkSize = 1280

var app *App

type Handler func(http.ResponseWriter, *http.Request)

type PoolConnection struct {
	Conn            net.Conn
	SendChan        chan *twtproto.ProxyComm
	ID              uint64
	InUse           bool
	LastUsed        time.Time
	SSHClient       *ssh.Client        // SSH client for this connection
	SSHConn         net.Conn           // SSH connection
	LocalPort       int                // Local port for SSH tunnel
	keepAliveCancel context.CancelFunc // Cancel function for SSH keep-alive goroutine
	keepAliveMutex  sync.Mutex         // Mutex to protect keep-alive operations
}

type Connection struct {
	Connection   net.Conn
	LastSeqIn    uint64                         // last sequence number we have used for the last incoming chunk
	NextSeqOut   uint64                         // next sequence number to be sent out
	MessageQueue map[uint64]*twtproto.ProxyComm // queue of messages with too high sequence numbers
}

type App struct {
	ListenPort            int
	PeerHost              string
	PeerPort              int
	SSHPort               int
	SSHUser               string // SSH user for reconnection
	SSHKeyPath            string // SSH key path for reconnection
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

func NewApp(f Handler, listenPort int, peerHost string, peerPort int, poolInit int, poolCap int, ping bool, isClient bool, sshUser string, sshKeyPath string, sshPort int) *App {
	appInstance := &App{
		ListenPort:        listenPort,
		PeerHost:          peerHost,
		PeerPort:          peerPort,
		SSHPort:           sshPort,
		SSHUser:           sshUser,
		SSHKeyPath:        sshKeyPath,
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

		// Initialize pool connections in parallel groups of 10
		appInstance.PoolConnections = createPoolConnectionsParallel(poolInit, appInstance.PeerHost, appInstance.PeerPort, ping, sshUser, sshKeyPath, appInstance.SSHPort)

		log.Debugf("Created %d pool connections", len(appInstance.PoolConnections))
	} else {
		log.Debug("Server side - no pool connections created")
	}

	// Set the global app variable so GetApp() returns the correct instance
	app = appInstance
	return app
}

func createPoolConnection(id uint64, host string, port int, ping bool, sshUser string, sshKeyPath string, sshPort int) *PoolConnection {
	log.Tracef("Creating SSH tunnel connection %d to %s@%s:%d (SSH port %d)", id, sshUser, host, port, sshPort)

	// Read SSH private key
	keyBytes, err := os.ReadFile(sshKeyPath)
	if err != nil {
		log.Errorf("Error reading SSH key file %s for connection %d: %v", sshKeyPath, id, err)
		return nil
	}

	// Parse SSH private key
	signer, err := ssh.ParsePrivateKey(keyBytes)
	if err != nil {
		log.Errorf("Error parsing SSH key for connection %d: %v", id, err)
		return nil
	}

	// SSH client configuration
	config := &ssh.ClientConfig{
		User: sshUser,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // For automated connections
		Timeout:         10 * time.Second,
	}

	// Connect to SSH server with retry logic
	var sshConn *ssh.Client
	maxRetries := 3
	retryDelay := 100 * time.Millisecond

	for retry := 0; retry < maxRetries; retry++ {
		sshConn, err = ssh.Dial("tcp", fmt.Sprintf("%s:%d", host, sshPort), config)
		if err == nil {
			break // Success
		}

		if retry < maxRetries-1 {
			log.Debugf("SSH connection attempt %d failed for connection %d, retrying in %v: %v",
				retry+1, id, retryDelay, err)
			time.Sleep(retryDelay)
			retryDelay *= 2 // Exponential backoff
		} else {
			log.Errorf("Error connecting to SSH server %s:%d for connection %d after %d attempts: %v",
				host, sshPort, id, maxRetries, err)
			return nil
		}
	}

	// Create local port forward
	localListener, err := net.Listen("tcp", "127.0.0.1:0") // Use port 0 for automatic assignment
	if err != nil {
		log.Errorf("Error creating local listener for connection %d: %v", id, err)
		sshConn.Close()
		return nil
	}

	localPort := localListener.Addr().(*net.TCPAddr).Port
	localListener.Close() // Close the listener, we just needed the port

	// Connect to the target service through SSH tunnel
	remoteConn, err := sshConn.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		log.Errorf("Error connecting through SSH tunnel for connection %d: %v", id, err)
		sshConn.Close()
		return nil
	}

	poolConn := &PoolConnection{
		Conn:      remoteConn,
		SendChan:  make(chan *twtproto.ProxyComm, 100),
		ID:        id,
		InUse:     false,
		LastUsed:  time.Now(),
		SSHClient: sshConn,
		SSHConn:   remoteConn,
		LocalPort: localPort,
	}

	// Start goroutines for this connection
	go poolConnectionSender(poolConn)
	go poolConnectionReceiver(poolConn)
	startSSHKeepAlive(poolConn) // Start SSH keep-alive for this connection

	// Send initial ping if enabled
	if ping {
		pingMessage := &twtproto.ProxyComm{
			Mt:    twtproto.ProxyComm_PING,
			Proxy: proxyID,
		}
		poolConn.SendChan <- pingMessage
	}

	log.Infof("SSH tunnel connection %d established: direct tunnel -> %s@%s:%d (protobuf port %d)", id, sshUser, host, sshPort, port)
	return poolConn
}

// createPoolConnectionsParallel creates pool connections in parallel groups to speed up initialization
func createPoolConnectionsParallel(poolInit int, host string, port int, ping bool, sshUser string, sshKeyPath string, sshPort int) []*PoolConnection {
	if poolInit <= 0 {
		return []*PoolConnection{}
	}

	const groupSize = 3                           // Smaller groups to avoid overwhelming SSH server
	const connectionDelay = 50 * time.Millisecond // Small delay between connections in a group
	const groupDelay = 200 * time.Millisecond     // Delay between groups
	var allConnections []*PoolConnection
	var mutex sync.Mutex
	var wg sync.WaitGroup

	log.Debugf("Creating %d pool connections in parallel groups of %d with rate limiting", poolInit, groupSize)

	// Process connections in smaller groups with delays
	for startIdx := 0; startIdx < poolInit; startIdx += groupSize {
		endIdx := startIdx + groupSize
		if endIdx > poolInit {
			endIdx = poolInit
		}

		// Create a group of connections with controlled parallelism
		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()

			var groupConnections []*PoolConnection
			var groupWg sync.WaitGroup

			// Create each connection in this group with small delays
			for i := start; i < end; i++ {
				groupWg.Add(1)
				go func(connID int) {
					defer groupWg.Done()

					// Add a small random delay to stagger connections
					if connID > start {
						time.Sleep(time.Duration(connID-start) * connectionDelay)
					}

					log.Tracef("Creating pool connection %d in parallel", connID)
					poolConn := createPoolConnection(uint64(connID), host, port, ping, sshUser, sshKeyPath, sshPort)

					if poolConn != nil {
						mutex.Lock()
						groupConnections = append(groupConnections, poolConn)
						mutex.Unlock()
						log.Tracef("Pool connection %d created successfully", connID)
					} else {
						log.Warnf("Failed to create pool connection %d", connID)
					}
				}(i)
			}

			// Wait for all connections in this group to complete
			groupWg.Wait()

			// Add group connections to the main list
			mutex.Lock()
			allConnections = append(allConnections, groupConnections...)
			mutex.Unlock()

			log.Debugf("Completed group of %d connections (IDs %d-%d)", end-start, start, end-1)
		}(startIdx, endIdx)

		// Add delay between groups to avoid overwhelming the server
		if startIdx+groupSize < poolInit {
			time.Sleep(groupDelay)
		}
	}

	// Wait for all groups to complete
	wg.Wait()

	log.Infof("Parallel pool creation completed: %d connections created out of %d requested", len(allConnections), poolInit)
	return allConnections
}

// startSSHKeepAlive starts the SSH keep-alive goroutine with proper lifecycle management
func startSSHKeepAlive(poolConn *PoolConnection) {
	poolConn.keepAliveMutex.Lock()
	defer poolConn.keepAliveMutex.Unlock()

	// Cancel any existing keep-alive goroutine
	if poolConn.keepAliveCancel != nil {
		poolConn.keepAliveCancel()
	}

	// Create new context for this keep-alive session
	ctx, cancel := context.WithCancel(context.Background())
	poolConn.keepAliveCancel = cancel

	// Start the keep-alive goroutine
	go sshKeepAlive(ctx, poolConn)
}

// sshKeepAlive sends periodic keep-alive messages to maintain SSH connections
// This helps prevent connection drops due to firewalls, NAT timeouts, or idle connection policies
func sshKeepAlive(ctx context.Context, poolConn *PoolConnection) {
	const keepAliveInterval = 30 * time.Second // Send keep-alive every 30 seconds
	const maxKeepAliveFailures = 3             // Close connection after 3 failed keep-alives

	log.Tracef("Starting SSH keep-alive for pool connection %d", poolConn.ID)
	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()

	failureCount := 0

	for {
		select {
		case <-ctx.Done():
			log.Tracef("SSH keep-alive cancelled for pool connection %d", poolConn.ID)
			return
		case <-ticker.C:
			if poolConn.SSHClient == nil {
				log.Tracef("SSH client is nil for pool connection %d, stopping keep-alive", poolConn.ID)
				return
			}

			// Send SSH keep-alive request (empty global request)
			_, _, err := poolConn.SSHClient.SendRequest("keepalive@openssh.com", true, nil)
			if err != nil {
				failureCount++
				log.Debugf("SSH keep-alive failed for pool connection %d (attempt %d/%d): %v",
					poolConn.ID, failureCount, maxKeepAliveFailures, err)

				if failureCount >= maxKeepAliveFailures {
					log.Warnf("SSH keep-alive failed %d times for pool connection %d, closing connection",
						maxKeepAliveFailures, poolConn.ID)

					// Close the SSH connection to trigger reconnection logic
					if poolConn.SSHClient != nil {
						poolConn.SSHClient.Close()
					}
					if poolConn.Conn != nil {
						poolConn.Conn.Close()
					}
					return
				}
			} else {
				// Reset failure count on successful keep-alive
				if failureCount > 0 {
					log.Tracef("SSH keep-alive recovered for pool connection %d", poolConn.ID)
					failureCount = 0
				}
				log.Tracef("SSH keep-alive sent successfully for pool connection %d", poolConn.ID)
			}
		}
	}
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

	// Keep track of reconnection parameters
	var reconnectDelay time.Duration = 1 * time.Second
	const maxReconnectDelay = 30 * time.Second
	const reconnectBackoffMultiplier = 2

	for {
		// Handle the connection - this will block until connection fails
		handleConnection(poolConn.Conn)

		log.Warnf("Pool connection %d lost, attempting reconnection in %v", poolConn.ID, reconnectDelay)

		// Clear any connection state that might be corrupted
		clearConnectionState()

		// Cancel the keep-alive for the old connection
		poolConn.keepAliveMutex.Lock()
		if poolConn.keepAliveCancel != nil {
			poolConn.keepAliveCancel()
			poolConn.keepAliveCancel = nil
		}
		poolConn.keepAliveMutex.Unlock()

		// Close existing connections
		if poolConn.Conn != nil {
			poolConn.Conn.Close()
		}
		if poolConn.SSHClient != nil {
			poolConn.SSHClient.Close()
		}

		// Wait before reconnecting
		time.Sleep(reconnectDelay)

		// Attempt to recreate the connection
		if app != nil {
			newPoolConn := createPoolConnection(poolConn.ID, app.PeerHost, app.PeerPort, app.Ping, app.SSHUser, app.SSHKeyPath, app.SSHPort)
			if newPoolConn != nil {
				// Update the pool connection with new connection details
				app.PoolMutex.Lock()
				poolConn.Conn = newPoolConn.Conn
				poolConn.SSHClient = newPoolConn.SSHClient
				poolConn.SSHConn = newPoolConn.SSHConn
				poolConn.LocalPort = newPoolConn.LocalPort
				poolConn.LastUsed = time.Now()
				app.PoolMutex.Unlock()

				log.Infof("Pool connection %d successfully reconnected", poolConn.ID)

				// Start new SSH keep-alive for the reconnected connection
				startSSHKeepAlive(poolConn)

				// Reset reconnection delay on successful connection
				reconnectDelay = 1 * time.Second

				// Send ping if enabled
				if app.Ping {
					pingMessage := &twtproto.ProxyComm{
						Mt:    twtproto.ProxyComm_PING,
						Proxy: proxyID,
					}
					select {
					case poolConn.SendChan <- pingMessage:
						log.Tracef("Reconnection ping queued for pool connection %d", poolConn.ID)
					default:
						log.Warnf("Could not queue reconnection ping for pool connection %d", poolConn.ID)
					}
				}

				// Continue the loop to handle the new connection
				continue
			}
		}

		// Reconnection failed, increase delay with exponential backoff
		log.Errorf("Failed to reconnect pool connection %d, retrying in %v", poolConn.ID, reconnectDelay)
		reconnectDelay *= reconnectBackoffMultiplier
		if reconnectDelay > maxReconnectDelay {
			reconnectDelay = maxReconnectDelay
		}
	}
}

// clearConnectionState clears potentially corrupted connection state
func clearConnectionState() {
	if app == nil {
		return
	}

	log.Warnf("Clearing connection state due to connection failure")

	// Clear local connections (client side)
	app.LocalConnectionMutex.Lock()
	localCount := len(app.LocalConnections)
	for connID, conn := range app.LocalConnections {
		if conn.Connection != nil {
			conn.Connection.Close()
		}
		delete(app.LocalConnections, connID)
	}
	app.LocalConnectionMutex.Unlock()

	// Clear remote connections (server side)
	app.RemoteConnectionMutex.Lock()
	remoteCount := len(app.RemoteConnections)
	for connID, conn := range app.RemoteConnections {
		if conn.Connection != nil {
			conn.Connection.Close()
		}
		delete(app.RemoteConnections, connID)
	}
	app.RemoteConnectionMutex.Unlock()

	// Clear the global protobuf connection
	if protobufConnection != nil {
		protobufConnection.Close()
		protobufConnection = nil
	}

	log.Infof("Cleared %d local and %d remote connections", localCount, remoteCount)
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
	app.LocalConnections[thisConnection] = Connection{Connection: conn, LastSeqIn: 0, MessageQueue: make(map[uint64]*twtproto.ProxyComm)}
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
	data, err := proto.Marshal(message)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	log.Tracef("Marshalling message hexdump length(%d): %s", len(data), hex.Dump(data))
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
	// Check if we're in server mode (no pool connections)
	if len(app.PoolConnections) == 0 {
		// Server mode - send directly through the protobuf connection
		if protobufConnection != nil {
			log.Tracef("Sending message via direct protobuf connection (server mode)")
			sendProtobufToConn(protobufConnection, message)
			return
		} else {
			log.Errorf("No protobuf connection available for server response")
			return
		}
	}

	// Client mode - use pool connections
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

var protobufConnection net.Conn // Global variable to store the protobuf connection for server responses

// isTLSTraffic detects if the incoming data appears to be TLS/SSL traffic
// TLS records have specific content types in the first byte
func isTLSTraffic(header []byte) bool {
	if len(header) < 2 {
		return false
	}

	// TLS Content Types (RFC 5246):
	// 0x14 = Change Cipher Spec
	// 0x15 = Alert
	// 0x16 = Handshake
	// 0x17 = Application Data
	firstByte := header[0]
	return firstByte == 0x14 || firstByte == 0x15 || firstByte == 0x16 || firstByte == 0x17
}

// getByteOrZero safely gets a byte from slice or returns 0 if index is out of bounds
func getByteOrZero(data []byte, index int) byte {
	if index < len(data) {
		return data[index]
	}
	return 0
}

func handleConnection(conn net.Conn) {
	// Store the connection for server responses
	protobufConnection = conn

	defer func() {
		// Ensure connection is closed when this function exits
		conn.Close()
		log.Tracef("handleConnection exiting, connection closed")
	}()

	for {
		l := make([]byte, 2)
		_, err := conn.Read(l)
		if err != nil {
			log.Infof("Error reading frame length: %v", err)
			return
		}

		length := int(l[1])*256 + int(l[0])

		// Validate message length to detect frame corruption
		const maxReasonableMessageSize = 64 * 1024 // 64KB max message size
		if length <= 0 || length > maxReasonableMessageSize {
			log.Errorf("Invalid message length %d (0x%x), frame sync lost. Length bytes: 0x%02x 0x%02x",
				length, length, l[0], l[1])
			log.Warnf("Closing connection due to frame corruption - will trigger reconnection")
			return
		}

		log.Tracef("Expecting protobuf message long %d bytes", length)
		B := make([]byte, 0, length)
		b := make([]byte, length)
		for len(B) != length {
			n, err := conn.Read(b)
			if err != nil {
				log.Infof("Error reading data: %v", err)
				if err != io.EOF {
					fmt.Println("read error:", err)
				}
				return
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

				// Provide additional diagnostic information
				if len(B) > 0 {
					log.Warnf("First few bytes: %02x %02x %02x %02x",
						B[0],
						getByteOrZero(B, 1),
						getByteOrZero(B, 2),
						getByteOrZero(B, 3))
				}

				log.Warnf("Closing connection due to protobuf parse error - will trigger reconnection")
				return
			}
			handleProxycommMessage(message)
		} else {
			log.Errorf("Error receiving protobuf message, expected %4d, got %4d", length, len(B))
			log.Warnf("Closing connection due to incomplete message - will trigger reconnection")
			return
		}
	}
}

func handleProxycommMessage(message *twtproto.ProxyComm) {
	// Check if app is initialized to prevent race condition crashes
	if app == nil {
		log.Warnf("Received message before app initialization, ignoring: %v", message.Mt)
		return
	}

	// Marshal message back to show hexdump
	data, err := proto.Marshal(message)
	if err != nil {
		log.Tracef("Received message: %v", message)
	} else {
		log.Tracef("Received message hexdump length(%d): %s", len(data), hex.Dump(data))
	}
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
			thisConnection.MessageQueue[message.Seq] = message
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
			app.RemoteConnections[message.Connection] = Connection{Connection: nil, LastSeqIn: 0, NextSeqOut: 0, MessageQueue: make(map[uint64]*twtproto.ProxyComm)}
			thisConnection = app.RemoteConnections[message.Connection]
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
			thisConnection.MessageQueue[message.Seq] = message
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
			closeConnectionLocal(queueMessage)
		case twtproto.ProxyComm_DATA_DOWN:
			backwardDataChunk(queueMessage)
		case twtproto.ProxyComm_CLOSE_CONN_S:
			closeConnectionRemote(queueMessage)
		case twtproto.ProxyComm_DATA_UP:
			forwardDataChunk(queueMessage)
		}
		delete((*connections)[queueMessage.Connection].MessageQueue, seq)
		seq++
		queueMessage, ok = thisConnection.MessageQueue[seq]
	}
	mutex.Unlock()
}

func newConnection(message *twtproto.ProxyComm) {
	log.Infof("Openning connection %4d to %s:%d", message.Connection, message.Address, message.Port)
	conn, err := net.Dial("tcp", net.JoinHostPort(message.Address, strconv.Itoa(int(message.Port))))
	if err != nil {
		log.Errorf("Failed to connect to %s:%d for connection %d: %v", message.Address, message.Port, message.Connection, err)

		// Send close connection message back to client to inform of failure
		closeMessage := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_CLOSE_CONN_C,
			Proxy:      proxyID,
			Connection: message.Connection,
			Seq:        0,
		}
		sendProtobuf(closeMessage)
		return
	}
	thisConnection, ok := app.RemoteConnections[message.Connection]
	if !ok {
		log.Tracef("Connection %4d not known, creating record", message.Connection)
		app.RemoteConnections[message.Connection] = Connection{Connection: conn, LastSeqIn: 0, NextSeqOut: 1, MessageQueue: make(map[uint64]*twtproto.ProxyComm)}
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
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", listenPort))
	if err != nil {
		log.Fatalf("Error listening: %s", err.Error())
	}
	defer l.Close()
	log.Infof("Listening on 127.0.0.1:%d (loopback only for SSH tunneling)", listenPort)
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
