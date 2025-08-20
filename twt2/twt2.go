package twt2

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
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

// Connection retry configuration
var connectionRetryConfig = struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	MaxRetries   int
}{
	InitialDelay: 1 * time.Second,
	MaxDelay:     30 * time.Second,
	MaxRetries:   -1, // -1 means unlimited retries
}
var connectionRetryMutex sync.RWMutex
var clearConnectionStateMutex sync.Mutex

var app *App
var appMutex sync.RWMutex

// Helper functions for safe app access
func getApp() *App {
	appMutex.RLock()
	defer appMutex.RUnlock()
	return app
}

func setApp(newApp *App) {
	appMutex.Lock()
	defer appMutex.Unlock()
	app = newApp
}

// initUndeliveredMessageSystem initializes the channel-based undelivered message system
func initUndeliveredMessageSystem() {
	undeliveredMessageSystemOnce.Do(func() {
		undeliveredMessageChan = make(chan *UndeliveredMessage, undeliveredMessageBufferSize)
		undeliveredPriorityMessageChan = make(chan *UndeliveredMessage, undeliveredPriorityBufferSize)
	})
}

// startUndeliveredMessageProcessor starts a single goroutine to process undelivered messages
func startUndeliveredMessageProcessor() {
	undeliveredMessageProcessor.mutex.Lock()
	defer undeliveredMessageProcessor.mutex.Unlock()

	if undeliveredMessageProcessor.running {
		return // Already running
	}

	// Create fresh channels each time we start
	undeliveredMessageProcessor.stop = make(chan struct{})
	undeliveredMessageProcessor.done = make(chan struct{})
	undeliveredMessageProcessor.running = true

	go func() {
		defer func() {
			undeliveredMessageProcessor.mutex.Lock()
			undeliveredMessageProcessor.running = false
			undeliveredMessageProcessor.mutex.Unlock()
			close(undeliveredMessageProcessor.done)
		}()

		log.Debug("Undelivered message processor started")

		for {
			select {
			case <-undeliveredMessageProcessor.stop:
				log.Debug("Undelivered message processor stopping")
				return

			case priorityMsg := <-undeliveredPriorityMessageChan:
				// Process priority messages first
				processUndeliveredMessage(priorityMsg, true)
				// Small delay to prevent excessive CPU usage when processing many messages
				time.Sleep(10 * time.Millisecond)

			case normalMsg := <-undeliveredMessageChan:
				// Process normal messages, but check for priority messages first
				select {
				case priorityMsg := <-undeliveredPriorityMessageChan:
					// Priority message arrived, process it first and put normal message back
					processUndeliveredMessage(priorityMsg, true)
					// Try to put normal message back (non-blocking)
					select {
					case undeliveredMessageChan <- normalMsg:
					default:
						log.Warnf("Undelivered message channel full, dropping normal message for connection %d", normalMsg.ConnectionID)
					}
				default:
					// No priority messages, process normal message
					processUndeliveredMessage(normalMsg, false)
				}
				// Small delay to prevent excessive CPU usage when processing many messages
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
}

// processUndeliveredMessage attempts to send a single undelivered message
func processUndeliveredMessage(msg *UndeliveredMessage, isPriority bool) {
	if msg == nil {
		return
	}

	currentApp := getApp()
	if currentApp == nil {
		return
	}

	maxRetries := 3
	retryDelay := time.Minute * 2 // Wait 2 minutes between retries

	// Check if message is too old or has too many retries
	if msg.RetryCount >= maxRetries {
		log.Warnf("Dropping %s undelivered message for connection %d after %d retries",
			getPriorityString(isPriority), msg.ConnectionID, msg.RetryCount)
		return
	}

	// Check if message is too new for retry (except first attempt)
	if msg.RetryCount > 0 && time.Since(msg.Timestamp) < retryDelay {
		// Put message back in appropriate channel for later processing
		// Don't update timestamp - keep original timestamp for proper retry delay
		select {
		case undeliveredPriorityMessageChan <- msg:
			if isPriority {
				log.Tracef("Requeued priority message for connection %d (retry %d, waiting %v)",
					msg.ConnectionID, msg.RetryCount, retryDelay-time.Since(msg.Timestamp))
			}
		case undeliveredMessageChan <- msg:
			if !isPriority {
				log.Tracef("Requeued normal message for connection %d (retry %d, waiting %v)",
					msg.ConnectionID, msg.RetryCount, retryDelay-time.Since(msg.Timestamp))
			}
		default:
			log.Warnf("Failed to requeue %s message for connection %d - channels full", getPriorityString(isPriority), msg.ConnectionID)
		}
		return
	}

	// Try to send through available connections
	sent := false

	// For client side, try pool connections
	if len(currentApp.PoolConnections) > 0 {
		currentApp.PoolMutex.Lock()
		for _, poolConn := range currentApp.PoolConnections {
			if poolConn != nil && poolConn.Conn != nil && poolConn.Healthy {
				// Send directly to avoid recursive sendProtobuf calls
				sendProtobufToConn(poolConn.Conn, msg.Message)
				sent = true
				log.Tracef("Resent %s undelivered message for connection %d (retry %d)",
					getPriorityString(isPriority), msg.ConnectionID, msg.RetryCount+1)
				break
			}
		}
		currentApp.PoolMutex.Unlock()
	} else {
		// For server side, try active client connections first, then fallback
		serverClientMutex.RLock()
		activeConnections := make([]*ServerClientConnection, 0)
		for _, clientConn := range serverClientConnections {
			if clientConn.Active && clientConn.Conn != nil {
				activeConnections = append(activeConnections, clientConn)
			}
		}
		serverClientMutex.RUnlock()

		if len(activeConnections) > 0 {
			// Use the first active client connection
			selectedConn := activeConnections[0]
			sendProtobufToConn(selectedConn.Conn, msg.Message)
			sent = true
			log.Tracef("Resent %s undelivered message for connection %d via server client connection (retry %d)",
				getPriorityString(isPriority), msg.ConnectionID, msg.RetryCount+1)
		} else {
			// Fallback to protobuf connection only if it exists and no client connections
			protobufConnectionMutex.RLock()
			conn := protobufConnection
			protobufConnectionMutex.RUnlock()

			if conn != nil {
				sendProtobufToConn(conn, msg.Message)
				sent = true
				log.Tracef("Resent %s undelivered message for connection %d via fallback protobuf connection (retry %d)",
					getPriorityString(isPriority), msg.ConnectionID, msg.RetryCount+1)
			}
		}
	}

	if !sent {
		// Increment retry count and requeue
		msg.RetryCount++
		msg.Timestamp = time.Now()

		// Put back in appropriate channel
		if isPriority {
			select {
			case undeliveredPriorityMessageChan <- msg:
				log.Tracef("Requeued priority message for connection %d (retry %d)", msg.ConnectionID, msg.RetryCount)
			default:
				log.Warnf("Priority undelivered message channel full, dropping message for connection %d", msg.ConnectionID)
			}
		} else {
			select {
			case undeliveredMessageChan <- msg:
				log.Tracef("Requeued normal message for connection %d (retry %d)", msg.ConnectionID, msg.RetryCount)
			default:
				log.Warnf("Normal undelivered message channel full, dropping message for connection %d", msg.ConnectionID)
			}
		}
	}
}

// getPriorityString returns a string representation of priority status
func getPriorityString(isPriority bool) string {
	if isPriority {
		return "priority"
	}
	return "normal"
}

// queueUndeliveredMessage adds a message to the appropriate undelivered message channel
func queueUndeliveredMessage(message *twtproto.ProxyComm, connectionID uint64, isLocal bool) {
	if message == nil {
		return
	}

	undeliveredMsg := &UndeliveredMessage{
		Message:      message,
		ConnectionID: connectionID,
		IsLocal:      isLocal,
		Timestamp:    time.Now(),
		RetryCount:   0,
	}

	// Determine if this is a priority message
	isPriority := isHighPriorityMessage(message)

	if isPriority {
		select {
		case undeliveredPriorityMessageChan <- undeliveredMsg:
			log.Tracef("Queued priority undelivered message for connection %d", connectionID)
		default:
			log.Warnf("Priority undelivered message channel full, dropping message for connection %d", connectionID)
		}
	} else {
		select {
		case undeliveredMessageChan <- undeliveredMsg:
			log.Tracef("Queued normal undelivered message for connection %d", connectionID)
		default:
			log.Warnf("Normal undelivered message channel full, dropping message for connection %d", connectionID)
		}
	}
}

// getUndeliveredMessageStats returns statistics about the undelivered message channels
func getUndeliveredMessageStats() (int, int, int) {
	normalTotal := len(undeliveredMessageChan)
	priorityTotal := len(undeliveredPriorityMessageChan)
	total := normalTotal + priorityTotal

	// We can't easily distinguish local vs remote from channel contents without draining,
	// so we'll return total, normal, priority instead
	return total, normalTotal, priorityTotal
}

// stopUndeliveredMessageProcessor stops the undelivered message processor
func stopUndeliveredMessageProcessor() {
	undeliveredMessageProcessor.mutex.Lock()

	if !undeliveredMessageProcessor.running {
		undeliveredMessageProcessor.mutex.Unlock()
		return
	}

	// Signal the processor to stop
	close(undeliveredMessageProcessor.stop)
	undeliveredMessageProcessor.mutex.Unlock()

	// Wait for processor to finish
	<-undeliveredMessageProcessor.done
}

// stopStatsLogger stops the stats logger
func stopStatsLogger() {
	statsLogger.mutex.Lock()

	if !statsLogger.running {
		statsLogger.mutex.Unlock()
		return
	}

	// Signal the logger to stop
	close(statsLogger.stop)
	statsLogger.mutex.Unlock()

	// Wait for logger to finish
	<-statsLogger.done
}

// SetConnectionRetryConfig configures connection retry parameters
func SetConnectionRetryConfig(initialDelay, maxDelay time.Duration, maxRetries int) {
	connectionRetryMutex.Lock()
	defer connectionRetryMutex.Unlock()
	connectionRetryConfig.InitialDelay = initialDelay
	connectionRetryConfig.MaxDelay = maxDelay
	connectionRetryConfig.MaxRetries = maxRetries
}

// getConnectionRetryConfig returns a copy of the current retry configuration
func getConnectionRetryConfig() (time.Duration, time.Duration, int) {
	connectionRetryMutex.RLock()
	defer connectionRetryMutex.RUnlock()
	return connectionRetryConfig.InitialDelay, connectionRetryConfig.MaxDelay, connectionRetryConfig.MaxRetries
}

// Global statistics for server-side tracking
var serverStats struct {
	TotalConnectionsHandled uint64
	TotalMessagesProcessed  uint64
	TotalBytesUp            uint64
	TotalBytesDown          uint64
	MessageCounts           map[twtproto.ProxyComm_MessageType]uint64 // Count by message type
	mutex                   sync.RWMutex
}

type Handler func(http.ResponseWriter, *http.Request)

// PoolConnectionStats tracks statistics for each pool connection
type PoolConnectionStats struct {
	MessageCounts map[twtproto.ProxyComm_MessageType]uint64 // Count of messages by type
	BytesUp       uint64                                    // Total bytes sent DATA_UP
	BytesDown     uint64                                    // Total bytes sent DATA_DOWN
	CreatedAt     time.Time                                 // When this connection was created
	LastMessageAt time.Time                                 // When last message was sent
	mutex         sync.RWMutex                              // Protect statistics
}

// NewPoolConnectionStats creates a new statistics tracker
func NewPoolConnectionStats() *PoolConnectionStats {
	return &PoolConnectionStats{
		MessageCounts: make(map[twtproto.ProxyComm_MessageType]uint64),
		CreatedAt:     time.Now(),
		LastMessageAt: time.Now(),
	}
}

// IncrementMessage increments the counter for a specific message type and bytes if applicable
func (s *PoolConnectionStats) IncrementMessage(msgType twtproto.ProxyComm_MessageType, dataLen int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.MessageCounts[msgType]++
	s.LastMessageAt = time.Now()

	switch msgType {
	case twtproto.ProxyComm_DATA_UP:
		s.BytesUp += uint64(dataLen)
	case twtproto.ProxyComm_DATA_DOWN:
		s.BytesDown += uint64(dataLen)
	}
}

// GetStats returns a copy of the current statistics
func (s *PoolConnectionStats) GetStats() (map[twtproto.ProxyComm_MessageType]uint64, uint64, uint64, time.Time, time.Time) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Create a copy of the message counts map
	counts := make(map[twtproto.ProxyComm_MessageType]uint64)
	for k, v := range s.MessageCounts {
		counts[k] = v
	}

	return counts, s.BytesUp, s.BytesDown, s.CreatedAt, s.LastMessageAt
}

// startStatsLogger starts a goroutine that logs connection pool statistics every 5 minutes
func startStatsLogger() {
	statsLogger.mutex.Lock()
	defer statsLogger.mutex.Unlock()

	if statsLogger.running {
		return
	}

	statsLogger.stop = make(chan struct{})
	statsLogger.done = make(chan struct{})
	statsLogger.ticker = time.NewTicker(1 * time.Minute)
	statsLogger.running = true

	go func() {
		defer func() {
			statsLogger.mutex.Lock()
			statsLogger.running = false
			if statsLogger.ticker != nil {
				statsLogger.ticker.Stop()
			}
			statsLogger.mutex.Unlock()
			close(statsLogger.done)
		}()

		log.Debug("Stats logger started")

		for {
			select {
			case <-statsLogger.stop:
				log.Debug("Stats logger stopping")
				return

			case <-statsLogger.ticker.C:
				// Check if app still exists before logging stats
				if getApp() != nil {
					logConnectionPoolStats()
				}
			}
		}
	}()
}

// logConnectionPoolStats logs detailed statistics for all pool connections (client) or connection stats (server)
func logConnectionPoolStats() {
	currentApp := getApp()
	if currentApp == nil {
		return
	}

	// Check if this is client side (has pool connections) or server side
	currentApp.PoolMutex.Lock()
	isClientSide := len(currentApp.PoolConnections) > 0
	currentApp.PoolMutex.Unlock()

	// Double-check app still exists after acquiring mutex
	if getApp() == nil {
		return
	}

	if isClientSide {
		// Client side - log pool connection statistics
		currentApp.PoolMutex.Lock()
		defer currentApp.PoolMutex.Unlock()

		// Final check if app still exists after acquiring the mutex
		if getApp() == nil {
			return
		}

		log.Debugf("=== Client Pool Connection Statistics ===")
		log.Debugf("Total pool connections: %d", len(currentApp.PoolConnections))

		totalMessages := make(map[twtproto.ProxyComm_MessageType]uint64)
		var totalBytesUp, totalBytesDown uint64

		for i, conn := range currentApp.PoolConnections {
			if conn.Stats == nil {
				log.Debugf("Connection %d (ID: %d): No statistics available", i, conn.ID)
				continue
			}

			counts, bytesUp, bytesDown, createdAt, lastMessageAt := conn.Stats.GetStats()

			log.Debugf("Connection %d (ID: %d):", i, conn.ID)
			log.Debugf("  Healthy: %v, In Use: %v", conn.Healthy, conn.InUse)
			log.Debugf("  Created: %v, Last Message: %v", createdAt.Format(time.RFC3339), lastMessageAt.Format(time.RFC3339))
			log.Debugf("  Bytes Up: %d, Bytes Down: %d", bytesUp, bytesDown)

			for msgType, count := range counts {
				if count > 0 {
					log.Debugf("  %s: %d messages", msgType.String(), count)
					totalMessages[msgType] += count
				}
			}

			totalBytesUp += bytesUp
			totalBytesDown += bytesDown
		}

		log.Debugf("=== Pool Totals ===")
		log.Debugf("Total Bytes Up: %d, Total Bytes Down: %d", totalBytesUp, totalBytesDown)
		for msgType, total := range totalMessages {
			if total > 0 {
				log.Debugf("Total %s messages: %d", msgType.String(), total)
			}
		}
		log.Debugf("=== End Client Statistics ===")
	} else {
		// Server side - log active connection statistics
		log.Debugf("=== Server Connection Statistics ===")

		currentApp.LocalConnectionMutex.Lock()
		localConnCount := len(currentApp.LocalConnections)
		var localConnDetails []string
		for connID, conn := range currentApp.LocalConnections {
			localConnDetails = append(localConnDetails, fmt.Sprintf("ID:%d(SeqIn:%d)", connID, conn.LastSeqIn))
		}
		currentApp.LocalConnectionMutex.Unlock()

		currentApp.RemoteConnectionMutex.Lock()
		remoteConnCount := len(currentApp.RemoteConnections)
		var remoteConnDetails []string
		for connID, conn := range currentApp.RemoteConnections {
			remoteConnDetails = append(remoteConnDetails, fmt.Sprintf("ID:%d(SeqIn:%d,SeqOut:%d)", connID, conn.LastSeqIn, conn.NextSeqOut))
		}
		currentApp.RemoteConnectionMutex.Unlock()

		protobufConnectionMutex.RLock()
		hasProtobufConn := protobufConnection != nil
		protobufConnectionMutex.RUnlock()

		// Get global server statistics
		serverStats.mutex.RLock()
		totalConnections := serverStats.TotalConnectionsHandled
		totalMessages := serverStats.TotalMessagesProcessed
		totalBytesUp := serverStats.TotalBytesUp
		totalBytesDown := serverStats.TotalBytesDown
		// Create a copy of message counts
		messageCounts := make(map[twtproto.ProxyComm_MessageType]uint64)
		for k, v := range serverStats.MessageCounts {
			messageCounts[k] = v
		}
		serverStats.mutex.RUnlock()

		log.Debugf("Active local connections: %d", localConnCount)
		if len(localConnDetails) > 0 {
			log.Debugf("  Local connection details: %v", localConnDetails)
		}
		log.Debugf("Active remote connections: %d", remoteConnCount)
		if len(remoteConnDetails) > 0 {
			log.Debugf("  Remote connection details: %v", remoteConnDetails)
		}
		log.Debugf("Protobuf connection active: %v", hasProtobufConn)
		log.Debugf("Total active connections: %d", localConnCount+remoteConnCount)
		log.Debugf("Total connections handled: %d", totalConnections)
		log.Debugf("Total messages processed: %d", totalMessages)
		log.Debugf("Total bytes up: %d, down: %d", totalBytesUp, totalBytesDown)
		log.Debugf("Message breakdown:")
		for msgType, count := range messageCounts {
			if count > 0 {
				log.Debugf("  %s: %d messages", msgType.String(), count)
			}
		}
		log.Debugf("Server mode detected (no pool connections)")
		log.Debugf("=== End Server Statistics ===")
	}
}

type PoolConnection struct {
	Conn            net.Conn
	SendChan        chan *twtproto.ProxyComm
	PriorityChan    chan *twtproto.ProxyComm // High-priority channel for ACK, OPEN_CONN, PING messages
	ID              uint64
	InUse           bool
	LastUsed        time.Time
	SSHClient       *ssh.Client          // SSH client for this connection
	SSHConn         net.Conn             // SSH connection
	LocalPort       int                  // Local port for SSH tunnel
	keepAliveCancel context.CancelFunc   // Cancel function for SSH keep-alive goroutine
	keepAliveMutex  sync.Mutex           // Mutex to protect keep-alive operations
	retryCancel     context.CancelFunc   // Cancel function for retry goroutine
	retryCtx        context.Context      // Context for retry operations
	LastHealthCheck time.Time            // Last time connection health was verified
	Healthy         bool                 // Whether connection is considered healthy
	Stats           *PoolConnectionStats // Statistics for this connection
}

// PendingAck represents a message waiting for acknowledgment
type PendingAck struct {
	Message          *twtproto.ProxyComm // the message waiting for ACK
	PoolConnectionID uint64              // ID of pool connection used to send this message (0 for server-side)
}

type Connection struct {
	Connection   net.Conn
	LastSeqIn    uint64                         // last sequence number we have used for the last incoming chunk
	NextSeqOut   uint64                         // next sequence number to be sent out
	MessageQueue map[uint64]*twtproto.ProxyComm // queue of messages with too high sequence numbers
	// ACK tracking fields for reliable message delivery
	PendingAcks map[uint64]*PendingAck // messages waiting for ACK, keyed by sequence number
	LastAckSent uint64                 // last sequence number we acknowledged
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
	// Proxy authentication
	ProxyAuthEnabled bool
	ProxyUsername    string
	ProxyPassword    string
	// PAC file configuration
	PACFilePath string
	// Background goroutine management
	ctx    context.Context
	cancel context.CancelFunc
}

func GetApp() *App {
	return app
}

func NewApp(f Handler, listenPort int, peerHost string, peerPort int, poolInit int, poolCap int, ping bool, isClient bool, sshUser string, sshKeyPath string, sshPort int, proxyAuthEnabled bool, proxyUsername string, proxyPassword string, pacFilePath string) *App {
	// Initialize server statistics
	serverStats.mutex.Lock()
	if serverStats.MessageCounts == nil {
		serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
	}
	serverStats.mutex.Unlock()

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
		ProxyAuthEnabled:  proxyAuthEnabled,
		ProxyUsername:     proxyUsername,
		ProxyPassword:     proxyPassword,
		PACFilePath:       pacFilePath,
	}

	// Create context for managing background goroutines
	appInstance.ctx, appInstance.cancel = context.WithCancel(context.Background())

	// Initialize undelivered message system for both client and server
	initUndeliveredMessageSystem()
	log.Debug("Initialized undelivered message system")

	// Only create pool connections on client side
	if isClient && peerHost != "" {
		log.Debug("Creating bidirectional connection pool (client side)")

		// Initialize pool connections in parallel groups of 10
		appInstance.PoolConnections = createPoolConnectionsParallel(poolInit, appInstance.PeerHost, appInstance.PeerPort, ping, sshUser, sshKeyPath, appInstance.SSHPort)

		log.Debugf("Created %d pool connections", len(appInstance.PoolConnections))

		// Start connection health monitoring
		startConnectionHealthMonitor()
		log.Debug("Started connection health monitoring")
	}

	// Start common services for both client and server side
	startUndeliveredMessageProcessor()
	log.Debug("Started undelivered message processing")

	startStatsLogger()
	log.Debug("Started connection pool statistics logging (every minute)")

	// Set the global app variable so GetApp() returns the correct instance
	setApp(appInstance)
	return app
}

// Stop cancels all background goroutines associated with this App
func (a *App) Stop() {
	if a.cancel != nil {
		a.cancel()
	}
	stopUndeliveredMessageProcessor()
	stopStatsLogger()
}

func createPoolConnection(id uint64, host string, port int, ping bool, sshUser string, sshKeyPath string, sshPort int) *PoolConnection {
	return createPoolConnectionWithOptions(id, host, port, ping, sshUser, sshKeyPath, sshPort, true)
}

func createPoolConnectionWithOptions(id uint64, host string, port int, ping bool, sshUser string, sshKeyPath string, sshPort int, startGoroutines bool) *PoolConnection {
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
		Conn:            remoteConn,
		SendChan:        make(chan *twtproto.ProxyComm, 1),
		PriorityChan:    make(chan *twtproto.ProxyComm, 10), // Larger buffer for priority messages
		ID:              id,
		InUse:           false,
		LastUsed:        time.Now(),
		SSHClient:       sshConn,
		SSHConn:         remoteConn,
		LocalPort:       localPort,
		LastHealthCheck: time.Now(),
		Healthy:         true,
		Stats:           NewPoolConnectionStats(),
	}

	// Start goroutines for this connection (only if requested)
	if startGoroutines {
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
						log.Warnf("Failed to create pool connection %d, creating placeholder for retry", connID)
						// Create a placeholder pool connection that will retry using existing reconnection logic
						retryCtx, retryCancel := context.WithCancel(context.Background())
						placeholderConn := &PoolConnection{
							Conn:            nil, // No connection initially
							SendChan:        make(chan *twtproto.ProxyComm, 2),
							PriorityChan:    make(chan *twtproto.ProxyComm, 10),
							ID:              uint64(connID),
							InUse:           false,
							LastUsed:        time.Now(),
							retryCtx:        retryCtx,
							retryCancel:     retryCancel,
							LastHealthCheck: time.Now(),
							Healthy:         false, // Not healthy until connected
							Stats:           NewPoolConnectionStats(),
						}

						// Start the sender and receiver goroutines - receiver will handle reconnection
						go poolConnectionSender(placeholderConn)
						go poolConnectionReceiver(placeholderConn)

						mutex.Lock()
						groupConnections = append(groupConnections, placeholderConn)
						mutex.Unlock()
						log.Tracef("Created placeholder pool connection %d for retry", connID)
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

// StopAllPoolConnections stops all retry goroutines for pool connections (for testing)
func StopAllPoolConnections() {
	currentApp := getApp()
	if currentApp == nil {
		return
	}

	currentApp.PoolMutex.Lock()
	defer currentApp.PoolMutex.Unlock()

	// Cancel all retry contexts to signal goroutines to stop
	for _, poolConn := range currentApp.PoolConnections {
		if poolConn.retryCancel != nil {
			poolConn.retryCancel()
		}
		// Also close the connection to break out of any blocking reads
		if poolConn.Conn != nil {
			poolConn.Conn.Close()
		}
	}

	// Wait a short time for goroutines to notice cancellation and exit
	// This is a practical timeout since we can't easily wait for all goroutines
	time.Sleep(200 * time.Millisecond)
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

// startConnectionHealthMonitor starts a background goroutine that monitors pool connection health
func startConnectionHealthMonitor() {
	go func() {
		ticker := time.NewTicker(2 * time.Minute) // Check every 2 minutes
		defer ticker.Stop()

		checkTicker := time.NewTicker(100 * time.Millisecond) // Quick check for nil app
		defer checkTicker.Stop()

		for {
			select {
			case <-ticker.C:
				// Main health check - only execute if app exists
				currentApp := getApp()
				if currentApp == nil {
					log.Tracef("Connection health monitor exiting due to nil app")
					return
				}

				currentApp.PoolMutex.Lock()
				if len(currentApp.PoolConnections) == 0 {
					currentApp.PoolMutex.Unlock()
					continue
				}
				healthyCount := 0
				totalCount := len(currentApp.PoolConnections)

				for _, poolConn := range currentApp.PoolConnections {
					if poolConn == nil {
						continue
					}

					if poolConn.Healthy && poolConn.Conn != nil {
						healthyCount++

						// Check for stale connections (no activity for 10 minutes)
						if time.Since(poolConn.LastHealthCheck) > 10*time.Minute {
							log.Debugf("Pool connection %d appears stale (last check %v ago)",
								poolConn.ID, time.Since(poolConn.LastHealthCheck))
						}
					}
				}

				log.Debugf("Connection health: %d/%d healthy connections", healthyCount, totalCount)
				currentApp.PoolMutex.Unlock()

			case <-checkTicker.C:
				// Quick check for nil app to exit during test cleanup
				if getApp() == nil {
					log.Tracef("Connection health monitor exiting due to nil app")
					return
				}
			}
		}
	}()
}

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
	if poolConn == nil {
		log.Errorf("poolConnectionSender called with nil poolConn")
		return
	}

	log.Tracef("Starting sender goroutine for pool connection %d", poolConn.ID)

	// Handle case where PriorityChan might not be initialized (for tests or old connections)
	if poolConn.PriorityChan == nil {
		log.Warnf("PriorityChan not initialized for pool connection %d, falling back to regular channel only", poolConn.ID)
		// Fallback to original behavior for backward compatibility
		for message := range poolConn.SendChan {
			log.Tracef("Processing regular message type %v for pool connection %d", message.Mt, poolConn.ID)
			// Check if connection is nil (closed/failed)
			if poolConn.Conn == nil {
				log.Warnf("Pool connection %d has nil connection, dropping message", poolConn.ID)
				poolConn.Healthy = false
				continue
			}

			// Send with timeout monitoring (same logic as below)
			done := make(chan bool, 1)
			var sendError error
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Errorf("Panic in sendProtobufToConn for connection %d: %v", poolConn.ID, r)
						sendError = fmt.Errorf("panic: %v", r)
					}
					done <- true
				}()

				// Set a write deadline before sending
				if poolConn.Conn != nil {
					poolConn.Conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				}

				sendProtobufToConn(poolConn.Conn, message)
			}()

			select {
			case <-done:
				if sendError != nil {
					log.Errorf("Pool connection %d send error: %v - marking as unhealthy", poolConn.ID, sendError)
					poolConn.Healthy = false
					if poolConn.Conn != nil {
						poolConn.Conn.Close()
						poolConn.Conn = nil
					}
					return // Exit sender to trigger reconnection via receiver
				}
				// Message sent successfully - mark as healthy
				poolConn.Healthy = true
				poolConn.LastHealthCheck = time.Now()

				// Track statistics for successfully sent message (if stats are initialized)
				if poolConn.Stats != nil {
					dataLen := 0
					if message.GetData() != nil {
						dataLen = len(message.GetData())
					}
					poolConn.Stats.IncrementMessage(message.GetMt(), dataLen)
				}

			case <-time.After(60 * time.Second):
				log.Errorf("Pool connection %d write timeout - marking as failed", poolConn.ID)
				// Mark connection as failed to trigger reconnection
				poolConn.Healthy = false
				if poolConn.Conn != nil {
					poolConn.Conn.Close()
					poolConn.Conn = nil
				}
				return // Exit sender to trigger reconnection via receiver
			}
		}
		log.Tracef("Sender goroutine for pool connection %d ended (regular channel only)", poolConn.ID)
		return
	}

	for {
		// Check if app still exists (for test cleanup) - only during testing
		testApp := getApp()
		if testApp == nil && poolConn.retryCtx != nil {
			// If app is nil and we have a test context, exit gracefully
			log.Tracef("Pool connection %d sender goroutine exiting due to nil app during test", poolConn.ID)
			return
		}

		var message *twtproto.ProxyComm
		var msgSource string

		// Priority select: always check priority channel first, then regular
		select {
		case message = <-poolConn.PriorityChan:
			msgSource = "priority"
		default:
			// No priority messages, check regular channel
			select {
			case message = <-poolConn.PriorityChan:
				msgSource = "priority"
			case message = <-poolConn.SendChan:
				msgSource = "regular"
			}
		}

		// If we got a message from the non-blocking select, process it
		if message != nil {
			log.Tracef("Processing %s message type %v for pool connection %d", msgSource, message.Mt, poolConn.ID)
		} else {
			// Both channels were empty, now block until we get a message
			select {
			case message = <-poolConn.PriorityChan:
				msgSource = "priority"
			case message = <-poolConn.SendChan:
				msgSource = "regular"
			}
			log.Tracef("Processing %s message type %v for pool connection %d", msgSource, message.Mt, poolConn.ID)
		}
		// Check if connection is nil (closed/failed)
		if poolConn.Conn == nil {
			log.Warnf("Pool connection %d has nil connection, dropping message", poolConn.ID)
			poolConn.Healthy = false
			continue
		}

		// Send with timeout monitoring
		done := make(chan bool, 1)
		var sendError error
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("Panic in sendProtobufToConn for connection %d: %v", poolConn.ID, r)
					sendError = fmt.Errorf("panic: %v", r)
				}
				done <- true
			}()

			// Set a write deadline before sending
			if poolConn.Conn != nil {
				poolConn.Conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
			}

			sendProtobufToConn(poolConn.Conn, message)
		}()

		select {
		case <-done:
			if sendError != nil {
				log.Errorf("Pool connection %d send error: %v - marking as unhealthy", poolConn.ID, sendError)
				poolConn.Healthy = false
				if poolConn.Conn != nil {
					poolConn.Conn.Close()
					poolConn.Conn = nil
				}
				return // Exit sender to trigger reconnection via receiver
			}
			// Message sent successfully - mark as healthy
			poolConn.Healthy = true
			poolConn.LastHealthCheck = time.Now()

			// Track statistics for successfully sent message (if stats are initialized)
			if poolConn.Stats != nil {
				dataLen := 0
				if message.GetData() != nil {
					dataLen = len(message.GetData())
				}
				poolConn.Stats.IncrementMessage(message.GetMt(), dataLen)
			}

		case <-time.After(60 * time.Second):
			log.Errorf("Pool connection %d write timeout - marking as failed", poolConn.ID)
			// Mark connection as failed to trigger reconnection
			poolConn.Healthy = false
			if poolConn.Conn != nil {
				poolConn.Conn.Close()
				poolConn.Conn = nil
			}
			return // Exit sender to trigger reconnection via receiver
		}
	}
	log.Tracef("Sender goroutine for pool connection %d ended", poolConn.ID)
}

func poolConnectionReceiver(poolConn *PoolConnection) {
	log.Tracef("Starting receiver goroutine for pool connection %d", poolConn.ID)

	// Keep track of reconnection parameters
	var retryCount int
	const reconnectBackoffMultiplier = 2

	// Get retry configuration
	reconnectDelay, maxReconnectDelay, maxRetries := getConnectionRetryConfig()

	for {
		// Check if app still exists (for test cleanup) - only during testing
		testApp := getApp()
		if testApp == nil && poolConn.retryCtx != nil {
			// If app is nil and we have a test context, exit gracefully
			log.Tracef("Pool connection %d receiver goroutine exiting due to nil app during test", poolConn.ID)
			return
		}

		// Check if context was canceled (for tests)
		if poolConn.retryCtx != nil {
			select {
			case <-poolConn.retryCtx.Done():
				log.Tracef("Pool connection %d receiver goroutine canceled", poolConn.ID)
				return
			default:
			}
		}

		// Handle the connection - this will block until connection fails
		// If poolConn.Conn is nil (placeholder connection), this will exit immediately
		// and trigger the reconnection logic below
		if poolConn.Conn != nil {
			handleConnectionWithPoolConn(poolConn.Conn, poolConn)
			log.Warnf("Pool connection %d lost, attempting reconnection in %v", poolConn.ID, reconnectDelay)
			// Mark connection as unhealthy immediately
			poolConn.Healthy = false
		} else {
			log.Infof("Pool connection %d starting initial connection attempt", poolConn.ID)
		}

		// Handle individual pool connection failure with proper isolation
		handlePoolConnectionFailure(poolConn)

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

		// Wait before reconnecting (skip initial delay for first connection attempt)
		if poolConn.Conn != nil {
			if poolConn.retryCtx != nil {
				// Use context with timeout for cancellable sleep
				timer := time.NewTimer(reconnectDelay)
				select {
				case <-poolConn.retryCtx.Done():
					timer.Stop()
					log.Tracef("Pool connection %d receiver goroutine canceled during delay", poolConn.ID)
					return
				case <-timer.C:
					// Continue with reconnection
				}
			} else {
				time.Sleep(reconnectDelay)
			}
		}

		// Attempt to recreate the connection
		currentApp := getApp()
		if currentApp != nil {
			newPoolConn := createPoolConnectionWithOptions(poolConn.ID, currentApp.PeerHost, currentApp.PeerPort, currentApp.Ping, currentApp.SSHUser, currentApp.SSHKeyPath, currentApp.SSHPort, false)
			if newPoolConn != nil {
				// Update the pool connection with new connection details
				currentApp.PoolMutex.Lock()
				poolConn.Conn = newPoolConn.Conn
				poolConn.SSHClient = newPoolConn.SSHClient
				poolConn.SSHConn = newPoolConn.SSHConn
				poolConn.LocalPort = newPoolConn.LocalPort
				poolConn.LastUsed = time.Now()
				poolConn.Healthy = true // Mark as healthy after successful reconnection
				poolConn.LastHealthCheck = time.Now()
				currentApp.PoolMutex.Unlock()

				log.Infof("Pool connection %d successfully connected", poolConn.ID)

				// Start new SSH keep-alive for the reconnected connection
				startSSHKeepAlive(poolConn)

				// Reset reconnection delay on successful connection
				retryCount = 0                                                             // Reset retry count on success
				reconnectDelay, maxReconnectDelay, maxRetries = getConnectionRetryConfig() // Get fresh config

				// Send ping if enabled (since we didn't start goroutines, no initial ping was sent)
				currentPingApp := getApp() // Use safe app access
				if currentPingApp != nil && currentPingApp.Ping {
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
		retryCount++

		// Check if we should stop retrying based on configuration
		if maxRetries > 0 && retryCount >= maxRetries {
			log.Warnf("Pool connection %d reached max retries (%d), stopping", poolConn.ID, maxRetries)
			return
		}

		reconnectDelay *= reconnectBackoffMultiplier
		if reconnectDelay > maxReconnectDelay {
			reconnectDelay = maxReconnectDelay
		}
	}
}

// handlePoolConnectionFailure handles the failure of a single pool connection
// while preserving all logical connections (they will be rerouted to healthy pools)
func handlePoolConnectionFailure(failedPoolConn *PoolConnection) {
	currentApp := getApp()
	if currentApp == nil {
		return
	}

	log.Warnf("Handling failure of pool connection %d - preserving logical connections", failedPoolConn.ID)

	// Close only the failed pool connection
	if failedPoolConn.Conn != nil {
		failedPoolConn.Conn.Close()
		failedPoolConn.Conn = nil
	}

	// Mark as unhealthy
	failedPoolConn.Healthy = false

	// Check if we have any healthy pool connections remaining
	healthyPoolCount := 0
	for _, poolConn := range currentApp.PoolConnections {
		if poolConn.Healthy && poolConn != failedPoolConn {
			healthyPoolCount++
		}
	}

	if healthyPoolCount > 0 {
		log.Infof("Pool connection %d failed, but %d healthy pool connections remain - logical connections preserved",
			failedPoolConn.ID, healthyPoolCount)

		// Retransmit all pending messages from the failed pool connection through healthy connections
		retransmitPendingMessagesFromFailedPool(failedPoolConn.ID)
	} else {
		log.Warnf("Pool connection %d failed and no healthy pool connections remain - clearing all connection state",
			failedPoolConn.ID)
		// Only clear all connection state if ALL pool connections are unhealthy
		clearConnectionState()
	}
}

// clearPendingAcksForFailedConnection clears pending ACKs to prevent
// retransmission attempts to a failed connection
func clearPendingAcksForFailedConnection(failedConnectionID uint64) {
	currentApp := getApp()
	if currentApp == nil {
		return
	}

	log.Debugf("Clearing pending ACKs for failed connection %d", failedConnectionID)

	currentApp.RemoteConnectionMutex.Lock()
	defer currentApp.RemoteConnectionMutex.Unlock()

	// Clear pending ACKs for all remote connections
	// This prevents ACK retransmissions from trying to use the failed connection
	clearedCount := 0
	for _, connection := range currentApp.RemoteConnections {
		if connection.PendingAcks != nil {
			clearedPending := len(connection.PendingAcks)
			if clearedPending > 0 {
				connection.PendingAcks = make(map[uint64]*PendingAck)
				clearedCount += clearedPending
			}
		}
	}

	if clearedCount > 0 {
		log.Infof("Cleared %d pending ACKs to prevent retransmission to failed connection %d",
			clearedCount, failedConnectionID)
	}
}

// retransmitPendingMessagesFromFailedPool retransmits all pending messages that were sent through a failed pool connection
func retransmitPendingMessagesFromFailedPool(failedPoolConnectionID uint64) {
	currentApp := getApp()
	if currentApp == nil {
		return
	}

	log.Debugf("Retransmitting pending messages from failed pool connection %d", failedPoolConnectionID)

	retransmittedCount := 0

	// Retransmit local connection messages (DATA_UP)
	currentApp.LocalConnectionMutex.Lock()
	for connID, connection := range currentApp.LocalConnections {
		if connection.PendingAcks == nil {
			continue
		}

		for seq, pendingAck := range connection.PendingAcks {
			// Only retransmit messages that were sent through the failed pool connection
			if pendingAck.PoolConnectionID == failedPoolConnectionID {
				log.Debugf("Retransmitting DATA_UP message conn=%d seq=%d from failed pool connection %d",
					connID, seq, failedPoolConnectionID)

				// Send through a different pool connection and update the tracking
				newPoolConnID := sendProtobufWithTracking(pendingAck.Message)
				pendingAck.PoolConnectionID = newPoolConnID
				retransmittedCount++
			}
		}
	}
	currentApp.LocalConnectionMutex.Unlock()

	// Retransmit remote connection messages (DATA_DOWN)
	currentApp.RemoteConnectionMutex.Lock()
	for connID, connection := range currentApp.RemoteConnections {
		if connection.PendingAcks == nil {
			continue
		}

		for seq, pendingAck := range connection.PendingAcks {
			// Only retransmit messages that were sent through the failed pool connection
			if pendingAck.PoolConnectionID == failedPoolConnectionID {
				log.Debugf("Retransmitting DATA_DOWN message conn=%d seq=%d from failed pool connection %d",
					connID, seq, failedPoolConnectionID)

				// Send through a different pool connection and update the tracking
				newPoolConnID := sendProtobufWithTracking(pendingAck.Message)
				pendingAck.PoolConnectionID = newPoolConnID
				retransmittedCount++
			}
		}
	}
	currentApp.RemoteConnectionMutex.Unlock()

	if retransmittedCount > 0 {
		log.Infof("Retransmitted %d pending messages from failed pool connection %d through other healthy connections",
			retransmittedCount, failedPoolConnectionID)
	}
}

// handleServerClientConnectionFailure handles the failure of a server-side client connection
// while preserving all remote connections (they will continue through other healthy client connections)
func handleServerClientConnectionFailure(failedClientConn *ServerClientConnection) {
	if failedClientConn == nil {
		return
	}

	log.Warnf("Handling failure of server client connection %d - preserving remote connections", failedClientConn.ID)

	// Immediately close the failed connection to prevent further usage
	if failedClientConn.Conn != nil {
		failedClientConn.Conn.Close()
		failedClientConn.Conn = nil
	}

	// Remove this connection from server-side pool atomically
	serverClientMutex.Lock()
	// Mark the connection as inactive first (while holding lock)
	failedClientConn.Active = false

	for i, conn := range serverClientConnections {
		if conn.ID == failedClientConn.ID {
			// Remove connection from slice
			serverClientConnections = append(serverClientConnections[:i], serverClientConnections[i+1:]...)
			break
		}
	}

	// Check if we have any healthy client connections remaining
	healthyClientCount := 0
	for _, clientConn := range serverClientConnections {
		if clientConn.Active && clientConn.Conn != nil {
			healthyClientCount++
		}
	}
	serverClientMutex.Unlock()

	// Clear any pending ACKs that were meant for this failed connection to prevent retransmission attempts
	clearPendingAcksForFailedConnection(failedClientConn.ID)

	if healthyClientCount > 0 {
		log.Infof("Server client connection %d failed, but %d healthy client connections remain - remote connections preserved",
			failedClientConn.ID, healthyClientCount)
		// Remote connections will automatically be rerouted to healthy client connections
		// by the existing load balancing logic in sendProtobuf()
	} else {
		log.Warnf("Server client connection %d failed and no healthy client connections remain - clearing all connection state",
			failedClientConn.ID)
		// Only clear all connection state if ALL client connections are gone
		clearConnectionState()
	}

	log.Debugf("Unregistered server client connection %d (remaining: %d)",
		failedClientConn.ID, healthyClientCount)
}

// clearConnectionState clears potentially corrupted connection state
func clearConnectionState() {
	// Ensure only one clearConnectionState operation at a time to prevent race conditions
	clearConnectionStateMutex.Lock()
	defer clearConnectionStateMutex.Unlock()

	currentApp := getApp() // Use safe app access
	if currentApp == nil {
		return
	}

	log.Warnf("Clearing connection state due to connection failure - preserving undelivered messages")

	messagesPreserved := 0

	// Clear local connections (client side) and preserve undelivered messages
	currentApp.LocalConnectionMutex.Lock()
	localCount := len(currentApp.LocalConnections)
	for connID, conn := range currentApp.LocalConnections {
		if conn.Connection != nil {
			conn.Connection.Close()
		}

		// Preserve messages from MessageQueue
		for seqNum, message := range conn.MessageQueue {
			queueUndeliveredMessage(message, connID, true)
			messagesPreserved++
			log.Tracef("Preserved local message seq=%d for connection %d", seqNum, connID)
		}

		// Preserve messages from PendingAcks
		for seqNum, pendingAck := range conn.PendingAcks {
			queueUndeliveredMessage(pendingAck.Message, connID, true)
			messagesPreserved++
			log.Tracef("Preserved local pending ACK seq=%d for connection %d", seqNum, connID)
		}

		// Clear buffer maps after preserving messages
		conn.MessageQueue = nil
		conn.PendingAcks = nil
		delete(currentApp.LocalConnections, connID)
	}
	currentApp.LocalConnectionMutex.Unlock()

	// Clear remote connections (server side) and preserve undelivered messages
	currentApp.RemoteConnectionMutex.Lock()
	remoteCount := len(currentApp.RemoteConnections)
	for connID, conn := range currentApp.RemoteConnections {
		if conn.Connection != nil {
			conn.Connection.Close()
		}

		// Preserve messages from MessageQueue
		for seqNum, message := range conn.MessageQueue {
			queueUndeliveredMessage(message, connID, false)
			messagesPreserved++
			log.Tracef("Preserved remote message seq=%d for connection %d", seqNum, connID)
		}

		// Preserve messages from PendingAcks
		for seqNum, pendingAck := range conn.PendingAcks {
			queueUndeliveredMessage(pendingAck.Message, connID, false)
			messagesPreserved++
			log.Tracef("Preserved remote pending ACK seq=%d for connection %d", seqNum, connID)
		}

		// Clear buffer maps after preserving messages
		conn.MessageQueue = nil
		conn.PendingAcks = nil
		delete(currentApp.RemoteConnections, connID)
	}
	currentApp.RemoteConnectionMutex.Unlock()

	// Clear the global protobuf connection
	protobufConnectionMutex.Lock()
	if protobufConnection != nil {
		protobufConnection.Close()
		protobufConnection = nil
	}
	protobufConnectionMutex.Unlock()

	total, normal, priority := getUndeliveredMessageStats()
	log.Infof("Cleared %d local and %d remote connections, preserved %d undelivered messages (queue: %d total, %d normal, %d priority)",
		localCount, remoteCount, messagesPreserved, total, normal, priority)
}

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.DefaultRoute(w, r)
}

// authenticateProxyRequest checks if the request has valid proxy authentication
func authenticateProxyRequest(r *http.Request, username, password string) bool {
	if username == "" && password == "" {
		log.Debugf("No proxy authentication required (both username and password empty)")
		return true // No authentication required
	}

	log.Debugf("Proxy authentication required - checking request headers")

	// Get the Proxy-Authorization header
	proxyAuth := r.Header.Get("Proxy-Authorization")
	if proxyAuth == "" {
		log.Debugf("No Proxy-Authorization header found in request")
		return false
	}

	// Check if it's Basic authentication
	if !strings.HasPrefix(proxyAuth, "Basic ") {
		log.Debugf("Proxy-Authorization header is not Basic authentication")
		return false
	}

	// Decode the base64 credentials
	encoded := strings.TrimPrefix(proxyAuth, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		log.Debugf("Failed to decode Proxy-Authorization header: %v", err)
		return false
	}

	// Split username:password
	credentials := strings.SplitN(string(decoded), ":", 2)
	if len(credentials) != 2 {
		log.Debugf("Invalid credential format in Proxy-Authorization header")
		return false
	}

	// Use constant-time comparison to prevent timing attacks
	usernameMatch := subtle.ConstantTimeCompare([]byte(credentials[0]), []byte(username)) == 1
	passwordMatch := subtle.ConstantTimeCompare([]byte(credentials[1]), []byte(password)) == 1

	success := usernameMatch && passwordMatch
	log.Debugf("Proxy authentication result: %t", success)

	return success
}

// sendProxyAuthRequired sends a 407 Proxy Authentication Required response
func sendProxyAuthRequired(w http.ResponseWriter) {
	w.Header().Set("Proxy-Authenticate", "Basic realm=\"TW2 Proxy\"")
	w.WriteHeader(http.StatusProxyAuthRequired)
	w.Write([]byte("Proxy Authentication Required"))
}

// servePACFile serves the PAC (Proxy Auto-Configuration) file
func servePACFile(w http.ResponseWriter, r *http.Request) {
	log.Infof("Serving PAC file request from %s", r.RemoteAddr)

	var pacContent []byte
	var err error

	currentApp := getApp()
	if currentApp != nil && currentApp.PACFilePath != "" {
		// Read PAC file from disk
		pacContent, err = os.ReadFile(currentApp.PACFilePath)
		if err != nil {
			log.Errorf("Error reading PAC file %s: %v", currentApp.PACFilePath, err)
			w.WriteHeader(http.StatusNotFound)
			return
		} else {
			log.Debugf("Successfully read PAC file from %s", currentApp.PACFilePath)
		}
	}

	// Set appropriate headers for PAC file
	w.Header().Set("Content-Type", "application/x-ns-proxy-autoconfig")
	w.Header().Set("Content-Length", strconv.Itoa(len(pacContent)))
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	w.WriteHeader(http.StatusOK)
	w.Write(pacContent)
}

// serveMetrics serves Prometheus-style metrics for monitoring
func serveMetrics(w http.ResponseWriter, r *http.Request) {
	log.Debugf("Serving metrics request from %s", r.RemoteAddr)

	currentApp := getApp()
	if currentApp == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("# App not initialized\n"))
		return
	}

	var metrics strings.Builder

	// Check if this is client side (has pool connections) or server side
	currentApp.PoolMutex.Lock()
	isClientSide := len(currentApp.PoolConnections) > 0
	currentApp.PoolMutex.Unlock()

	if isClientSide {
		// Client-side metrics
		generateClientMetrics(&metrics, currentApp)
	} else {
		// Server-side metrics
		generateServerMetrics(&metrics, currentApp)
	}

	// Add undelivered message queue statistics (available on both client and server)
	total, normal, priority := getUndeliveredMessageStats()
	metrics.WriteString("# HELP twt2_undelivered_messages_total Total number of undelivered messages in queues\n")
	metrics.WriteString("# TYPE twt2_undelivered_messages_total gauge\n")
	metrics.WriteString(fmt.Sprintf("twt2_undelivered_messages_total %d\n", total))
	metrics.WriteString("# HELP twt2_undelivered_messages_normal Number of normal undelivered messages in queue\n")
	metrics.WriteString("# TYPE twt2_undelivered_messages_normal gauge\n")
	metrics.WriteString(fmt.Sprintf("twt2_undelivered_messages_normal %d\n", normal))
	metrics.WriteString("# HELP twt2_undelivered_messages_priority Number of priority undelivered messages in queue\n")
	metrics.WriteString("# TYPE twt2_undelivered_messages_priority gauge\n")
	metrics.WriteString(fmt.Sprintf("twt2_undelivered_messages_priority %d\n", priority))

	// Set appropriate headers for Prometheus metrics
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(metrics.String()))
}

// generateClientMetrics generates Prometheus metrics for client-side (pool connections)
func generateClientMetrics(metrics *strings.Builder, currentApp *App) {
	currentApp.PoolMutex.Lock()
	defer currentApp.PoolMutex.Unlock()

	// Pool connection overview
	totalConnections := len(currentApp.PoolConnections)
	healthyConnections := 0
	inUseConnections := 0

	for _, conn := range currentApp.PoolConnections {
		if conn != nil {
			if conn.Healthy {
				healthyConnections++
			}
			if conn.InUse {
				inUseConnections++
			}
		}
	}

	// Connection pool overview metrics
	metrics.WriteString("# HELP twt2_pool_connections_total Total number of pool connections\n")
	metrics.WriteString("# TYPE twt2_pool_connections_total gauge\n")
	metrics.WriteString(fmt.Sprintf("twt2_pool_connections_total %d\n", totalConnections))

	metrics.WriteString("# HELP twt2_pool_connections_healthy Number of healthy pool connections\n")
	metrics.WriteString("# TYPE twt2_pool_connections_healthy gauge\n")
	metrics.WriteString(fmt.Sprintf("twt2_pool_connections_healthy %d\n", healthyConnections))

	metrics.WriteString("# HELP twt2_pool_connections_in_use Number of pool connections currently in use\n")
	metrics.WriteString("# TYPE twt2_pool_connections_in_use gauge\n")
	metrics.WriteString(fmt.Sprintf("twt2_pool_connections_in_use %d\n", inUseConnections))

	// Per-connection metrics
	metrics.WriteString("# HELP twt2_pool_connection_queue_length Current queue length for pool connection send channel\n")
	metrics.WriteString("# TYPE twt2_pool_connection_queue_length gauge\n")

	metrics.WriteString("# HELP twt2_pool_connection_queue_capacity Maximum queue capacity for pool connection send channel\n")
	metrics.WriteString("# TYPE twt2_pool_connection_queue_capacity gauge\n")

	metrics.WriteString("# HELP twt2_pool_connection_messages_total Total messages sent through pool connection\n")
	metrics.WriteString("# TYPE twt2_pool_connection_messages_total counter\n")

	metrics.WriteString("# HELP twt2_pool_connection_bytes_total Total bytes sent through pool connection\n")
	metrics.WriteString("# TYPE twt2_pool_connection_bytes_total counter\n")

	// Aggregate totals
	var totalMessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
	var totalBytesUp, totalBytesDown uint64

	for _, conn := range currentApp.PoolConnections {
		if conn == nil {
			continue
		}

		connID := strconv.FormatUint(conn.ID, 10)
		healthyStatus := "0"
		if conn.Healthy {
			healthyStatus = "1"
		}
		inUseStatus := "0"
		if conn.InUse {
			inUseStatus = "1"
		}

		// Queue metrics
		queueLength := len(conn.SendChan)
		queueCapacity := cap(conn.SendChan)

		metrics.WriteString(fmt.Sprintf("twt2_pool_connection_queue_length{connection_id=\"%s\",healthy=\"%s\",in_use=\"%s\"} %d\n",
			connID, healthyStatus, inUseStatus, queueLength))
		metrics.WriteString(fmt.Sprintf("twt2_pool_connection_queue_capacity{connection_id=\"%s\",healthy=\"%s\",in_use=\"%s\"} %d\n",
			connID, healthyStatus, inUseStatus, queueCapacity))

		// Connection statistics
		if conn.Stats != nil {
			counts, bytesUp, bytesDown, createdAt, lastMessageAt := conn.Stats.GetStats()

			for msgType, count := range counts {
				msgTypeName := strings.ToLower(msgType.String())
				metrics.WriteString(fmt.Sprintf("twt2_pool_connection_messages_total{connection_id=\"%s\",message_type=\"%s\",healthy=\"%s\",in_use=\"%s\"} %d\n",
					connID, msgTypeName, healthyStatus, inUseStatus, count))
				totalMessageCounts[msgType] += count
			}

			metrics.WriteString(fmt.Sprintf("twt2_pool_connection_bytes_total{connection_id=\"%s\",direction=\"up\",healthy=\"%s\",in_use=\"%s\"} %d\n",
				connID, healthyStatus, inUseStatus, bytesUp))
			metrics.WriteString(fmt.Sprintf("twt2_pool_connection_bytes_total{connection_id=\"%s\",direction=\"down\",healthy=\"%s\",in_use=\"%s\"} %d\n",
				connID, healthyStatus, inUseStatus, bytesDown))

			totalBytesUp += bytesUp
			totalBytesDown += bytesDown

			// Connection timing metrics
			metrics.WriteString("# HELP twt2_pool_connection_created_timestamp_seconds Unix timestamp when connection was created\n")
			metrics.WriteString("# TYPE twt2_pool_connection_created_timestamp_seconds gauge\n")
			metrics.WriteString(fmt.Sprintf("twt2_pool_connection_created_timestamp_seconds{connection_id=\"%s\",healthy=\"%s\",in_use=\"%s\"} %d\n",
				connID, healthyStatus, inUseStatus, createdAt.Unix()))

			metrics.WriteString("# HELP twt2_pool_connection_last_message_timestamp_seconds Unix timestamp of last message sent\n")
			metrics.WriteString("# TYPE twt2_pool_connection_last_message_timestamp_seconds gauge\n")
			metrics.WriteString(fmt.Sprintf("twt2_pool_connection_last_message_timestamp_seconds{connection_id=\"%s\",healthy=\"%s\",in_use=\"%s\"} %d\n",
				connID, healthyStatus, inUseStatus, lastMessageAt.Unix()))
		}

		// Last health check and last used timing
		metrics.WriteString("# HELP twt2_pool_connection_last_health_check_timestamp_seconds Unix timestamp of last health check\n")
		metrics.WriteString("# TYPE twt2_pool_connection_last_health_check_timestamp_seconds gauge\n")
		metrics.WriteString(fmt.Sprintf("twt2_pool_connection_last_health_check_timestamp_seconds{connection_id=\"%s\",healthy=\"%s\",in_use=\"%s\"} %d\n",
			connID, healthyStatus, inUseStatus, conn.LastHealthCheck.Unix()))

		metrics.WriteString("# HELP twt2_pool_connection_last_used_timestamp_seconds Unix timestamp when connection was last used\n")
		metrics.WriteString("# TYPE twt2_pool_connection_last_used_timestamp_seconds gauge\n")
		metrics.WriteString(fmt.Sprintf("twt2_pool_connection_last_used_timestamp_seconds{connection_id=\"%s\",healthy=\"%s\",in_use=\"%s\"} %d\n",
			connID, healthyStatus, inUseStatus, conn.LastUsed.Unix()))
	}

	// Aggregate metrics
	metrics.WriteString("# HELP twt2_messages_total Total messages processed by all pool connections\n")
	metrics.WriteString("# TYPE twt2_messages_total counter\n")
	for msgType, total := range totalMessageCounts {
		msgTypeName := strings.ToLower(msgType.String())
		metrics.WriteString(fmt.Sprintf("twt2_messages_total{message_type=\"%s\"} %d\n", msgTypeName, total))
	}

	metrics.WriteString("# HELP twt2_bytes_total Total bytes processed by all pool connections\n")
	metrics.WriteString("# TYPE twt2_bytes_total counter\n")
	metrics.WriteString(fmt.Sprintf("twt2_bytes_total{direction=\"up\"} %d\n", totalBytesUp))
	metrics.WriteString(fmt.Sprintf("twt2_bytes_total{direction=\"down\"} %d\n", totalBytesDown))
}

// generateServerMetrics generates Prometheus metrics for server-side
func generateServerMetrics(metrics *strings.Builder, currentApp *App) {
	// Get local and remote connection counts
	currentApp.LocalConnectionMutex.Lock()
	localConnCount := len(currentApp.LocalConnections)
	currentApp.LocalConnectionMutex.Unlock()

	currentApp.RemoteConnectionMutex.Lock()
	remoteConnCount := len(currentApp.RemoteConnections)
	currentApp.RemoteConnectionMutex.Unlock()

	// Server client connections count
	serverClientMutex.RLock()
	serverClientCount := len(serverClientConnections)
	activeServerClientCount := 0
	for _, conn := range serverClientConnections {
		if conn.Active {
			activeServerClientCount++
		}
	}
	serverClientMutex.RUnlock()

	// Connection overview metrics
	metrics.WriteString("# HELP twt2_server_local_connections_total Current number of local connections on server\n")
	metrics.WriteString("# TYPE twt2_server_local_connections_total gauge\n")
	metrics.WriteString(fmt.Sprintf("twt2_server_local_connections_total %d\n", localConnCount))

	metrics.WriteString("# HELP twt2_server_remote_connections_total Current number of remote connections on server\n")
	metrics.WriteString("# TYPE twt2_server_remote_connections_total gauge\n")
	metrics.WriteString(fmt.Sprintf("twt2_server_remote_connections_total %d\n", remoteConnCount))

	metrics.WriteString("# HELP twt2_server_client_connections_total Current number of client connections to server\n")
	metrics.WriteString("# TYPE twt2_server_client_connections_total gauge\n")
	metrics.WriteString(fmt.Sprintf("twt2_server_client_connections_total %d\n", serverClientCount))

	metrics.WriteString("# HELP twt2_server_client_connections_active Current number of active client connections to server\n")
	metrics.WriteString("# TYPE twt2_server_client_connections_active gauge\n")
	metrics.WriteString(fmt.Sprintf("twt2_server_client_connections_active %d\n", activeServerClientCount))

	// Get global server statistics
	serverStats.mutex.RLock()
	totalConnections := serverStats.TotalConnectionsHandled
	totalMessages := serverStats.TotalMessagesProcessed
	totalBytesUp := serverStats.TotalBytesUp
	totalBytesDown := serverStats.TotalBytesDown
	messageCounts := make(map[twtproto.ProxyComm_MessageType]uint64)
	for k, v := range serverStats.MessageCounts {
		messageCounts[k] = v
	}
	serverStats.mutex.RUnlock()

	// Server statistics metrics
	metrics.WriteString("# HELP twt2_server_connections_handled_total Total number of connections handled by server\n")
	metrics.WriteString("# TYPE twt2_server_connections_handled_total counter\n")
	metrics.WriteString(fmt.Sprintf("twt2_server_connections_handled_total %d\n", totalConnections))

	metrics.WriteString("# HELP twt2_server_messages_processed_total Total number of messages processed by server\n")
	metrics.WriteString("# TYPE twt2_server_messages_processed_total counter\n")
	metrics.WriteString(fmt.Sprintf("twt2_server_messages_processed_total %d\n", totalMessages))

	metrics.WriteString("# HELP twt2_server_bytes_total Total bytes processed by server\n")
	metrics.WriteString("# TYPE twt2_server_bytes_total counter\n")
	metrics.WriteString(fmt.Sprintf("twt2_server_bytes_total{direction=\"up\"} %d\n", totalBytesUp))
	metrics.WriteString(fmt.Sprintf("twt2_server_bytes_total{direction=\"down\"} %d\n", totalBytesDown))

	// Message type breakdown
	metrics.WriteString("# HELP twt2_server_messages_by_type_total Total messages processed by server by message type\n")
	metrics.WriteString("# TYPE twt2_server_messages_by_type_total counter\n")
	for msgType, count := range messageCounts {
		msgTypeName := strings.ToLower(msgType.String())
		metrics.WriteString(fmt.Sprintf("twt2_server_messages_by_type_total{message_type=\"%s\"} %d\n", msgTypeName, count))
	}

	// Server client connection details
	serverClientMutex.RLock()
	metrics.WriteString("# HELP twt2_server_client_connection_last_used_timestamp_seconds Unix timestamp when server client connection was last used\n")
	metrics.WriteString("# TYPE twt2_server_client_connection_last_used_timestamp_seconds gauge\n")
	for _, conn := range serverClientConnections {
		connID := strconv.FormatUint(conn.ID, 10)
		activeStatus := "0"
		if conn.Active {
			activeStatus = "1"
		}
		metrics.WriteString(fmt.Sprintf("twt2_server_client_connection_last_used_timestamp_seconds{connection_id=\"%s\",active=\"%s\"} %d\n",
			connID, activeStatus, conn.LastUsed.Unix()))
	}
	serverClientMutex.RUnlock()
}

// handleHTTPRequest handles HTTP requests (non-CONNECT) including PAC file requests
func handleHTTPRequest(w http.ResponseWriter, r *http.Request) {
	// Check if this is a PAC file request
	if r.Method == "GET" && (r.URL.Path == "/proxy.pac" || r.URL.Path == "/wpad.dat") {
		servePACFile(w, r)
		return
	}

	// Check if this is a metrics request
	if r.Method == "GET" && r.URL.Path == "/metrics" {
		serveMetrics(w, r)
		return
	}

	// For other HTTP requests, return a simple response
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("TW2\n"))
}

func Hijack(w http.ResponseWriter, r *http.Request) {
	// Check if app is initialized
	currentApp := getApp()
	if currentApp == nil {
		log.Errorf("App not initialized, cannot handle hijack request")
		http.Error(w, "Service not available", http.StatusServiceUnavailable)
		return
	}

	// Handle GET requests (including PAC file requests)
	if r.Method == "GET" {
		handleHTTPRequest(w, r)
		return
	}

	// Handle CONNECT requests (proxy tunneling)
	if r.Method != "CONNECT" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check proxy authentication if enabled
	if currentApp.ProxyAuthEnabled {
		log.Debugf("Proxy authentication enabled, checking credentials for %s from %s", r.Host, r.RemoteAddr)
		log.Debugf("Configured username: '%s', password configured: %t", currentApp.ProxyUsername, currentApp.ProxyPassword != "")
		if !authenticateProxyRequest(r, currentApp.ProxyUsername, currentApp.ProxyPassword) {
			log.Warnf("Proxy authentication failed for %s from %s", r.Host, r.RemoteAddr)
			sendProxyAuthRequired(w)
			return
		}
		log.Tracef("Proxy authentication successful for %s from %s", r.Host, r.RemoteAddr)
	}

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
	app.LocalConnections[thisConnection] = Connection{
		Connection:   conn,
		LastSeqIn:    0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*PendingAck),
		LastAckSent:  0,
	}
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
			if err == io.EOF {
				log.Infof("Client closed connection, going to send CLOSE_CONN_S message to remote end")
			} else {
				log.Infof("Error reading local connection: %v, going to send CLOSE_CONN_S message to remote end", err)
			}
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
			dataMessage := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_DATA_UP,
				Proxy:      proxyID,
				Connection: thisConnection,
				Seq:        connRecord.LastSeqIn,
				Data:       b[:n],
			}
			// Store message in PendingAcks for reliable delivery and track which pool connection is used
			if connRecord.PendingAcks == nil {
				connRecord.PendingAcks = make(map[uint64]*PendingAck)
			}

			// Send the message and get the pool connection ID used
			poolConnID := sendProtobufWithTracking(dataMessage)

			// Store the pending ACK with pool connection information
			connRecord.PendingAcks[connRecord.LastSeqIn] = &PendingAck{
				Message:          dataMessage,
				PoolConnectionID: poolConnID,
			}
			app.LocalConnections[thisConnection] = connRecord
			app.LocalConnectionMutex.Unlock()
		}
	}
}

func sendProtobufToConn(conn net.Conn, message *twtproto.ProxyComm) {
	// Check if connection is still valid before attempting to write
	if conn == nil {
		log.Tracef("Cannot send protobuf message: connection is nil")
		return
	}

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

	// Serialize writes to prevent frame corruption from concurrent writes
	protobufWriteMutex.Lock()
	defer protobufWriteMutex.Unlock()

	// Set write timeout to prevent blocking
	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	_, err = conn.Write(B)
	if err != nil {
		// Check if it's a closed connection error to reduce log noise
		if strings.Contains(err.Error(), "use of closed network connection") {
			log.Tracef("Connection closed while writing protobuf message: %v", err)
		} else {
			log.Warnf("Failed to write protobuf message: %v", err)
		}
		return
	}
	// Clear the deadline after successful write
	conn.SetWriteDeadline(time.Time{})
}

// isHighPriorityMessage determines if a message should be sent via the priority channel
func isHighPriorityMessage(message *twtproto.ProxyComm) bool {
	if message == nil {
		return false
	}

	switch message.Mt {
	case twtproto.ProxyComm_ACK_DOWN,
		twtproto.ProxyComm_ACK_UP,
		twtproto.ProxyComm_OPEN_CONN,
		twtproto.ProxyComm_PING:
		return true
	default:
		return false
	}
}

// sendProtobufWithTracking sends a protobuf message and returns the pool connection ID used
// Returns 0 if sent via server-side connection or fallback connection
func sendProtobufWithTracking(message *twtproto.ProxyComm) uint64 {
	// Check if message is nil
	if message == nil {
		log.Warnf("Cannot send nil protobuf message")
		return 0
	}

	// Check if app is initialized
	currentApp := getApp()
	if currentApp == nil {
		log.Errorf("App not initialized, cannot send protobuf message")
		return 0
	}

	// Check if we're in server mode (no pool connections)
	if len(currentApp.PoolConnections) == 0 {
		// Server mode - use regular sendProtobuf for server-side
		sendProtobuf(message)
		return 0 // Server-side connections don't have pool connection IDs
	}

	// Client mode - use pool connections and track which one was used
	currentApp.PoolMutex.Lock()
	defer currentApp.PoolMutex.Unlock()

	// Find a healthy available pool connection using priority-based selection with LRU
	var selectedConn *PoolConnection
	var healthyConnections []*PoolConnection

	// First pass - collect all healthy, available connections (highest priority)
	for _, poolConn := range currentApp.PoolConnections {
		if poolConn != nil && poolConn.Conn != nil && poolConn.Healthy && !poolConn.InUse {
			healthyConnections = append(healthyConnections, poolConn)
		}
	}

	// Second pass - if no available connections, collect healthy but busy ones
	if len(healthyConnections) == 0 {
		for _, poolConn := range currentApp.PoolConnections {
			if poolConn != nil && poolConn.Conn != nil && poolConn.Healthy {
				healthyConnections = append(healthyConnections, poolConn)
			}
		}
	}

	// Third pass - if no healthy connections, collect any available connections
	if len(healthyConnections) == 0 {
		for _, poolConn := range currentApp.PoolConnections {
			if poolConn != nil && poolConn.Conn != nil && !poolConn.InUse {
				healthyConnections = append(healthyConnections, poolConn)
			}
		}
	}

	// Fourth pass - if still no connections, collect any connection with a valid Conn
	if len(healthyConnections) == 0 {
		for _, poolConn := range currentApp.PoolConnections {
			if poolConn != nil && poolConn.Conn != nil {
				healthyConnections = append(healthyConnections, poolConn)
			}
		}
	}

	// Always use least recently used (LRU) selection from collected connections
	if len(healthyConnections) > 0 {
		selectedConn = healthyConnections[0]
		for _, poolConn := range healthyConnections {
			if poolConn.LastUsed.Before(selectedConn.LastUsed) {
				selectedConn = poolConn
			}
		}
	}

	if selectedConn == nil {
		log.Errorf("No healthy pool connections available (total: %d)", len(currentApp.PoolConnections))
		return 0
	}

	log.Tracef("Sending message via pool connection %d", selectedConn.ID)
	selectedConn.LastUsed = time.Now()

	// Determine which channel to use based on message priority
	isHighPriority := isHighPriorityMessage(message)
	targetChan := selectedConn.SendChan
	channelType := "regular"

	if isHighPriority && selectedConn.PriorityChan != nil {
		targetChan = selectedConn.PriorityChan
		channelType = "priority"
	}

	// Send message through the appropriate channel (non-blocking with timeout)
	select {
	case targetChan <- message:
		log.Tracef("Message queued for pool connection %d (%s channel)", selectedConn.ID, channelType)
		return selectedConn.ID
	case <-time.After(5 * time.Second):
		log.Warnf("Failed to queue message for pool connection %d (%s channel) - timeout", selectedConn.ID, channelType)
		// Try fallback to regular channel if priority channel failed
		if isHighPriority && channelType == "priority" {
			fallbackChan := selectedConn.SendChan
			select {
			case fallbackChan <- message:
				log.Tracef("Message queued for pool connection %d (fallback to regular channel)", selectedConn.ID)
				return selectedConn.ID
			default:
				log.Errorf("Failed to queue message for pool connection %d (fallback also failed)", selectedConn.ID)
				return 0
			}
		}
		return 0
	}
}

func sendProtobuf(message *twtproto.ProxyComm) {
	// Check if message is nil
	if message == nil {
		log.Warnf("Cannot send nil protobuf message")
		return
	}

	// Check if app is initialized
	currentApp := getApp()
	if currentApp == nil {
		log.Errorf("App not initialized, cannot send protobuf message")
		return
	}

	// Check if we're in server mode (no pool connections)
	if len(currentApp.PoolConnections) == 0 {
		// Server mode - use load balancing across client connections
		serverClientMutex.RLock()
		availableConnections := make([]*ServerClientConnection, 0)
		for _, clientConn := range serverClientConnections {
			if clientConn.Active && clientConn.Conn != nil {
				availableConnections = append(availableConnections, clientConn)
			}
		}
		serverClientMutex.RUnlock()

		if len(availableConnections) == 0 {
			log.Errorf("No active server client connections available for sending ACK_UP")
			// Fallback to global protobuf connection if no client connections available
			protobufConnectionMutex.RLock()
			conn := protobufConnection
			protobufConnectionMutex.RUnlock()

			if conn != nil {
				log.Tracef("Sending ACK_UP via fallback protobuf connection")
				sendProtobufToConn(conn, message)
				return
			}

			log.Errorf("No active server client connections and no fallback protobuf connection available")
			return
		}

		// Use LRU selection for server-side load balancing
		selectedConn := availableConnections[0]
		for _, clientConn := range availableConnections {
			if clientConn.LastUsed.Before(selectedConn.LastUsed) {
				selectedConn = clientConn
			}
		}

		log.Tracef("Sending ACK_UP via server client connection %d (LRU)", selectedConn.ID)
		selectedConn.LastUsed = time.Now()
		sendProtobufToConn(selectedConn.Conn, message)
		return
	}

	// Client mode - use pool connections
	currentApp.PoolMutex.Lock()
	// Note: We'll handle mutex unlock explicitly in each return path

	// Find a healthy available pool connection using priority-based selection with LRU
	var selectedConn *PoolConnection
	var healthyConnections []*PoolConnection

	// First pass - collect all healthy, available connections (highest priority)
	for _, poolConn := range currentApp.PoolConnections {
		if poolConn != nil && poolConn.Conn != nil && poolConn.Healthy && !poolConn.InUse {
			healthyConnections = append(healthyConnections, poolConn)
		}
	}

	// Second pass - if no available connections, collect healthy but busy ones
	if len(healthyConnections) == 0 {
		for _, poolConn := range currentApp.PoolConnections {
			if poolConn != nil && poolConn.Conn != nil && poolConn.Healthy {
				healthyConnections = append(healthyConnections, poolConn)
			}
		}
	}

	// Third pass - if no healthy connections, collect any available connections
	if len(healthyConnections) == 0 {
		for _, poolConn := range currentApp.PoolConnections {
			if poolConn != nil && poolConn.Conn != nil && !poolConn.InUse {
				healthyConnections = append(healthyConnections, poolConn)
			}
		}
	}

	// Fourth pass - if still no connections, collect any connection with a valid Conn
	if len(healthyConnections) == 0 {
		for _, poolConn := range currentApp.PoolConnections {
			if poolConn != nil && poolConn.Conn != nil {
				healthyConnections = append(healthyConnections, poolConn)
			}
		}
	}

	// Always use least recently used (LRU) selection from collected connections
	if len(healthyConnections) > 0 {
		selectedConn = healthyConnections[0]
		for _, poolConn := range healthyConnections {
			if poolConn.LastUsed.Before(selectedConn.LastUsed) {
				selectedConn = poolConn
			}
		}
	}

	if selectedConn == nil {
		log.Errorf("No healthy pool connections available (total: %d)", len(currentApp.PoolConnections))
		currentApp.PoolMutex.Unlock()
		return
	}

	log.Tracef("Sending message via pool connection %d", selectedConn.ID)
	selectedConn.LastUsed = time.Now()

	// Determine which channel to use based on message priority
	isHighPriority := isHighPriorityMessage(message)
	targetChan := selectedConn.SendChan
	channelType := "regular"

	if isHighPriority && selectedConn.PriorityChan != nil {
		targetChan = selectedConn.PriorityChan
		channelType = "priority"
	}

	// Send message through the appropriate channel (non-blocking with timeout)
	select {
	case targetChan <- message:
		log.Tracef("Message queued for pool connection %d (%s channel)", selectedConn.ID, channelType)
		currentApp.PoolMutex.Unlock()
		return
	default:
		log.Tracef("Send channel full for pool connection %d (%s channel, capacity: %d)", selectedConn.ID, channelType, cap(targetChan))
		// Try to find another connection instead of dropping the message
		for _, poolConn := range healthyConnections {
			if poolConn != selectedConn {
				// Use same priority channel for fallback, with nil check
				fallbackChan := poolConn.SendChan
				if isHighPriority && poolConn.PriorityChan != nil {
					fallbackChan = poolConn.PriorityChan
				}

				select {
				case fallbackChan <- message:
					log.Tracef("Message queued for alternative pool connection %d", poolConn.ID)
					poolConn.LastUsed = time.Now()
					currentApp.PoolMutex.Unlock()
					return
				default:
					continue // Try next connection
				}
			}
		}

		// All channels are full - wait for space instead of dropping
		log.Debugf("All pool connection channels are full - waiting for space to send message type %v for connection %d", message.Mt, message.Connection)

		// Release the mutex before entering retry loop to prevent deadlock
		currentApp.PoolMutex.Unlock()

		// Block and retry periodically until we can send the message
		ticker := time.NewTicker(10 * time.Millisecond) // Check every 10ms
		defer ticker.Stop()
		timeout := time.NewTimer(5 * time.Second) // Timeout after 5 seconds
		defer timeout.Stop()

		for {
			select {
			case <-timeout.C:
				log.Warnf("Timeout waiting to send message type %v for connection %d - dropping message", message.Mt, message.Connection)
				return
			case <-ticker.C:
				// Re-acquire the mutex for each retry attempt
				currentApp.PoolMutex.Lock()
				sent := false
				for _, poolConn := range currentApp.PoolConnections {
					if poolConn != nil && poolConn.Conn != nil && poolConn.Healthy {
						// Use appropriate channel based on priority, with nil check
						retryChan := poolConn.SendChan
						if isHighPriority && poolConn.PriorityChan != nil {
							retryChan = poolConn.PriorityChan
						}

						select {
						case retryChan <- message:
							log.Tracef("Message successfully queued for pool connection %d after waiting (%s channel)", poolConn.ID, channelType)
							poolConn.LastUsed = time.Now()
							sent = true
						default:
							continue // Channel still full, try next
						}
						if sent {
							break
						}
					}
				}
				currentApp.PoolMutex.Unlock()

				if sent {
					return // Successfully sent, exit function
				}
			}
		}
	}
}

// protobuf server

var protobufConnection net.Conn          // Global variable to store the protobuf connection for server responses
var protobufConnectionMutex sync.RWMutex // Mutex to protect protobufConnection
var protobufWriteMutex sync.Mutex        // Mutex to serialize protobuf writes and prevent frame corruption

// Global channels for undelivered messages during connection failures
var undeliveredMessageChan chan *UndeliveredMessage
var undeliveredPriorityMessageChan chan *UndeliveredMessage
var undeliveredMessageProcessor struct {
	running bool
	stop    chan struct{}
	done    chan struct{}
	mutex   sync.Mutex
}

// Ensure the undelivered message system is initialized only once
var undeliveredMessageSystemOnce sync.Once

// Global stats logger management
var statsLogger struct {
	running bool
	stop    chan struct{}
	done    chan struct{}
	ticker  *time.Ticker
	mutex   sync.Mutex
}

const undeliveredMessageBufferSize = 1000 // Buffer size for normal undelivered messages
const undeliveredPriorityBufferSize = 100 // Buffer size for priority undelivered messages

// UndeliveredMessage represents a message that couldn't be delivered due to connection failure
type UndeliveredMessage struct {
	Message      *twtproto.ProxyComm
	ConnectionID uint64
	IsLocal      bool // true for local connections, false for remote
	Timestamp    time.Time
	RetryCount   int
}

// Server-side client connection tracking for load balancing
type ServerClientConnection struct {
	Conn     net.Conn
	ID       uint64
	LastUsed time.Time
	Active   bool
}

var serverClientConnections []*ServerClientConnection
var serverClientMutex sync.RWMutex
var nextServerClientID uint64

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
	if index >= 0 && index < len(data) {
		return data[index]
	}
	return 0
}

func handleConnection(conn net.Conn) {
	handleConnectionWithPoolConn(conn, nil)
}

func handleConnectionWithPoolConn(conn net.Conn, poolConn *PoolConnection) {
	// Store the connection for server responses
	protobufConnectionMutex.Lock()
	protobufConnection = conn
	protobufConnectionMutex.Unlock()

	// Register this connection in server-side client pool for load balancing
	var serverClientConn *ServerClientConnection
	serverClientMutex.Lock()
	serverClientConn = &ServerClientConnection{
		Conn:     conn,
		ID:       nextServerClientID,
		LastUsed: time.Now(),
		Active:   true,
	}
	serverClientConnections = append(serverClientConnections, serverClientConn)
	nextServerClientID++
	serverClientMutex.Unlock()
	log.Debugf("Registered server client connection %d (total: %d)",
		serverClientConn.ID, len(serverClientConnections))

	// Send initial ping from server if enabled
	currentApp := getApp()
	if currentApp != nil && currentApp.Ping {
		pingMessage := &twtproto.ProxyComm{
			Mt:    twtproto.ProxyComm_PING,
			Proxy: 0, // Server-side ping
		}
		sendProtobufToConn(conn, pingMessage)
		log.Tracef("Server sent initial ping to client connection %d", serverClientConn.ID)
	}

	defer func() {
		// Handle server client connection failure with proper isolation
		if serverClientConn != nil {
			handleServerClientConnectionFailure(serverClientConn)
		}

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
			handleProxycommMessageWithPoolConn(message, poolConn)
		} else {
			log.Errorf("Error receiving protobuf message, expected %4d, got %4d", length, len(B))
			log.Warnf("Closing connection due to incomplete message - will trigger reconnection")
			return
		}
	}
}

func handleProxycommMessage(message *twtproto.ProxyComm) {
	handleProxycommMessageWithPoolConn(message, nil)
}

func handleProxycommMessageWithPoolConn(message *twtproto.ProxyComm, poolConn *PoolConnection) {
	// Check if app is initialized to prevent race condition crashes
	if app == nil {
		log.Warnf("Received message before app initialization, ignoring: %v", message.Mt)
		return
	}

	// Track all message processing for server statistics
	serverStats.mutex.Lock()
	if serverStats.MessageCounts == nil {
		serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
	}
	serverStats.TotalMessagesProcessed++
	serverStats.MessageCounts[message.Mt]++
	serverStats.mutex.Unlock()

	// Track statistics for the specific pool connection if provided (client side)
	if poolConn != nil && poolConn.Stats != nil {
		dataLen := 0
		if message.GetData() != nil {
			dataLen = len(message.GetData())
		}
		poolConn.Stats.IncrementMessage(message.Mt, dataLen)
	}

	// Update health check timestamp when successfully receiving messages (indicates healthy connection)
	if poolConn != nil {
		poolConn.LastHealthCheck = time.Now()
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

	// Handle ACK messages immediately without sequence checking
	// ACKs should be processed regardless of sequence order
	if message.Mt == twtproto.ProxyComm_ACK_UP {
		handleAckUp(message)
		return
	}
	if message.Mt == twtproto.ProxyComm_ACK_DOWN {
		handleAckDown(message)
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
			// Initialize MessageQueue if it's nil
			if thisConnection.MessageQueue == nil {
				thisConnection.MessageQueue = make(map[uint64]*twtproto.ProxyComm)
				app.LocalConnections[message.Connection] = thisConnection
			}
			app.LocalConnections[message.Connection].MessageQueue[message.Seq] = message
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
			app.RemoteConnections[message.Connection] = Connection{
				Connection:   nil,
				LastSeqIn:    0,
				NextSeqOut:   0,
				MessageQueue: make(map[uint64]*twtproto.ProxyComm),
				PendingAcks:  make(map[uint64]*PendingAck),
				LastAckSent:  0,
			}
			thisConnection = app.RemoteConnections[message.Connection]
		}
		log.Tracef("Seq UP %d %d", message.Seq, thisConnection.NextSeqOut)
		if message.Seq != thisConnection.NextSeqOut {
			if message.Mt == twtproto.ProxyComm_CLOSE_CONN_S {
				log.Tracef("Out of order processing of %v message for connection %d seq %d", message.Mt, message.Connection, message.Seq)
				closeConnectionRemoteNoLock(message)
				mutex.Unlock()
				return
			}
			log.Tracef("Queueing message UP conn %d seq %d", message.Connection, message.Seq)
			// Initialize MessageQueue if it's nil
			if thisConnection.MessageQueue == nil {
				thisConnection.MessageQueue = make(map[uint64]*twtproto.ProxyComm)
				app.RemoteConnections[message.Connection] = thisConnection
			}
			app.RemoteConnections[message.Connection].MessageQueue[message.Seq] = message
			mutex.Unlock()
			return
		}
	}
	log.Tracef("Handling current message for connection %d", message.Connection)
	switch message.Mt {
	case twtproto.ProxyComm_OPEN_CONN:
		newConnectionNoLock(message)
	case twtproto.ProxyComm_CLOSE_CONN_C:
		closeConnectionLocalNoLock(message)
	case twtproto.ProxyComm_DATA_DOWN:
		backwardDataChunk(message)
	case twtproto.ProxyComm_CLOSE_CONN_S:
		closeConnectionRemoteNoLock(message)
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
			closeConnectionLocalNoLock(queueMessage)
		case twtproto.ProxyComm_DATA_DOWN:
			backwardDataChunk(queueMessage)
		case twtproto.ProxyComm_CLOSE_CONN_S:
			closeConnectionRemoteNoLock(queueMessage)
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
	app.RemoteConnectionMutex.Lock()
	defer app.RemoteConnectionMutex.Unlock()

	newConnectionNoLock(message)
}

func newConnectionNoLock(message *twtproto.ProxyComm) {
	log.Infof("Openning connection %4d to %s:%d", message.Connection, message.Address, message.Port)

	// Track this connection for server statistics
	serverStats.mutex.Lock()
	serverStats.TotalConnectionsHandled++
	serverStats.mutex.Unlock()

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

		// Track outgoing CLOSE_CONN_C messages on server side
		serverStats.mutex.Lock()
		if serverStats.MessageCounts == nil {
			serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
		}
		serverStats.MessageCounts[twtproto.ProxyComm_CLOSE_CONN_C]++
		serverStats.mutex.Unlock()

		sendProtobuf(closeMessage)
		return
	}
	thisConnection, ok := app.RemoteConnections[message.Connection]
	if !ok {
		log.Tracef("Connection %4d not known, creating record", message.Connection)
		app.RemoteConnections[message.Connection] = Connection{
			Connection:   conn,
			LastSeqIn:    0,
			NextSeqOut:   1,
			MessageQueue: make(map[uint64]*twtproto.ProxyComm),
			PendingAcks:  make(map[uint64]*PendingAck),
			LastAckSent:  0,
		}
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

	app.RemoteConnectionMutex.Lock()
	defer app.RemoteConnectionMutex.Unlock()

	closeConnectionRemoteNoLock(message)
}

func closeConnectionRemoteNoLock(message *twtproto.ProxyComm) {
	connRecord, ok := app.RemoteConnections[message.Connection]
	if ok {
		conn := connRecord.Connection

		// Explicitly clear buffer maps to free memory immediately
		connRecord.MessageQueue = nil
		connRecord.PendingAcks = nil

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

	app.LocalConnectionMutex.Lock()
	defer app.LocalConnectionMutex.Unlock()

	closeConnectionLocalNoLock(message)
}

func closeConnectionLocalNoLock(message *twtproto.ProxyComm) {
	connRecord, ok := app.LocalConnections[message.Connection]
	if ok {
		conn := connRecord.Connection

		// Explicitly clear buffer maps to free memory immediately
		connRecord.MessageQueue = nil
		connRecord.PendingAcks = nil

		delete(app.LocalConnections, message.Connection)
		time.AfterFunc(1*time.Second, func() { conn.Close() })
	}
}

func backwardDataChunk(message *twtproto.ProxyComm) {
	//  log.Tracef("DATA_DOWN %v", message)

	// Track server statistics (bytes only, message count is tracked in handleProxycommMessage)
	serverStats.mutex.Lock()
	serverStats.TotalBytesDown += uint64(len(message.Data))
	serverStats.mutex.Unlock()

	// Get app safely - note: LocalConnectionMutex is already held by caller
	currentApp := getApp()
	if currentApp == nil {
		log.Errorf("App not initialized, cannot forward data chunk")
		return
	}

	// Note: LocalConnectionMutex is already held by caller, so we don't acquire it
	thisConnection, exists := currentApp.LocalConnections[message.Connection]
	if !exists {
		log.Errorf("Unknown local connection %d for DATA_DOWN", message.Connection)
		return
	}
	thisConnection.NextSeqOut++
	currentApp.LocalConnections[message.Connection] = thisConnection

	n, err := thisConnection.Connection.Write(message.Data)
	if err != nil {
		log.Debugf("Error forwarding data chunk downward for connection %4d, seq %8d, length %5d, %v", message.Connection, message.Seq, len(message.Data), err)
		return
	}
	log.Debugf("Succesfully forwarded data chunk downward for connection %4d, seq %8d, length %5d, sent %5d", message.Connection, message.Seq, len(message.Data), n)

	// Send ACK_DOWN to acknowledge successful receipt of DATA_DOWN
	ackMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_ACK_DOWN,
		Proxy:      proxyID,
		Connection: message.Connection,
		Seq:        message.Seq,
	}
	sendProtobuf(ackMessage)
	log.Tracef("Sent ACK_DOWN for connection %d, seq %d", message.Connection, message.Seq)
}

func forwardDataChunk(message *twtproto.ProxyComm) {
	// Track server statistics (bytes only, message count is tracked in handleProxycommMessage)
	serverStats.mutex.Lock()
	serverStats.TotalBytesUp += uint64(len(message.Data))
	serverStats.mutex.Unlock()

	// Send ACK_UP immediately upon receiving DATA_UP - this MUST happen first
	// The client needs to know we received the message regardless of forwarding success
	ackMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_ACK_UP,
		Proxy:      proxyID,
		Connection: message.Connection,
		Seq:        message.Seq,
	}
	log.Debugf("Attempting to send ACK_UP for connection %d, seq %d", message.Connection, message.Seq)
	sendProtobuf(ackMessage)
	log.Tracef("Sent ACK_UP for connection %d, seq %d", message.Connection, message.Seq)

	// Get app safely - note: RemoteConnectionMutex is already held by caller
	currentApp := getApp()
	if currentApp == nil {
		log.Errorf("App not initialized, cannot forward data chunk")
		return
	}

	// Note: RemoteConnectionMutex is already held by caller, so we don't acquire it
	thisConnection, exists := currentApp.RemoteConnections[message.Connection]
	if !exists {
		log.Errorf("Unknown remote connection %d for DATA_UP", message.Connection)
		return
	}
	thisConnection.NextSeqOut++
	currentApp.RemoteConnections[message.Connection] = thisConnection

	n, err := thisConnection.Connection.Write(message.Data)
	if err != nil {
		log.Debugf("Error forwarding data chunk   upward for connection %4d, seq %8d, length %5d, %v", message.Connection, message.Seq, len(message.Data), err)
		return
	}
	log.Debugf("Succesfully forwarded data chunk   upward for connection %4d, seq %8d, length %5d, sent %5d", message.Connection, message.Seq, len(message.Data), n)
}

// handleAckDown processes ACK messages for DATA_DOWN (acknowledgment from remote to local)
func handleAckDown(message *twtproto.ProxyComm) {
	log.Tracef("ACK_DOWN received for connection %d, seq %d", message.Connection, message.Seq)

	// Get app safely
	currentApp := getApp()
	if currentApp == nil {
		log.Warnf("ACK_DOWN received but app is nil")
		return
	}

	// Find the connection in remote connections (where DATA_DOWN was sent from)
	currentApp.RemoteConnectionMutex.Lock()
	defer currentApp.RemoteConnectionMutex.Unlock()

	thisConnection, ok := currentApp.RemoteConnections[message.Connection]
	if !ok {
		log.Tracef("Connection %d not found for ACK_DOWN", message.Connection)
		return
	}

	// Initialize maps if they don't exist (for backward compatibility)
	if thisConnection.PendingAcks == nil {
		thisConnection.PendingAcks = make(map[uint64]*PendingAck)
	}

	// Remove the acknowledged message from pending ACKs
	delete(thisConnection.PendingAcks, message.Seq)

	log.Debugf("ACK_DOWN processed for connection %d, seq %d - removed from pending", message.Connection, message.Seq)
}

// handleAckUp processes ACK messages for DATA_UP (acknowledgment from local to remote)
func handleAckUp(message *twtproto.ProxyComm) {
	log.Tracef("ACK_UP received for connection %d, seq %d", message.Connection, message.Seq)

	// Get app safely
	currentApp := getApp()
	if currentApp == nil {
		log.Warnf("ACK_UP received but app is nil")
		return
	}

	// Find the connection in local connections (where DATA_UP was sent from)
	currentApp.LocalConnectionMutex.Lock()
	defer currentApp.LocalConnectionMutex.Unlock()

	thisConnection, ok := currentApp.LocalConnections[message.Connection]
	if !ok {
		log.Tracef("Connection %d not found for ACK_UP", message.Connection)
		return
	}

	// Initialize maps if they don't exist (for backward compatibility)
	if thisConnection.PendingAcks == nil {
		thisConnection.PendingAcks = make(map[uint64]*PendingAck)
	}

	// Remove the acknowledged message from pending ACKs
	delete(thisConnection.PendingAcks, message.Seq)

	log.Debugf("ACK_UP processed for connection %d, seq %d - removed from pending", message.Connection, message.Seq)
}

func handleRemoteSideConnection(conn net.Conn, connID uint64) {
	log.Infof("Starting remote side connection handler for connection %d", connID)
	b := make([]byte, chunkSize)
	for {
		n, err := conn.Read(b)
		if err != nil {
			if err == io.EOF {
				log.Infof("Server closed connection %d, going to send CLOSE_CONN_C message to client end", connID)
				currentApp := getApp()
				if currentApp == nil {
					log.Tracef("App is nil, exiting remote connection handler for connection %d", connID)
					conn.Close()
					return
				}
				currentApp.RemoteConnectionMutex.Lock()
				connRecord, ok := currentApp.RemoteConnections[connID]
				if !ok {
					log.Tracef("Connection %d was already closed and removed earlier, exiting goroutine", connID)
					currentApp.RemoteConnectionMutex.Unlock()
					return
				}
				seq := connRecord.LastSeqIn
				delete(currentApp.RemoteConnections, connID)
				currentApp.RemoteConnectionMutex.Unlock()
				closeMessage := &twtproto.ProxyComm{
					Mt:         twtproto.ProxyComm_CLOSE_CONN_C,
					Proxy:      proxyID,
					Connection: connID,
					Seq:        seq,
				}

				// Track outgoing CLOSE_CONN_C messages on server side
				serverStats.mutex.Lock()
				if serverStats.MessageCounts == nil {
					serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
				}
				serverStats.MessageCounts[twtproto.ProxyComm_CLOSE_CONN_C]++
				serverStats.mutex.Unlock()

				sendProtobuf(closeMessage)
				return
			} else {
				log.Infof("Error reading remote connection %d: %v", connID, err)
			}
			currentApp := getApp()
			if currentApp != nil {
				currentApp.RemoteConnectionMutex.Lock()
				delete(currentApp.RemoteConnections, connID)
				currentApp.RemoteConnectionMutex.Unlock()
			}
			conn.Close()
			return
		}
		log.Tracef("Sending data from remote connection %4d downward, length %5d", connID, n)
		currentApp := getApp()
		if currentApp == nil {
			conn.Close()
			return
		}
		currentApp.RemoteConnectionMutex.Lock()
		connRecord := currentApp.RemoteConnections[connID]
		seq := connRecord.LastSeqIn
		connRecord.LastSeqIn++
		dataMessage := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Proxy:      proxyID,
			Connection: connID,
			Seq:        seq,
			Data:       b[:n],
		}
		// Store message in PendingAcks for reliable delivery and track which pool connection is used
		if connRecord.PendingAcks == nil {
			connRecord.PendingAcks = make(map[uint64]*PendingAck)
		}

		// Send the message and get the pool connection ID used (0 for server-side)
		poolConnID := sendProtobufWithTracking(dataMessage)

		// Store the pending ACK with pool connection information
		connRecord.PendingAcks[seq] = &PendingAck{
			Message:          dataMessage,
			PoolConnectionID: poolConnID,
		}
		currentApp.RemoteConnections[connID] = connRecord
		currentApp.RemoteConnectionMutex.Unlock()
		//    log.Tracef("%s", hex.Dump(b[:n]))

		// Track outgoing DATA_DOWN messages on server side
		serverStats.mutex.Lock()
		if serverStats.MessageCounts == nil {
			serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
		}
		serverStats.MessageCounts[twtproto.ProxyComm_DATA_DOWN]++
		serverStats.mutex.Unlock()

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
