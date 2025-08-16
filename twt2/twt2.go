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
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			logConnectionPoolStats()
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

	if isClientSide {
		// Client side - log pool connection statistics
		currentApp.PoolMutex.Lock()
		defer currentApp.PoolMutex.Unlock()

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

type Connection struct {
	Connection   net.Conn
	LastSeqIn    uint64                         // last sequence number we have used for the last incoming chunk
	NextSeqOut   uint64                         // next sequence number to be sent out
	MessageQueue map[uint64]*twtproto.ProxyComm // queue of messages with too high sequence numbers
	// ACK tracking fields for reliable message delivery
	PendingAcks map[uint64]*twtproto.ProxyComm // messages waiting for ACK, keyed by sequence number
	AckTimeouts map[uint64]time.Time           // timeout timestamps for pending ACKs
	LastAckSent uint64                         // last sequence number we acknowledged
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

	// Only create pool connections on client side
	if isClient && peerHost != "" {
		log.Debug("Creating bidirectional connection pool (client side)")

		// Initialize pool connections in parallel groups of 10
		appInstance.PoolConnections = createPoolConnectionsParallel(poolInit, appInstance.PeerHost, appInstance.PeerPort, ping, sshUser, sshKeyPath, appInstance.SSHPort)

		log.Debugf("Created %d pool connections", len(appInstance.PoolConnections))

		// Start connection health monitoring
		startConnectionHealthMonitor()
		log.Debug("Started connection health monitoring")

		// Start ACK timeout checking
		startAckTimeoutChecker(appInstance)
		log.Debug("Started ACK timeout monitoring")
	} else {
		log.Debug("Server side - no pool connections created")

		// Start ACK timeout checking on server side too
		startAckTimeoutChecker(appInstance)
		log.Debug("Started ACK timeout monitoring (server side)")
	}

	// Start statistics logging on both client and server side
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
		Conn:            remoteConn,
		SendChan:        make(chan *twtproto.ProxyComm, 100),
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
						log.Warnf("Failed to create pool connection %d, creating placeholder for retry", connID)
						// Create a placeholder pool connection that will retry using existing reconnection logic
						retryCtx, retryCancel := context.WithCancel(context.Background())
						placeholderConn := &PoolConnection{
							Conn:            nil, // No connection initially
							SendChan:        make(chan *twtproto.ProxyComm, 100),
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

	for _, poolConn := range currentApp.PoolConnections {
		if poolConn.retryCancel != nil {
			poolConn.retryCancel()
		}
	}
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

		for range ticker.C {
			currentApp := getApp()
			if currentApp == nil {
				continue
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
						log.Debugf("Pool connection %d appears stale (last check %v ago), checking channel queue",
							poolConn.ID, time.Since(poolConn.LastHealthCheck))

						// Check if send channel is getting full
						queuedMessages := len(poolConn.SendChan)
						capacity := cap(poolConn.SendChan)
						if queuedMessages > capacity/2 {
							log.Warnf("Pool connection %d has %d/%d messages queued - possible congestion",
								poolConn.ID, queuedMessages, capacity)

							// If queue is nearly full, mark as unhealthy to trigger reconnection
							if queuedMessages > capacity*3/4 {
								log.Errorf("Pool connection %d queue nearly full (%d/%d) - marking unhealthy",
									poolConn.ID, queuedMessages, capacity)
								poolConn.Healthy = false
							}
						}
					}
				}
			}

			log.Debugf("Connection health: %d/%d healthy connections", healthyCount, totalCount)
			currentApp.PoolMutex.Unlock()
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

// startAckTimeoutChecker monitors pending ACKs and retransmits timed-out messages
func startAckTimeoutChecker(appInstance *App) {
	go func() {
		ticker := time.NewTicker(1 * time.Second) // Check every second
		defer ticker.Stop()

		for {
			select {
			case <-appInstance.ctx.Done():
				return // Stop when context is cancelled
			case <-ticker.C:
				// Get app safely
				currentApp := getApp()
				if currentApp == nil {
					continue
				}

				now := time.Now()

				// Check local connections for timed-out DATA_UP messages
				currentApp.LocalConnectionMutex.Lock()
				for connID, connection := range currentApp.LocalConnections {
					// Initialize maps if they don't exist (for backward compatibility)
					if connection.PendingAcks == nil {
						connection.PendingAcks = make(map[uint64]*twtproto.ProxyComm)
					}
					if connection.AckTimeouts == nil {
						connection.AckTimeouts = make(map[uint64]time.Time)
					}

					for seq, timeout := range connection.AckTimeouts {
						if now.After(timeout) {
							// Timeout occurred, retransmit the message
							if pendingMsg, exists := connection.PendingAcks[seq]; exists {
								log.Warnf("ACK timeout for DATA_UP connection %d, seq %d - retransmitting", connID, seq)
								// Update timeout for next attempt
								connection.AckTimeouts[seq] = now.Add(5 * time.Second)
								// Retransmit the message
								sendProtobuf(pendingMsg)
							}
						}
					}
				}
				currentApp.LocalConnectionMutex.Unlock()

				// Check remote connections for timed-out DATA_DOWN messages
				currentApp.RemoteConnectionMutex.Lock()
				for connID, connection := range currentApp.RemoteConnections {
					// Initialize maps if they don't exist (for backward compatibility)
					if connection.PendingAcks == nil {
						connection.PendingAcks = make(map[uint64]*twtproto.ProxyComm)
					}
					if connection.AckTimeouts == nil {
						connection.AckTimeouts = make(map[uint64]time.Time)
					}

					for seq, timeout := range connection.AckTimeouts {
						if now.After(timeout) {
							// Timeout occurred, retransmit the message
							if pendingMsg, exists := connection.PendingAcks[seq]; exists {
								log.Warnf("ACK timeout for DATA_DOWN connection %d, seq %d - retransmitting", connID, seq)
								// Update timeout for next attempt
								connection.AckTimeouts[seq] = now.Add(5 * time.Second)
								// Retransmit the message
								sendProtobuf(pendingMsg)
							}
						}
					}
				}
				currentApp.RemoteConnectionMutex.Unlock()
			}
		}
	}()
}

func poolConnectionSender(poolConn *PoolConnection) {
	if poolConn == nil {
		log.Errorf("poolConnectionSender called with nil poolConn")
		return
	}

	log.Tracef("Starting sender goroutine for pool connection %d", poolConn.ID)

	for message := range poolConn.SendChan {
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

		// Clear any connection state that might be corrupted (only if we had a connection)
		if poolConn.Conn != nil {
			clearConnectionState()
		}

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
			newPoolConn := createPoolConnection(poolConn.ID, currentApp.PeerHost, currentApp.PeerPort, currentApp.Ping, currentApp.SSHUser, currentApp.SSHKeyPath, currentApp.SSHPort)
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

				// Send ping if enabled
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

// clearConnectionState clears potentially corrupted connection state
func clearConnectionState() {
	currentApp := getApp() // Use safe app access
	if currentApp == nil {
		return
	}

	log.Warnf("Clearing connection state due to connection failure")

	// Clear local connections (client side)
	currentApp.LocalConnectionMutex.Lock()
	localCount := len(currentApp.LocalConnections)
	for connID, conn := range currentApp.LocalConnections {
		if conn.Connection != nil {
			conn.Connection.Close()
		}
		delete(currentApp.LocalConnections, connID)
	}
	currentApp.LocalConnectionMutex.Unlock()

	// Clear remote connections (server side)
	currentApp.RemoteConnectionMutex.Lock()
	remoteCount := len(currentApp.RemoteConnections)
	for connID, conn := range currentApp.RemoteConnections {
		if conn.Connection != nil {
			conn.Connection.Close()
		}
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

	log.Infof("Cleared %d local and %d remote connections", localCount, remoteCount)
}

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.DefaultRoute(w, r)
}

// authenticateProxyRequest checks if the request has valid proxy authentication
func authenticateProxyRequest(r *http.Request, username, password string) bool {
	if username == "" && password == "" {
		return true // No authentication required
	}

	// Get the Proxy-Authorization header
	proxyAuth := r.Header.Get("Proxy-Authorization")
	if proxyAuth == "" {
		return false
	}

	// Check if it's Basic authentication
	if !strings.HasPrefix(proxyAuth, "Basic ") {
		return false
	}

	// Decode the base64 credentials
	encoded := strings.TrimPrefix(proxyAuth, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return false
	}

	// Split username:password
	credentials := strings.SplitN(string(decoded), ":", 2)
	if len(credentials) != 2 {
		return false
	}

	// Use constant-time comparison to prevent timing attacks
	usernameMatch := subtle.ConstantTimeCompare([]byte(credentials[0]), []byte(username)) == 1
	passwordMatch := subtle.ConstantTimeCompare([]byte(credentials[1]), []byte(password)) == 1

	return usernameMatch && passwordMatch
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
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
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
			dataMessage := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_DATA_UP,
				Proxy:      proxyID,
				Connection: thisConnection,
				Seq:        connRecord.LastSeqIn,
				Data:       b[:n],
			}
			// Store message in PendingAcks for reliable delivery
			if connRecord.PendingAcks == nil {
				connRecord.PendingAcks = make(map[uint64]*twtproto.ProxyComm)
			}
			if connRecord.AckTimeouts == nil {
				connRecord.AckTimeouts = make(map[uint64]time.Time)
			}
			connRecord.PendingAcks[connRecord.LastSeqIn] = dataMessage
			connRecord.AckTimeouts[connRecord.LastSeqIn] = time.Now().Add(5 * time.Second) // 5 second timeout
			app.LocalConnections[thisConnection] = connRecord
			app.LocalConnectionMutex.Unlock()
			sendProtobuf(dataMessage)
		}
	}
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

	// Serialize writes to prevent frame corruption from concurrent writes
	protobufWriteMutex.Lock()
	defer protobufWriteMutex.Unlock()

	// Set write timeout to prevent blocking
	conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
	_, err = conn.Write(B)
	if err != nil {
		log.Warnf("Failed to write protobuf message: %v", err)
		return
	}
	// Clear the deadline after successful write
	conn.SetWriteDeadline(time.Time{})
}

func sendProtobuf(message *twtproto.ProxyComm) {
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
			// Fallback to global protobuf connection if no client connections available
			protobufConnectionMutex.RLock()
			conn := protobufConnection
			protobufConnectionMutex.RUnlock()

			if conn != nil {
				log.Tracef("Sending message via fallback protobuf connection")
				sendProtobufToConn(conn, message)
				return
			}

			log.Errorf("No active server client connections available")
			return
		}

		// Use LRU selection for server-side load balancing
		selectedConn := availableConnections[0]
		for _, clientConn := range availableConnections {
			if clientConn.LastUsed.Before(selectedConn.LastUsed) {
				selectedConn = clientConn
			}
		}

		log.Tracef("Sending message via server client connection %d (LRU)", selectedConn.ID)
		selectedConn.LastUsed = time.Now()
		sendProtobufToConn(selectedConn.Conn, message)
		return
	}

	// Client mode - use pool connections
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
		return
	}

	log.Tracef("Sending message via pool connection %d", selectedConn.ID)
	selectedConn.LastUsed = time.Now()

	// Send message through the channel (non-blocking with timeout)
	select {
	case selectedConn.SendChan <- message:
		log.Tracef("Message queued for pool connection %d", selectedConn.ID)
	default:
		log.Warnf("Send channel full for pool connection %d (capacity: %d)", selectedConn.ID, cap(selectedConn.SendChan))
		// Try to find another connection instead of dropping the message
		for _, poolConn := range healthyConnections {
			if poolConn != selectedConn {
				select {
				case poolConn.SendChan <- message:
					log.Tracef("Message queued for alternative pool connection %d", poolConn.ID)
					poolConn.LastUsed = time.Now()
					return
				default:
					continue // Try next connection
				}
			}
		}
		log.Errorf("All pool connection channels are full - dropping message type %v for connection %d", message.Mt, message.Connection)
	}
}

// protobuf server

var protobufConnection net.Conn          // Global variable to store the protobuf connection for server responses
var protobufConnectionMutex sync.RWMutex // Mutex to protect protobufConnection
var protobufWriteMutex sync.Mutex        // Mutex to serialize protobuf writes and prevent frame corruption

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

	defer func() {
		// Remove this connection from server-side pool when it closes
		if serverClientConn != nil {
			serverClientMutex.Lock()
			for i, conn := range serverClientConnections {
				if conn.ID == serverClientConn.ID {
					// Remove connection from slice
					serverClientConnections = append(serverClientConnections[:i], serverClientConnections[i+1:]...)
					break
				}
			}
			serverClientMutex.Unlock()
			log.Debugf("Unregistered server client connection %d (remaining: %d)",
				serverClientConn.ID, len(serverClientConnections))
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
				PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
				AckTimeouts:  make(map[uint64]time.Time),
				LastAckSent:  0,
			}
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
			app.RemoteConnections[message.Connection].MessageQueue[message.Seq] = message
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
			PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
			AckTimeouts:  make(map[uint64]time.Time),
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
		thisConnection.PendingAcks = make(map[uint64]*twtproto.ProxyComm)
	}
	if thisConnection.AckTimeouts == nil {
		thisConnection.AckTimeouts = make(map[uint64]time.Time)
	}

	// Remove the acknowledged message from pending ACKs and timeouts
	delete(thisConnection.PendingAcks, message.Seq)
	delete(thisConnection.AckTimeouts, message.Seq)

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
		thisConnection.PendingAcks = make(map[uint64]*twtproto.ProxyComm)
	}
	if thisConnection.AckTimeouts == nil {
		thisConnection.AckTimeouts = make(map[uint64]time.Time)
	}

	// Remove the acknowledged message from pending ACKs and timeouts
	delete(thisConnection.PendingAcks, message.Seq)
	delete(thisConnection.AckTimeouts, message.Seq)

	log.Debugf("ACK_UP processed for connection %d, seq %d - removed from pending", message.Connection, message.Seq)
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
			log.Tracef("Error reading remote connection %d: %v, exiting goroutine", connID, err)
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
		// Store message in PendingAcks for reliable delivery
		if connRecord.PendingAcks == nil {
			connRecord.PendingAcks = make(map[uint64]*twtproto.ProxyComm)
		}
		if connRecord.AckTimeouts == nil {
			connRecord.AckTimeouts = make(map[uint64]time.Time)
		}
		connRecord.PendingAcks[seq] = dataMessage
		connRecord.AckTimeouts[seq] = time.Now().Add(5 * time.Second) // 5 second timeout
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
