package twt2

import (
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"palecci.cz/twtproto"
)

// TestServerSideDownstreamLoadBalancing verifies that the server distributes DATA_DOWN across multiple client connections
func TestServerSideDownstreamLoadBalancing(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Reset server-side client connections
	serverClientMutex.Lock()
	serverClientConnections = []*ServerClientConnection{}
	nextServerClientID = 0
	serverClientMutex.Unlock()

	// Initialize server stats
	serverStats.mutex.Lock()
	if serverStats.MessageCounts == nil {
		serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
	}
	for k := range serverStats.MessageCounts {
		serverStats.MessageCounts[k] = 0
	}
	serverStats.TotalBytesDown = 0
	serverStats.TotalBytesUp = 0
	serverStats.mutex.Unlock()

	// Create app in server mode (no pool connections)
	app = &App{
		PoolConnections:       []*PoolConnection{}, // Empty = server mode
		PoolMutex:             sync.Mutex{},
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Create mock client connections (simulate multiple clients connecting to server)
	mockConnections := make([]net.Conn, 3)
	for i := 0; i < 3; i++ {
		client, server := net.Pipe()
		mockConnections[i] = server

		// Register each connection as a server-side client connection
		serverClientMutex.Lock()
		serverClientConn := &ServerClientConnection{
			Conn:     server,
			ID:       uint64(i),
			LastUsed: time.Now().Add(-time.Duration(3-i) * time.Hour), // Different LastUsed times
			Active:   true,
		}
		serverClientConnections = append(serverClientConnections, serverClientConn)
		nextServerClientID++
		serverClientMutex.Unlock()

		// Close client side to avoid resource leaks
		defer client.Close()
		defer server.Close()
	}

	// Track which server client connections receive DATA_DOWN messages
	messageTracker := make(map[uint64]int)
	receivedMessages := make(chan struct {
		connID uint64
		msg    *twtproto.ProxyComm
	}, 100)

	// Start goroutines to monitor each connection for received messages
	for i, conn := range mockConnections {
		go func(connID uint64, mockConn net.Conn) {
			for {
				// Try to read a message (this is a simplified version)
				buffer := make([]byte, 2)
				_, err := mockConn.Read(buffer)
				if err != nil {
					return // Connection closed
				}

				length := int(buffer[1])*256 + int(buffer[0])
				if length > 0 && length < 1024 { // Reasonable message size
					data := make([]byte, length)
					n, err := mockConn.Read(data)
					if err != nil || n != length {
						continue
					}

					// Try to unmarshal the protobuf message
					message := &twtproto.ProxyComm{}
					if err := proto.Unmarshal(data, message); err == nil {
						if message.Mt == twtproto.ProxyComm_DATA_DOWN {
							receivedMessages <- struct {
								connID uint64
								msg    *twtproto.ProxyComm
							}{connID, message}
						}
					}
				}
			}
		}(uint64(i), conn)
	}

	// Send multiple DATA_DOWN messages that should be load balanced
	for i := 0; i < 9; i++ {
		dataDownMessage := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Proxy:      0,
			Connection: uint64(100 + i%3), // Different connection IDs
			Seq:        uint64(i),
			Data:       []byte("server downstream data"),
		}

		// This should trigger server-side load balancing
		sendProtobuf(dataDownMessage)

		// Small delay to ensure messages are processed
		time.Sleep(1 * time.Millisecond)
	}

	// Collect results with timeout
	timeout := time.After(100 * time.Millisecond)
	for {
		select {
		case received := <-receivedMessages:
			messageTracker[received.connID]++
			t.Logf("DATA_DOWN message received by server client connection %d", received.connID)

			if len(messageTracker) >= 3 && getTotalMessages(messageTracker) >= 6 {
				// We have enough evidence of load balancing
				goto checkResults
			}
		case <-timeout:
			goto checkResults
		}
	}

checkResults:
	t.Logf("Server-side downstream distribution: %v", messageTracker)

	// Verify that multiple connections received messages (load balancing working)
	if len(messageTracker) < 2 {
		t.Errorf("Server-side load balancing failed - only %d connections received messages", len(messageTracker))
	}

	// Verify each connection that received messages got at least 1
	for connID, count := range messageTracker {
		if count < 1 {
			t.Errorf("Server client connection %d received %d messages (too few)", connID, count)
		}
		t.Logf("Server client connection %d: %d DATA_DOWN messages", connID, count)
	}

	// Check that server client connections were registered
	serverClientMutex.RLock()
	totalServerClients := len(serverClientConnections)
	serverClientMutex.RUnlock()

	if totalServerClients != 3 {
		t.Errorf("Expected 3 server client connections, got %d", totalServerClients)
	}

	t.Logf("✓ Server-side downstream load balancing test completed")
	t.Logf("  Messages distributed across %d server client connections", len(messageTracker))
	t.Logf("  Total server client connections registered: %d", totalServerClients)
}

// Helper function to count total messages
func getTotalMessages(tracker map[uint64]int) int {
	total := 0
	for _, count := range tracker {
		total += count
	}
	return total
}

// TestServerClientConnectionManagement tests the lifecycle of server client connections
func TestServerClientConnectionManagement(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Reset server-side client connections
	serverClientMutex.Lock()
	serverClientConnections = []*ServerClientConnection{}
	nextServerClientID = 0
	serverClientMutex.Unlock()

	// Create app in server mode
	app = &App{
		PoolConnections: []*PoolConnection{}, // Empty = server mode
		PoolMutex:       sync.Mutex{},
	}

	// Test adding connections
	client1, server1 := net.Pipe()
	defer client1.Close()

	// Simulate handleConnectionWithPoolConn registering a connection
	go func() {
		handleConnectionWithPoolConn(server1, nil)
	}()

	// Give some time for connection registration
	time.Sleep(10 * time.Millisecond)

	// Check that connection was registered
	serverClientMutex.RLock()
	connectionCount := len(serverClientConnections)
	serverClientMutex.RUnlock()

	if connectionCount != 1 {
		t.Errorf("Expected 1 server client connection, got %d", connectionCount)
	}

	// Close the connection
	server1.Close()

	// Give some time for connection cleanup
	time.Sleep(10 * time.Millisecond)

	// Check that connection was removed
	serverClientMutex.RLock()
	finalCount := len(serverClientConnections)
	serverClientMutex.RUnlock()

	if finalCount != 0 {
		t.Errorf("Expected 0 server client connections after close, got %d", finalCount)
	}

	t.Logf("✓ Server client connection management test passed")
}
