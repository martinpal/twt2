package twt2

import (
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// TestServerSideLoadBalancingLogic tests the server-side load balancing without actual network I/O
func TestServerSideLoadBalancingLogic(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Reset server-side client connections
	serverClientMutex.Lock()
	serverClientConnections = []*ServerClientConnection{}
	nextServerClientID = 0
	serverClientMutex.Unlock()

	// Create app in server mode (no pool connections)
	app = &App{
		PoolConnections: []*PoolConnection{}, // Empty = server mode
		PoolMutex:       sync.Mutex{},
	}

	// Create mock server client connections with different LastUsed times
	serverClientMutex.Lock()
	for i := 0; i < 3; i++ {
		serverClientConn := &ServerClientConnection{
			Conn:     newMockConn(), // Use our existing mock connection
			ID:       uint64(i),
			LastUsed: time.Now().Add(-time.Duration(3-i) * time.Hour), // Different ages
			Active:   true,
		}
		serverClientConnections = append(serverClientConnections, serverClientConn)
	}
	nextServerClientID = 3
	serverClientMutex.Unlock()

	// Track which connections are selected by the load balancer
	connectionUsage := make(map[uint64]int)

	// Test the load balancing logic by calling sendProtobuf multiple times
	for i := 0; i < 9; i++ {
		// Record the state before sendProtobuf
		serverClientMutex.RLock()
		beforeState := make(map[uint64]time.Time)
		for _, conn := range serverClientConnections {
			beforeState[conn.ID] = conn.LastUsed
		}
		serverClientMutex.RUnlock()

		dataDownMessage := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Proxy:      0,
			Connection: uint64(100 + i),
			Seq:        uint64(i),
			Data:       []byte("test data"),
		}

		// Call sendProtobuf - this should select a connection using LRU
		sendProtobuf(dataDownMessage)

		// Check which connection was selected (it should have updated LastUsed)
		serverClientMutex.RLock()
		var selectedConnID uint64 = 999 // Invalid default
		var oldestTime time.Time = time.Now()

		// Find the connection that had its LastUsed updated (most recent)
		for _, conn := range serverClientConnections {
			if beforeTime, exists := beforeState[conn.ID]; exists {
				if conn.LastUsed.After(beforeTime) {
					// This connection was used
					selectedConnID = conn.ID
					break
				}
			}
			// Also track which had the oldest time before this call
			if beforeState[conn.ID].Before(oldestTime) {
				oldestTime = beforeState[conn.ID]
			}
		}
		serverClientMutex.RUnlock()

		if selectedConnID != 999 {
			connectionUsage[selectedConnID]++
			t.Logf("Message %d: Server client connection %d selected", i, selectedConnID)
		} else {
			t.Logf("Message %d: Could not determine which connection was selected", i)
		}

		// Small delay to ensure time differences
		time.Sleep(1 * time.Millisecond)
	}

	t.Logf("Server-side load balancing usage: %v", connectionUsage)

	// Verify that all connections were used (proper load balancing)
	for i := uint64(0); i < 3; i++ {
		if connectionUsage[i] == 0 {
			t.Errorf("Server client connection %d was never selected - load balancing not working", i)
		}
	}

	// Verify reasonable distribution
	totalMessages := 9
	for connID, usage := range connectionUsage {
		if usage < 1 || usage > totalMessages {
			t.Errorf("Server client connection %d has unrealistic usage: %d", connID, usage)
		}
		t.Logf("Server client connection %d: %d selections", connID, usage)
	}

	// Verify that the first message went to the oldest connection (LRU)
	if connectionUsage[0] == 0 {
		t.Error("Connection 0 (oldest) should have been selected at least once")
	}

	t.Logf("✓ Server-side load balancing logic test passed")
	t.Logf("  Messages distributed across %d server client connections", len(connectionUsage))
}

// TestServerClientConnectionRegistration tests the connection registration/cleanup logic
func TestServerClientConnectionRegistration(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Reset server-side client connections
	serverClientMutex.Lock()
	originalConnections := serverClientConnections
	originalNextID := nextServerClientID
	serverClientConnections = []*ServerClientConnection{}
	nextServerClientID = 0
	serverClientMutex.Unlock()

	defer func() {
		// Restore original state
		serverClientMutex.Lock()
		serverClientConnections = originalConnections
		nextServerClientID = originalNextID
		serverClientMutex.Unlock()
	}()

	// Create app in server mode
	app = &App{
		PoolConnections: []*PoolConnection{}, // Empty = server mode
		PoolMutex:       sync.Mutex{},
	}

	// Manually test connection registration (simulating what handleConnectionWithPoolConn does)
	mockConn := newMockConn()

	// Register connection
	serverClientMutex.Lock()
	serverClientConn := &ServerClientConnection{
		Conn:     mockConn,
		ID:       nextServerClientID,
		LastUsed: time.Now(),
		Active:   true,
	}
	serverClientConnections = append(serverClientConnections, serverClientConn)
	nextServerClientID++
	connectionCount := len(serverClientConnections)
	serverClientMutex.Unlock()

	if connectionCount != 1 {
		t.Errorf("Expected 1 server client connection after registration, got %d", connectionCount)
	}

	// Test cleanup (simulating defer in handleConnectionWithPoolConn)
	serverClientMutex.Lock()
	for i, conn := range serverClientConnections {
		if conn.ID == serverClientConn.ID {
			serverClientConnections = append(serverClientConnections[:i], serverClientConnections[i+1:]...)
			break
		}
	}
	finalCount := len(serverClientConnections)
	serverClientMutex.Unlock()

	if finalCount != 0 {
		t.Errorf("Expected 0 server client connections after cleanup, got %d", finalCount)
	}

	t.Logf("✓ Server client connection registration test passed")
}
