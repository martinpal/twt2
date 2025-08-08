package twt2

import (
	"net"
	"sync"
	"testing"

	"palecci.cz/twtproto"
)

// Test to verify that DATA_DOWN messages are tracked in pool connection statistics
func TestDataDownStatsTracking(t *testing.T) {
	// Save original app
	originalApp := app
	defer func() { app = originalApp }()

	// Initialize server stats properly
	serverStats.mutex.Lock()
	if serverStats.MessageCounts == nil {
		serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
	}
	serverStats.TotalBytesDown = 0
	serverStats.TotalMessagesProcessed = 0
	serverStats.mutex.Unlock()

	// Create test app with pool connections (client side)
	app = &App{
		PoolConnections:       []*PoolConnection{},
		PoolMutex:             sync.Mutex{},
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Create mock connection pair
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Create a pool connection with statistics
	poolConn := &PoolConnection{
		Conn:     client,
		SendChan: make(chan *twtproto.ProxyComm, 10),
		ID:       1,
		Stats:    NewPoolConnectionStats(),
		Healthy:  true,
	}

	// Add pool connection to app
	app.PoolMutex.Lock()
	app.PoolConnections = append(app.PoolConnections, poolConn)
	app.PoolMutex.Unlock()

	// Create DATA_DOWN message
	dataDownMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Proxy:      0,
		Connection: 1,
		Seq:        1,
		Data:       []byte("test response data"),
	}

	// Create a local connection to receive the data
	mockLocalConn := newMockConn()
	app.LocalConnectionMutex.Lock()
	app.LocalConnections[1] = Connection{
		Connection:   mockLocalConn,
		NextSeqOut:   1,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}
	app.LocalConnectionMutex.Unlock()

	// Test: Process DATA_DOWN message through the pool connection
	handleProxycommMessageWithPoolConn(dataDownMessage, poolConn)

	// Verify: Check that the pool connection statistics were updated
	counts, bytesUp, bytesDown, _, _ := poolConn.Stats.GetStats()

	// Check message count
	if counts[twtproto.ProxyComm_DATA_DOWN] != 1 {
		t.Errorf("Expected 1 DATA_DOWN message in pool connection stats, got %d",
			counts[twtproto.ProxyComm_DATA_DOWN])
	}

	// Check bytes down
	expectedBytes := uint64(len(dataDownMessage.Data))
	if bytesDown != expectedBytes {
		t.Errorf("Expected %d bytes down in pool connection stats, got %d",
			expectedBytes, bytesDown)
	}

	// Check bytes up should be 0
	if bytesUp != 0 {
		t.Errorf("Expected 0 bytes up in pool connection stats, got %d", bytesUp)
	}

	t.Logf("✓ Pool connection stats correctly tracked DATA_DOWN: %d messages, %d bytes",
		counts[twtproto.ProxyComm_DATA_DOWN], bytesDown)
}

// Test to verify that server-side statistics still work correctly (no pool connection)
func TestDataDownStatsServerSide(t *testing.T) {
	// Save original app
	originalApp := app
	defer func() { app = originalApp }()

	// Initialize server stats properly
	serverStats.mutex.Lock()
	if serverStats.MessageCounts == nil {
		serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
	}
	serverStats.TotalBytesDown = 0
	serverStats.TotalMessagesProcessed = 0
	serverStats.mutex.Unlock()

	// Create test app without pool connections (server side)
	app = &App{
		PoolConnections:       []*PoolConnection{}, // Empty = server side
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Create DATA_DOWN message
	dataDownMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Proxy:      0,
		Connection: 1,
		Seq:        1,
		Data:       []byte("server response data"),
	}

	// Create a local connection to receive the data
	mockLocalConn := newMockConn()
	app.LocalConnectionMutex.Lock()
	app.LocalConnections[1] = Connection{
		Connection:   mockLocalConn,
		NextSeqOut:   1,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}
	app.LocalConnectionMutex.Unlock()

	// Test: Process DATA_DOWN message without pool connection (server side)
	handleProxycommMessageWithPoolConn(dataDownMessage, nil)

	// Verify: Check that server statistics were updated
	serverStats.mutex.RLock()
	dataDownCount := serverStats.MessageCounts[twtproto.ProxyComm_DATA_DOWN]
	totalBytesDown := serverStats.TotalBytesDown
	serverStats.mutex.RUnlock()

	// Check message count
	if dataDownCount != 1 {
		t.Errorf("Expected 1 DATA_DOWN message in server stats, got %d", dataDownCount)
	}

	// Check bytes down - this should be tracked in backwardDataChunk, not here
	expectedBytes := uint64(len(dataDownMessage.Data))
	if totalBytesDown != expectedBytes {
		t.Errorf("Expected %d bytes down in server stats, got %d", expectedBytes, totalBytesDown)
	}

	t.Logf("✓ Server stats correctly tracked DATA_DOWN: %d messages, %d bytes",
		dataDownCount, totalBytesDown)
}

// Test to compare client vs server DATA_DOWN tracking
func TestDataDownStatsComparison(t *testing.T) {
	// Save original app
	originalApp := app
	defer func() { app = originalApp }()

	// Test 1: Client side with pool connection
	t.Run("ClientSide", func(t *testing.T) {
		// Reset server stats
		serverStats.mutex.Lock()
		if serverStats.MessageCounts == nil {
			serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
		}
		serverStats.TotalBytesDown = 0
		serverStats.TotalMessagesProcessed = 0
		serverStats.mutex.Unlock()

		// Create client-side app
		app = &App{
			PoolConnections:       []*PoolConnection{},
			LocalConnections:      make(map[uint64]Connection),
			RemoteConnections:     make(map[uint64]Connection),
			LocalConnectionMutex:  sync.Mutex{},
			RemoteConnectionMutex: sync.Mutex{},
		}

		// Create pool connection
		poolConn := &PoolConnection{
			ID:    1,
			Stats: NewPoolConnectionStats(),
		}
		app.PoolConnections = append(app.PoolConnections, poolConn)

		// Create and process DATA_DOWN message
		message := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Connection: 1,
			Data:       []byte("client test data"),
		}

		// Create local connection
		mockConn := newMockConn()
		app.LocalConnections[1] = Connection{
			Connection:   mockConn,
			NextSeqOut:   1,
			MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		}

		handleProxycommMessageWithPoolConn(message, poolConn)

		// Check pool connection stats
		counts, _, bytesDown, _, _ := poolConn.Stats.GetStats()
		if counts[twtproto.ProxyComm_DATA_DOWN] != 1 {
			t.Errorf("Client pool connection should have 1 DATA_DOWN message, got %d",
				counts[twtproto.ProxyComm_DATA_DOWN])
		}
		if bytesDown != uint64(len(message.Data)) {
			t.Errorf("Client pool connection should have %d bytes down, got %d",
				len(message.Data), bytesDown)
		}

		t.Logf("✓ Client-side DATA_DOWN tracked in pool connection: %d messages, %d bytes",
			counts[twtproto.ProxyComm_DATA_DOWN], bytesDown)
	})

	// Test 2: Server side without pool connection
	t.Run("ServerSide", func(t *testing.T) {
		// Reset server stats
		serverStats.mutex.Lock()
		if serverStats.MessageCounts == nil {
			serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
		}
		serverStats.TotalBytesDown = 0
		serverStats.TotalMessagesProcessed = 0
		serverStats.mutex.Unlock()

		// Create server-side app (no pool connections)
		app = &App{
			PoolConnections:       []*PoolConnection{}, // Empty = server side
			LocalConnections:      make(map[uint64]Connection),
			RemoteConnections:     make(map[uint64]Connection),
			LocalConnectionMutex:  sync.Mutex{},
			RemoteConnectionMutex: sync.Mutex{},
		}

		// Create and process DATA_DOWN message
		message := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Connection: 1,
			Data:       []byte("server test data"),
		}

		// Create local connection
		mockConn := newMockConn()
		app.LocalConnections[1] = Connection{
			Connection:   mockConn,
			NextSeqOut:   1,
			MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		}

		handleProxycommMessageWithPoolConn(message, nil)

		// Check server stats
		serverStats.mutex.RLock()
		count := serverStats.MessageCounts[twtproto.ProxyComm_DATA_DOWN]
		bytesDown := serverStats.TotalBytesDown
		serverStats.mutex.RUnlock()

		if count != 1 {
			t.Errorf("Server should have 1 DATA_DOWN message, got %d", count)
		}
		if bytesDown != uint64(len(message.Data)) {
			t.Errorf("Server should have %d bytes down, got %d", len(message.Data), bytesDown)
		}

		t.Logf("✓ Server-side DATA_DOWN tracked in server stats: %d messages, %d bytes",
			count, bytesDown)
	})
}
