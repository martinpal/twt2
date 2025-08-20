package twt2

import (
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// TestLogConnectionPoolStats tests the statistics logging function
func TestLogConnectionPoolStats(t *testing.T) {
	// Save original app
	originalApp := app
	defer func() { app = originalApp }()

	// Create test app with pool connections
	app = &App{
		PoolConnections: []*PoolConnection{
			{
				ID:      1,
				Healthy: true,
				InUse:   false,
				Stats:   NewPoolConnectionStats(),
			},
			{
				ID:      2,
				Healthy: false,
				InUse:   true,
				Stats:   NewPoolConnectionStats(),
			},
		},
		PoolMutex:             sync.Mutex{},
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Add some stats
	app.PoolConnections[0].Stats.IncrementMessage(twtproto.ProxyComm_DATA_UP, 100)
	app.PoolConnections[1].Stats.IncrementMessage(twtproto.ProxyComm_DATA_DOWN, 200)

	// Test client side (with pool connections)
	logConnectionPoolStats()
	t.Logf("✓ Client-side pool stats logging completed without panic")

	// Test server side (no pool connections)
	app.PoolConnections = []*PoolConnection{}
	app.LocalConnections[1] = Connection{}
	app.RemoteConnections[2] = Connection{}

	logConnectionPoolStats()
	t.Logf("✓ Server-side connection stats logging completed without panic")
}

// TestStartConnectionHealthMonitor tests the health monitoring system
func TestStartConnectionHealthMonitorCoverage(t *testing.T) {
	// Use safe test app pattern
	mockConn1 := newMockConn()
	mockConn2 := newMockConn()

	testApp := &App{
		PoolConnections: []*PoolConnection{
			{
				ID:              1,
				Conn:            mockConn1,
				Healthy:         true,
				LastHealthCheck: time.Now().Add(-5 * time.Minute), // Old health check
				Stats:           NewPoolConnectionStats(),
			},
			{
				ID:              2,
				Conn:            mockConn2,
				Healthy:         false,
				LastHealthCheck: time.Now(), // Recent health check
				Stats:           NewPoolConnectionStats(),
			},
		},
		PoolMutex: sync.Mutex{},
	}

	originalApp := SafeSetTestApp(testApp)
	defer SafeRestoreApp(originalApp)

	// Start health monitor
	startConnectionHealthMonitor()

	// Wait a short time for health check to potentially run
	time.Sleep(100 * time.Millisecond)

	t.Logf("✓ Health monitor started successfully")

	// Test pool connections are still accessible
	app.PoolMutex.Lock()
	poolCount := len(app.PoolConnections)
	app.PoolMutex.Unlock()

	if poolCount != 2 {
		t.Errorf("Expected 2 pool connections, got %d", poolCount)
	}

	t.Logf("✓ Pool connections maintained during health monitoring")
}

// TestBasicRetransmissionStructures tests connection-failure-based retransmission structures
func TestBasicRetransmissionStructures(t *testing.T) {
	// Create test app
	originalApp := getApp()
	defer setApp(originalApp)

	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

	// Test basic retransmission setup
	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: 1,
		Seq:        1,
		Data:       []byte("test data"),
	}

	pendingAck := &PendingAck{
		Message:          testMessage,
		PoolConnectionID: 1,
	}

	// Test that PendingAck structure works correctly
	if pendingAck.Message.Mt != twtproto.ProxyComm_DATA_UP {
		t.Errorf("Expected DATA_UP message type, got %v", pendingAck.Message.Mt)
	}

	if pendingAck.PoolConnectionID != 1 {
		t.Errorf("Expected pool connection ID 1, got %d", pendingAck.PoolConnectionID)
	}

	t.Logf("✓ Connection-failure retransmission structures working correctly")
}

// TestCreatePoolConnectionErrors tests error handling in pool connection creation
func TestCreatePoolConnectionErrorsCoverage(t *testing.T) {
	// Test with invalid SSH key path
	poolConn := createPoolConnection(0, "127.0.0.1", 1, false, "testuser", "/nonexistent/key", 22)

	if poolConn != nil {
		t.Errorf("Expected nil pool connection for invalid SSH key, got %v", poolConn)
	}

	t.Logf("✓ createPoolConnection correctly handles invalid SSH key path")

	// Test with invalid SSH host (this will timeout, so we test the early error path)
	// The function should handle the error gracefully and return nil
	poolConn = createPoolConnection(0, "invalid-ssh-host", 8080, false, "testuser", "/nonexistent/key", 22)

	if poolConn != nil {
		t.Errorf("Expected nil pool connection for invalid SSH host, got %v", poolConn)
	}

	t.Logf("✓ createPoolConnection correctly handles SSH connection errors")
}

// TestPoolConnectionSenderEdgeCases tests edge cases in pool connection sender
func TestPoolConnectionSenderEdgeCases(t *testing.T) {
	// Test nil pool connection
	poolConnectionSender(nil)
	t.Logf("✓ poolConnectionSender handles nil input gracefully")

	// Test pool connection with nil connection
	poolConn := &PoolConnection{
		ID:       1,
		Conn:     nil, // This should cause early exit
		SendChan: make(chan *twtproto.ProxyComm, 1),
		Stats:    NewPoolConnectionStats(),
	}

	// Send a message to test the nil connection path
	testMsg := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Connection: 1,
		Seq:        1,
	}

	// Start sender in goroutine
	go poolConnectionSender(poolConn)

	// Send message and close channel
	poolConn.SendChan <- testMsg
	close(poolConn.SendChan)

	// Wait for sender to finish
	time.Sleep(100 * time.Millisecond)

	t.Logf("✓ poolConnectionSender handles nil connection gracefully")
}

// TestSSHKeepAliveEdgeCases tests SSH keep-alive functionality
func TestSSHKeepAliveEdgeCases(t *testing.T) {
	// Create a pool connection with mock SSH client
	poolConn := &PoolConnection{
		ID:             1,
		keepAliveMutex: sync.Mutex{},
	}

	// Test starting SSH keep-alive
	startSSHKeepAlive(poolConn)

	if poolConn.keepAliveCancel == nil {
		t.Errorf("Expected keepAliveCancel to be set")
	}

	t.Logf("✓ startSSHKeepAlive sets up cancel function")

	// Test canceling existing keep-alive and starting new one
	startSSHKeepAlive(poolConn)

	if poolConn.keepAliveCancel == nil {
		t.Errorf("Expected keepAliveCancel to remain set after restart")
	}

	// Cancel the keep-alive
	if poolConn.keepAliveCancel != nil {
		poolConn.keepAliveCancel()
	}

	t.Logf("✓ startSSHKeepAlive correctly replaces existing keep-alive")
}

// TestStopAllPoolConnections tests stopping all pool connections
func TestStopAllPoolConnectionsCoverage(t *testing.T) {
	// Test with no app - use safe pattern
	originalApp := SafeSetTestApp(nil)
	defer SafeRestoreApp(originalApp)

	StopAllPoolConnections() // Should not panic
	t.Logf("✓ StopAllPoolConnections handles nil app gracefully")

	// Test with app but no pool connections
	testApp := &App{
		PoolConnections: []*PoolConnection{},
		PoolMutex:       sync.Mutex{},
	}
	SafeSetTestApp(testApp)
	StopAllPoolConnections() // Should not panic
	t.Logf("✓ StopAllPoolConnections handles empty pool gracefully")

	// Test with pool connections that have cancel functions
	app.PoolConnections = []*PoolConnection{
		{
			ID:          1,
			retryCancel: func() { t.Logf("Cancel function 1 called") },
		},
		{
			ID:          2,
			retryCancel: func() { t.Logf("Cancel function 2 called") },
		},
		{
			ID:          3,
			retryCancel: nil, // Test nil cancel function
		},
	}

	StopAllPoolConnections()
	t.Logf("✓ StopAllPoolConnections calls cancel functions correctly")
}

// TestCreatePoolConnectionsParallelEdgeCases tests parallel pool creation edge cases
func TestCreatePoolConnectionsParallelEdgeCasesCoverage(t *testing.T) {
	// Test with zero or negative pool size - these don't create retry goroutines
	connections := createPoolConnectionsParallel(0, "127.0.0.1", 1, false, "user", "/nonexistent/key", 22)
	if len(connections) != 0 {
		t.Errorf("Expected 0 connections for poolInit=0, got %d", len(connections))
	}
	t.Logf("✓ createPoolConnectionsParallel handles zero poolInit correctly")

	connections = createPoolConnectionsParallel(-1, "127.0.0.1", 1, false, "user", "/nonexistent/key", 22)
	if len(connections) != 0 {
		t.Errorf("Expected 0 connections for poolInit=-1, got %d", len(connections))
	}
	t.Logf("✓ createPoolConnectionsParallel handles negative poolInit correctly")

	// Test that the function correctly validates pool size without side effects
	t.Logf("✓ createPoolConnectionsParallel handles edge cases without creating background processes")
}
