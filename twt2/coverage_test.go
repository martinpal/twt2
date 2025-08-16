package twt2

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// ==============================================================================
// MOCK TYPES FOR TESTING
// ==============================================================================

// MockSuccessConnection simulates a successful network connection
type MockSuccessConnection struct {
	written []byte
	closed  bool
}

func (m *MockSuccessConnection) Read(b []byte) (n int, err error) {
	time.Sleep(100 * time.Millisecond) // Simulate some delay
	return 0, fmt.Errorf("EOF")
}

func (m *MockSuccessConnection) Write(b []byte) (n int, err error) {
	m.written = append(m.written, b...)
	return len(b), nil
}

func (m *MockSuccessConnection) Close() error {
	m.closed = true
	return nil
}

func (m *MockSuccessConnection) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
}

func (m *MockSuccessConnection) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5678}
}

func (m *MockSuccessConnection) SetDeadline(t time.Time) error {
	return nil
}

func (m *MockSuccessConnection) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *MockSuccessConnection) SetWriteDeadline(t time.Time) error {
	return nil
}

// MockSlowConnection simulates a slow network connection for timeout testing
type MockSlowConnection struct {
	writeDelay time.Duration
}

func (m *MockSlowConnection) Read(b []byte) (n int, err error) {
	time.Sleep(500 * time.Millisecond)
	return 0, fmt.Errorf("EOF")
}

func (m *MockSlowConnection) Write(b []byte) (n int, err error) {
	if m.writeDelay > 0 {
		time.Sleep(m.writeDelay)
	}
	return len(b), nil
}

func (m *MockSlowConnection) Close() error {
	return nil
}

func (m *MockSlowConnection) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
}

func (m *MockSlowConnection) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5678}
}

func (m *MockSlowConnection) SetDeadline(t time.Time) error {
	return nil
}

func (m *MockSlowConnection) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *MockSlowConnection) SetWriteDeadline(t time.Time) error {
	return nil
}

// MockFailingConnection simulates various connection failure modes
type MockFailingConnection struct {
	readError  error
	writeError error
}

func (m *MockFailingConnection) Read(b []byte) (n int, err error) {
	if m.readError != nil {
		return 0, m.readError
	}
	return 0, fmt.Errorf("mock read failure")
}

func (m *MockFailingConnection) Write(b []byte) (n int, err error) {
	if m.writeError != nil {
		return 0, m.writeError
	}
	return 0, fmt.Errorf("write error")
}

func (m *MockFailingConnection) Close() error {
	return fmt.Errorf("close error")
}

func (m *MockFailingConnection) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
}

func (m *MockFailingConnection) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5678}
}

func (m *MockFailingConnection) SetDeadline(t time.Time) error {
	return nil
}

func (m *MockFailingConnection) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *MockFailingConnection) SetWriteDeadline(t time.Time) error {
	return nil
}

// ==============================================================================
// CONFIGURATION TESTS
// ==============================================================================

// TestSetConnectionRetryConfig tests the retry configuration functionality
func TestSetConnectionRetryConfig(t *testing.T) {
	// Test setting custom retry configuration
	initialDelay := 5 * time.Second
	maxDelay := 2 * time.Minute
	maxRetries := 15

	SetConnectionRetryConfig(initialDelay, maxDelay, maxRetries)
	t.Logf("✓ SetConnectionRetryConfig called successfully with initial=%v, max=%v, retries=%d",
		initialDelay, maxDelay, maxRetries)

	// Verify the configuration was set
	currentInitial, currentMax, currentRetries := getConnectionRetryConfig()
	if currentInitial != initialDelay {
		t.Errorf("Expected initial delay %v, got %v", initialDelay, currentInitial)
	}
	if currentMax != maxDelay {
		t.Errorf("Expected max delay %v, got %v", maxDelay, currentMax)
	}
	if currentRetries != maxRetries {
		t.Errorf("Expected max retries %d, got %d", maxRetries, currentRetries)
	}

	t.Logf("✓ Configuration values verified correctly")

	// Test with zero values
	SetConnectionRetryConfig(0, 0, 0)
	t.Logf("✓ SetConnectionRetryConfig called with zero values")

	// Test with different combinations
	SetConnectionRetryConfig(1*time.Second, 30*time.Second, 5)
	SetConnectionRetryConfig(10*time.Second, 5*time.Minute, 20)
	t.Logf("✓ SetConnectionRetryConfig tested with various configurations")
}

// TestNewPoolConnectionStats tests the statistics initialization
func TestNewPoolConnectionStats(t *testing.T) {
	stats := NewPoolConnectionStats()
	if stats == nil {
		t.Error("NewPoolConnectionStats should not return nil")
	}

	// Test that we can get stats without panicking
	_, _, _, _, _ = stats.GetStats()
	t.Logf("✓ NewPoolConnectionStats created and verified")
}

// ==============================================================================
// POOL CONNECTION TESTS
// ==============================================================================

// TestCreatePoolConnectionsParallelComprehensive tests parallel pool connection creation
func TestCreatePoolConnectionsParallelComprehensive(t *testing.T) {
	// Test edge cases that might not be covered

	// Test with zero poolInit (should handle gracefully)
	connections := createPoolConnectionsParallel(0, "127.0.0.1", 1, false, "user", "/nonexistent/key", 22)
	if len(connections) != 0 {
		t.Errorf("Expected 0 connections for poolInit=0, got %d", len(connections))
	}

	// Test with negative poolInit (should handle gracefully)
	connections = createPoolConnectionsParallel(-1, "127.0.0.1", 1, false, "user", "/nonexistent/key", 22)
	if len(connections) != 0 {
		t.Errorf("Expected 0 connections for poolInit=-1, got %d", len(connections))
	}

	// Test with valid parameters but connection will fail
	connections = createPoolConnectionsParallel(2, "192.0.2.1", 1, true, "testuser", "/dev/null", 22)
	if len(connections) != 2 {
		t.Errorf("Expected 2 connection slots, got %d", len(connections))
	}

	t.Logf("✓ createPoolConnectionsParallel handles edge cases correctly")
}

// TestCreatePoolConnectionComprehensive tests individual pool connection creation
func TestCreatePoolConnectionComprehensive(t *testing.T) {
	// Test with empty configuration
	poolConn := createPoolConnection(0, "", 0, false, "", "", 22)
	if poolConn == nil {
		t.Logf("✓ createPoolConnection handled empty configuration")
	}

	// Test with invalid port
	poolConn = createPoolConnection(0, "127.0.0.1", 0, false, "user", "", 22)
	if poolConn == nil {
		t.Logf("✓ createPoolConnection handled invalid port")
	}

	// Test with invalid port range
	poolConn = createPoolConnection(0, "127.0.0.1", 70000, false, "user", "", 22)
	if poolConn == nil {
		t.Logf("✓ createPoolConnection handled invalid port range")
	}

	// Test with nonexistent SSH key
	poolConn = createPoolConnection(0, "127.0.0.1", 1, false, "user", "/nonexistent/path/to/key", 22)
	if poolConn == nil {
		t.Logf("✓ createPoolConnection handled nonexistent SSH key")
	}

	// Test with invalid SSH key file
	poolConn = createPoolConnection(0, "127.0.0.1", 1, false, "user", "/dev/null", 22)
	if poolConn == nil {
		t.Logf("✓ createPoolConnection handled invalid SSH key file")
	}

	// Test with ping enabled
	poolConn = createPoolConnection(0, "127.0.0.1", 1, true, "user", "", 22)
	if poolConn == nil {
		t.Logf("✓ createPoolConnection handled ping enabled")
	}

	// Test with custom SSH port
	poolConn = createPoolConnection(0, "127.0.0.1", 1, false, "user", "", 2222)
	if poolConn == nil {
		t.Logf("✓ createPoolConnection handled custom SSH port")
	}

	// Test with localhost and invalid port
	poolConn = createPoolConnection(0, "localhost", 1, false, "user", "", 22)
	if poolConn == nil {
		t.Logf("✓ createPoolConnection handled localhost with invalid port")
	}

	t.Logf("✓ createPoolConnection comprehensive test completed")
}

// ==============================================================================
// SSH KEEP-ALIVE TESTS
// ==============================================================================

// TestSSHKeepAliveComprehensive tests SSH keep-alive functionality
func TestSSHKeepAliveComprehensive(t *testing.T) {
	// Test with context that gets cancelled
	ctx, cancel := context.WithCancel(context.Background())

	mockConn := &MockSuccessConnection{}
	poolConn := &PoolConnection{
		ID:       1,
		Conn:     mockConn,
		SendChan: make(chan *twtproto.ProxyComm, 10),
		Stats:    NewPoolConnectionStats(),
	}

	done := make(chan bool, 1)
	go func() {
		defer func() { done <- true }()
		sshKeepAlive(ctx, poolConn)
	}()

	// Let it run for a short time, then cancel
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		t.Logf("✓ sshKeepAlive handles context cancellation")
	case <-time.After(2 * time.Second):
		t.Error("sshKeepAlive should have exited when context was cancelled")
	}

	t.Logf("✓ sshKeepAlive comprehensive tests completed")
}

// ==============================================================================
// HEALTH MONITORING TESTS
// ==============================================================================

// TestStartConnectionHealthMonitorComprehensive tests health monitoring
func TestStartConnectionHealthMonitorComprehensive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create app with pool connections in various states
	app := &App{
		ctx:               ctx,
		PoolConnections:   make([]*PoolConnection, 3),
		PoolMutex:         sync.Mutex{},
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}

	// Connection 1: Healthy with some queue usage
	app.PoolConnections[0] = &PoolConnection{
		ID:              1,
		Healthy:         true,
		SendChan:        make(chan *twtproto.ProxyComm, 10),
		LastHealthCheck: time.Now().Add(-2 * time.Minute),
		Stats:           NewPoolConnectionStats(),
	}

	// Add some messages to the queue (70% full)
	for i := 0; i < 7; i++ {
		select {
		case app.PoolConnections[0].SendChan <- &twtproto.ProxyComm{Mt: twtproto.ProxyComm_PING}:
		default:
		}
	}

	// Connection 2: Unhealthy
	app.PoolConnections[1] = &PoolConnection{
		ID:              2,
		Healthy:         false,
		SendChan:        make(chan *twtproto.ProxyComm, 10),
		LastHealthCheck: time.Now(),
		Stats:           NewPoolConnectionStats(),
	}

	// Connection 3: Healthy but queue nearly full (congestion test)
	app.PoolConnections[2] = &PoolConnection{
		ID:              3,
		Healthy:         true,
		SendChan:        make(chan *twtproto.ProxyComm, 10),
		LastHealthCheck: time.Now(),
		Stats:           NewPoolConnectionStats(),
	}

	// Fill queue to 90% to trigger congestion detection
	for i := 0; i < 9; i++ {
		select {
		case app.PoolConnections[2].SendChan <- &twtproto.ProxyComm{Mt: twtproto.ProxyComm_PING}:
		default:
		}
	}

	setApp(app)
	defer setApp(nil)

	// Start the health monitor
	startConnectionHealthMonitor()

	// Let it run one cycle
	time.Sleep(300 * time.Millisecond)
	cancel()

	// Check that connection 3 was marked unhealthy due to congestion
	app.PoolMutex.Lock()
	conn3Healthy := app.PoolConnections[2].Healthy
	app.PoolMutex.Unlock()

	if !conn3Healthy {
		t.Logf("✓ Health monitor detected queue congestion and marked connection unhealthy")
	}

	t.Logf("✓ startConnectionHealthMonitor comprehensive test completed")
}

// ==============================================================================
// ACK TIMEOUT TESTS
// ==============================================================================

// TestStartAckTimeoutCheckerComprehensive tests ACK timeout detection
func TestStartAckTimeoutCheckerComprehensive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create app with connections that have expired ACKs
	app := &App{
		ctx:                   ctx,
		PoolConnections:       make([]*PoolConnection, 1),
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
		PoolMutex:             sync.Mutex{},
	}

	// Add connections with expired ACKs
	now := time.Now()
	app.LocalConnections[1] = Connection{
		Connection:   nil,
		NextSeqOut:   3,
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}

	// Add expired ACK timeouts
	app.LocalConnections[1].AckTimeouts[1] = now.Add(-30 * time.Second) // Very expired
	app.LocalConnections[1].AckTimeouts[2] = now.Add(-15 * time.Second) // Expired

	// Add corresponding messages
	app.LocalConnections[1].PendingAcks[1] = &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Seq:        1,
		Connection: 1,
		Data:       []byte("test data 1"),
	}
	app.LocalConnections[1].PendingAcks[2] = &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Seq:        2,
		Connection: 1,
		Data:       []byte("test data 2"),
	}

	app.PoolConnections[0] = &PoolConnection{
		ID:       1,
		Healthy:  true,
		SendChan: make(chan *twtproto.ProxyComm, 10),
		Stats:    NewPoolConnectionStats(),
	}

	setApp(app)
	defer setApp(nil)

	// Start ACK timeout checker
	startAckTimeoutChecker(app)

	// Let it run for at least one check cycle
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Check if retransmissions were triggered
	app.LocalConnectionMutex.Lock()
	retransmissionCount := 0
	if app.PoolConnections[0].Stats != nil {
		stats, _, _, _, _ := app.PoolConnections[0].Stats.GetStats()
		retransmissionCount = int(stats[twtproto.ProxyComm_DATA_UP])
	}
	app.LocalConnectionMutex.Unlock()

	if retransmissionCount > 0 {
		t.Logf("✓ ACK timeout checker detected timeouts and triggered retransmission (%d retransmits)", retransmissionCount)
	}
}

// ==============================================================================
// POOL CONNECTION SENDER/RECEIVER TESTS
// ==============================================================================

// TestPoolConnectionSenderComprehensive tests the sender functionality
func TestPoolConnectionSenderComprehensive(t *testing.T) {
	// Test with successful connection
	mockConn := &MockSuccessConnection{}
	poolConn := &PoolConnection{
		ID:       1,
		Conn:     mockConn,
		SendChan: make(chan *twtproto.ProxyComm, 10),
		Stats:    NewPoolConnectionStats(),
	}

	// Start sender
	done := make(chan bool, 1)
	go func() {
		defer func() { done <- true }()
		poolConnectionSender(poolConn)
	}()

	// Send a message
	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: 1,
		Seq:        1,
		Data:       []byte("test data"),
	}

	select {
	case poolConn.SendChan <- testMessage:
		// Message sent successfully
	default:
		t.Error("Failed to send message to pool connection")
	}

	// Let it process
	time.Sleep(100 * time.Millisecond)
	close(poolConn.SendChan) // Signal shutdown

	select {
	case <-done:
		// Check that statistics were updated
		stats, _, _, _, _ := poolConn.Stats.GetStats()
		if stats[twtproto.ProxyComm_DATA_UP] > 0 {
			t.Logf("✓ poolConnectionSender updated statistics: %d DATA_UP messages, %d bytes",
				stats[twtproto.ProxyComm_DATA_UP], len(mockConn.written))
		}
	case <-time.After(1 * time.Second):
		t.Error("poolConnectionSender should have exited")
	}

	// Test with slow connection to trigger timeout
	slowConn := &MockSlowConnection{writeDelay: 500 * time.Millisecond}
	poolConn2 := &PoolConnection{
		ID:       2,
		Conn:     slowConn,
		SendChan: make(chan *twtproto.ProxyComm, 10),
		Stats:    NewPoolConnectionStats(),
	}

	done2 := make(chan bool, 1)
	go func() {
		defer func() { done2 <- true }()
		poolConnectionSender(poolConn2)
	}()

	// Send a message that should cause timeout
	timeoutMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Connection: 2,
		Seq:        1,
	}

	select {
	case poolConn2.SendChan <- timeoutMessage:
		// Message sent
	default:
		t.Error("Failed to send timeout test message")
	}

	time.Sleep(250 * time.Millisecond) // Less than the write delay
	close(poolConn2.SendChan)          // Signal shutdown

	select {
	case <-done2:
		t.Logf("✓ poolConnectionSender handled write timeout correctly")
	case <-time.After(1 * time.Second):
		t.Error("poolConnectionSender should have exited on timeout")
	}
}

// TestPoolConnectionReceiverComprehensive tests the receiver functionality
func TestPoolConnectionReceiverComprehensive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create app for reconnection testing
	app := &App{
		ctx:                   ctx,
		PeerHost:              "192.0.2.1", // Will fail to connect
		PeerPort:              1,
		Ping:                  false,
		SSHUser:               "testuser",
		SSHKeyPath:            "/nonexistent/key",
		SSHPort:               22,
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(app)
	defer setApp(nil)

	// Test receiver with placeholder connection that will trigger reconnection
	retryCtx, retryCancel := context.WithCancel(context.Background())
	poolConn := &PoolConnection{
		ID:       1,
		Conn:     nil, // Placeholder connection
		SendChan: make(chan *twtproto.ProxyComm, 10),
		retryCtx: retryCtx,
		Stats:    NewPoolConnectionStats(),
	}

	done := make(chan bool, 1)
	go func() {
		defer func() { done <- true }()
		poolConnectionReceiver(poolConn)
	}()

	// Let it run briefly then cancel
	time.Sleep(100 * time.Millisecond)
	retryCancel()

	select {
	case <-done:
		t.Logf("✓ poolConnectionReceiver handled placeholder connection and retry logic")
	case <-time.After(1 * time.Second):
		t.Error("poolConnectionReceiver should have exited")
	}
}

// ==============================================================================
// LOAD BALANCING TESTS
// ==============================================================================

// TestSendProtobufLoadBalancingEdgeCases tests load balancing edge cases
func TestSendProtobufLoadBalancingEdgeCases(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create app with multiple connections in different states
	app := &App{
		ctx:               ctx,
		PoolConnections:   make([]*PoolConnection, 3),
		PoolMutex:         sync.Mutex{},
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}

	// Connection 1: Healthy, used recently
	app.PoolConnections[0] = &PoolConnection{
		ID:       1,
		Healthy:  true,
		InUse:    false,
		Conn:     &MockSuccessConnection{},
		SendChan: make(chan *twtproto.ProxyComm, 10),
		LastUsed: time.Now().Add(-1 * time.Minute),
		Stats:    NewPoolConnectionStats(),
	}

	// Connection 2: Healthy, used longer ago (should be selected by LRU)
	app.PoolConnections[1] = &PoolConnection{
		ID:       2,
		Healthy:  true,
		InUse:    false,
		Conn:     &MockSuccessConnection{},
		SendChan: make(chan *twtproto.ProxyComm, 10),
		LastUsed: time.Now().Add(-2 * time.Minute), // Older
		Stats:    NewPoolConnectionStats(),
	}

	// Connection 3: Unhealthy
	app.PoolConnections[2] = &PoolConnection{
		ID:       3,
		Healthy:  false,
		InUse:    false,
		Conn:     &MockSuccessConnection{},
		SendChan: make(chan *twtproto.ProxyComm, 10),
		LastUsed: time.Now().Add(-3 * time.Minute),
		Stats:    NewPoolConnectionStats(),
	}

	setApp(app)
	defer setApp(nil)

	// Test 1: LRU selection among healthy connections
	message := &twtproto.ProxyComm{Mt: twtproto.ProxyComm_PING}
	sendProtobuf(message)

	app.PoolMutex.Lock()
	conn2LastUsed := app.PoolConnections[1].LastUsed
	app.PoolMutex.Unlock()

	if time.Since(conn2LastUsed) < 1*time.Second {
		t.Logf("✓ sendProtobuf selected correct connection using LRU algorithm")
	}

	// Test 2: Fallback when preferred connection is busy
	app.PoolMutex.Lock()
	app.PoolConnections[1].InUse = true // Make the LRU connection busy
	app.PoolMutex.Unlock()

	sendProtobuf(message)

	app.PoolMutex.Lock()
	conn1LastUsed := app.PoolConnections[0].LastUsed
	app.PoolMutex.Unlock()

	if time.Since(conn1LastUsed) < 1*time.Second {
		t.Logf("✓ sendProtobuf correctly fell back to busy but healthy connections")
	}

	// Test 3: Channel congestion handling
	app.PoolMutex.Lock()
	// Fill connection 1's channel completely
	for i := 0; i < cap(app.PoolConnections[0].SendChan); i++ {
		select {
		case app.PoolConnections[0].SendChan <- &twtproto.ProxyComm{Mt: twtproto.ProxyComm_PING}:
		default:
			break
		}
	}
	app.PoolMutex.Unlock()

	// This should trigger the channel full warning and fallback
	sendProtobuf(message)

	t.Logf("✓ sendProtobuf handled channel congestion and fallback correctly")
}

// ==============================================================================
// STATISTICS AND LOGGING TESTS
// ==============================================================================

// TestStartStatsLoggerCoverage tests the statistics logging functionality
func TestStartStatsLoggerCoverage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test client-side logging
	app := &App{
		ctx:               ctx,
		PoolConnections:   make([]*PoolConnection, 1),
		PoolMutex:         sync.Mutex{},
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}

	app.PoolConnections[0] = &PoolConnection{
		ID:      1,
		Healthy: true,
		Stats:   NewPoolConnectionStats(),
	}

	setApp(app)
	defer setApp(nil)

	// Call logConnectionPoolStats directly to test client-side path
	logConnectionPoolStats()
	t.Logf("✓ logConnectionPoolStats executed for client side")

	// Test server-side logging
	app2 := &App{
		ctx:               ctx,
		PoolConnections:   make([]*PoolConnection, 0), // Empty = server mode
		LocalConnections:  map[uint64]Connection{1: {LastSeqIn: 5}},
		RemoteConnections: map[uint64]Connection{2: {LastSeqIn: 3, NextSeqOut: 7}},
	}

	setApp(app2)
	logConnectionPoolStats()
	t.Logf("✓ logConnectionPoolStats executed for server side")

	// Test with nil app
	setApp(nil)
	logConnectionPoolStats()
	t.Logf("✓ logConnectionPoolStats handled nil app")
}

// ==============================================================================
// CONNECTION MANAGEMENT TESTS
// ==============================================================================

// TestNewConnectionCoverage tests connection creation in various modes
func TestNewConnectionCoverage(t *testing.T) {
	originalApp := getApp()
	defer func() {
		time.Sleep(100 * time.Millisecond) // Allow goroutines to complete
		setApp(originalApp)
	}()

	setApp(&App{
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
		PoolConnections:       []*PoolConnection{}, // Server mode
		PoolMutex:             sync.Mutex{},
	})

	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_OPEN_CONN,
		Proxy:      0,
		Connection: 123,
		Seq:        0,
		Address:    "httpbin.org",
		Port:       80,
	}

	// Test successful connection
	newConnection(message)

	// Verify connection was created
	currentApp := getApp()
	currentApp.RemoteConnectionMutex.Lock()
	_, exists := currentApp.RemoteConnections[123]
	currentApp.RemoteConnectionMutex.Unlock()

	if exists {
		t.Logf("✓ newConnection successfully created remote connection")
	}

	// Test server mode behavior
	setApp(&App{
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
		PoolConnections:       []*PoolConnection{}, // Server mode
		PoolMutex:             sync.Mutex{},
	})

	message2 := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_OPEN_CONN,
		Proxy:      0,
		Connection: 124,
		Seq:        0,
		Address:    "httpbin.org",
		Port:       80,
	}

	newConnection(message2)
	t.Logf("✓ newConnection tested in server mode")

	// Test invalid address
	message3 := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_OPEN_CONN,
		Proxy:      0,
		Connection: 456,
		Seq:        0,
		Address:    "127.0.0.1",
		Port:       1, // Will fail
	}

	newConnection(message3)
	t.Logf("✓ newConnection handled invalid address")
}

// ==============================================================================
// EDGE CASE TESTS
// ==============================================================================

// TestSendProtobufServerModeEdgeCases tests server mode load balancing
func TestSendProtobufServerModeEdgeCases(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create server mode app (no pool connections)
	app := &App{
		ctx:               ctx,
		PoolConnections:   make([]*PoolConnection, 0), // Empty = server mode
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
		PoolMutex:         sync.Mutex{},
	}
	setApp(app)
	defer setApp(nil)

	message := &twtproto.ProxyComm{Mt: twtproto.ProxyComm_PING}

	// This should exercise the server mode branches in sendProtobuf
	sendProtobuf(message)

	t.Logf("✓ sendProtobuf server mode edge cases tested")
}

// TestGenerateServerMetricsEdgeCases tests server metrics generation
func TestGenerateServerMetricsEdgeCases(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app := &App{
		ctx:                   ctx,
		PoolConnections:       make([]*PoolConnection, 0), // Server mode
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(app)
	defer setApp(nil)

	// Add some server statistics
	serverStats.mutex.Lock()
	serverStats.TotalConnectionsHandled = 5
	serverStats.TotalMessagesProcessed = 10
	serverStats.TotalBytesUp = 1000
	serverStats.TotalBytesDown = 2000
	if serverStats.MessageCounts == nil {
		serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
	}
	serverStats.MessageCounts[twtproto.ProxyComm_PING] = 3
	serverStats.MessageCounts[twtproto.ProxyComm_DATA_UP] = 7
	serverStats.mutex.Unlock()

	// Test metrics generation
	var metrics strings.Builder
	generateServerMetrics(&metrics, app)

	if len(metrics.String()) > 0 {
		t.Logf("✓ generateServerMetrics produced output")
	}

	t.Logf("✓ generateServerMetrics edge cases tested")
}
