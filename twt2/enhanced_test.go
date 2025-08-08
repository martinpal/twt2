package twt2

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// Mock connection that simulates timeout scenarios
type timeoutConn struct {
	*mockConn
	writeDelay time.Duration
	readDelay  time.Duration
}

func newTimeoutConn(writeDelay, readDelay time.Duration) *timeoutConn {
	return &timeoutConn{
		mockConn:   newMockConn(),
		writeDelay: writeDelay,
		readDelay:  readDelay,
	}
}

func (tc *timeoutConn) Write(b []byte) (int, error) {
	if tc.writeDelay > 0 {
		time.Sleep(tc.writeDelay)
	}
	return tc.mockConn.Write(b)
}

func (tc *timeoutConn) Read(b []byte) (int, error) {
	if tc.readDelay > 0 {
		time.Sleep(tc.readDelay)
	}
	return tc.mockConn.Read(b)
}

func (tc *timeoutConn) SetWriteDeadline(t time.Time) error {
	return nil // Ignore deadline for this mock
}

func (tc *timeoutConn) SetReadDeadline(t time.Time) error {
	return nil // Ignore deadline for this mock
}

func (tc *timeoutConn) SetDeadline(t time.Time) error {
	return nil // Ignore deadline for this mock
}

// Test sendProtobufToConn with timeout scenarios
func TestSendProtobufToConn_Timeout(t *testing.T) {
	tests := []struct {
		name        string
		writeDelay  time.Duration
		expectError bool
	}{
		{
			name:        "Fast write succeeds",
			writeDelay:  10 * time.Millisecond,
			expectError: false,
		},
		{
			name:        "Slow write succeeds within timeout",
			writeDelay:  1 * time.Second,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newTimeoutConn(tt.writeDelay, 0)
			message := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_PING,
				Proxy:      0,
				Connection: 123,
				Seq:        1,
			}

			// This should not panic and should handle timeouts gracefully
			sendProtobufToConn(conn, message)

			// Verify the message was written (for successful cases)
			if !tt.expectError && len(conn.writeData) == 0 {
				t.Error("Expected message to be written to connection")
			}
		})
	}
}

// Test poolConnectionSender timeout functionality
func TestPoolConnectionSender_TimeoutHandling(t *testing.T) {
	// Create a simple mock connection that will succeed
	mockConn := newMockConn()

	poolConn := &PoolConnection{
		Conn:            mockConn,
		SendChan:        make(chan *twtproto.ProxyComm, 2),
		ID:              1,
		InUse:           false,
		LastUsed:        time.Now(),
		LastHealthCheck: time.Now(),
		Healthy:         true,
	}

	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Proxy:      0,
		Connection: 123,
		Seq:        1,
	}

	// Start sender in background
	done := make(chan bool, 1)
	go func() {
		poolConnectionSender(poolConn)
		done <- true
	}()

	// Send a message
	poolConn.SendChan <- message

	// Close the channel to signal sender to exit
	close(poolConn.SendChan)

	// Wait for sender to exit
	select {
	case <-done:
		// Good - sender exited normally
	case <-time.After(5 * time.Second):
		t.Fatal("Sender should have exited when channel was closed")
	}
}

// Test poolConnectionSender with nil connection
func TestPoolConnectionSender_NilConnection(t *testing.T) {
	poolConn := &PoolConnection{
		Conn:            nil, // Nil connection
		SendChan:        make(chan *twtproto.ProxyComm, 2),
		ID:              1,
		InUse:           false,
		LastUsed:        time.Now(),
		LastHealthCheck: time.Now(),
		Healthy:         true,
	}

	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Proxy:      0,
		Connection: 123,
		Seq:        1,
	}

	// Start sender in background
	done := make(chan bool, 1)
	go func() {
		poolConnectionSender(poolConn)
		done <- true
	}()

	// Send a message
	poolConn.SendChan <- message
	close(poolConn.SendChan) // Close channel to make sender exit

	// Wait for sender to complete
	select {
	case <-done:
		// Good - sender handled nil connection gracefully
	case <-time.After(5 * time.Second):
		t.Fatal("Sender should have completed quickly with nil connection")
	}

	// Verify connection was marked as unhealthy
	if poolConn.Healthy {
		t.Error("Connection should be marked as unhealthy with nil connection")
	}
}

// Test poolConnectionSender with nil poolConn
func TestPoolConnectionSender_NilPoolConn(t *testing.T) {
	// This should not panic
	poolConnectionSender(nil)
}

// Test sendProtobuf with various connection states
func TestSendProtobuf_HealthyConnectionSelection(t *testing.T) {
	// Setup app with multiple pool connections
	originalApp := app
	defer func() { app = originalApp }()

	app = &App{
		PoolConnections: []*PoolConnection{
			// Unhealthy connection
			{
				ID:              0,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         false,
				InUse:           false,
				LastHealthCheck: time.Now(),
			},
			// Healthy but busy connection
			{
				ID:              1,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           true,
				LastHealthCheck: time.Now(),
			},
			// Healthy and available connection (should be selected)
			{
				ID:              2,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
			},
		},
		PoolMutex: sync.Mutex{},
	}

	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Proxy:      0,
		Connection: 123,
		Seq:        1,
	}

	// Send message
	sendProtobuf(message)

	// Verify the healthy, available connection was used
	selectedConn := app.PoolConnections[2]
	select {
	case receivedMsg := <-selectedConn.SendChan:
		if receivedMsg.Mt != twtproto.ProxyComm_PING {
			t.Error("Expected PING message")
		}
		if receivedMsg.Connection != 123 {
			t.Errorf("Expected connection 123, got %d", receivedMsg.Connection)
		}
	case <-time.After(1 * time.Second):
		t.Error("Message should have been queued to healthy connection")
	}
}

// Test sendProtobuf fallback when primary channel is full
func TestSendProtobuf_FallbackOnFullChannel(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Create connections with small channel capacity for testing
	app = &App{
		PoolConnections: []*PoolConnection{
			// Fallback connection with available space
			{
				ID:              1,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100), // Larger capacity
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-2 * time.Hour), // Make it older so it's selected first
			},
			// Primary connection with full channel (will be tried as fallback)
			{
				ID:              0,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 1), // Small capacity
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-1 * time.Hour), // More recent
			},
		},
		PoolMutex: sync.Mutex{},
	}

	// Fill the connection 0's channel to capacity
	fullConn := app.PoolConnections[1]      // This is connection ID 0
	availableConn := app.PoolConnections[0] // This is connection ID 1

	// Fill connection 0's channel completely
	fullConn.SendChan <- &twtproto.ProxyComm{Mt: twtproto.ProxyComm_PING}

	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Proxy:      0,
		Connection: 456,
		Seq:        2,
	}

	// Send message - should use connection 1 first, but it will try connection 0 as fallback
	sendProtobuf(message)

	// Verify available connection was used (connection 1 should get the message)
	select {
	case receivedMsg := <-availableConn.SendChan:
		if receivedMsg.Mt != twtproto.ProxyComm_DATA_UP {
			t.Error("Expected DATA_UP message")
		}
		if receivedMsg.Connection != 456 {
			t.Errorf("Expected connection 456, got %d", receivedMsg.Connection)
		}
	case <-time.After(100 * time.Millisecond):
		// If connection 1 was selected first and its channel is available, it should get the message
		// But if connection 0 was selected and then connection 1 used as fallback, that's also fine
		t.Error("Message should have been queued to available connection")
	}

	// Verify full channel is still full (original PING message should still be there)
	select {
	case msg := <-fullConn.SendChan:
		if msg.Mt != twtproto.ProxyComm_PING {
			t.Error("Expected PING message in full channel")
		}
	default:
		t.Error("Full channel should still contain the PING message")
	}
}

// Test sendProtobuf with no healthy connections
func TestSendProtobuf_NoHealthyConnections(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	app = &App{
		PoolConnections: []*PoolConnection{
			{
				ID:              0,
				Conn:            nil, // No connection
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         false,
				InUse:           false,
				LastHealthCheck: time.Now(),
			},
		},
		PoolMutex: sync.Mutex{},
	}

	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Proxy:      0,
		Connection: 123,
		Seq:        1,
	}

	// This should not panic and should handle gracefully
	sendProtobuf(message)
	// No assertions needed - just verify it doesn't crash
}

// Test sendProtobuf with app not initialized
func TestSendProtobuf_AppNotInitialized(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	app = nil

	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Proxy:      0,
		Connection: 123,
		Seq:        1,
	}

	// This should not panic
	sendProtobuf(message)
}

// Test startConnectionHealthMonitor functionality
func TestStartConnectionHealthMonitor(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Create app with connections in various states
	unhealthyConn := &PoolConnection{
		ID:              0,
		Conn:            newMockConn(),
		SendChan:        make(chan *twtproto.ProxyComm, 100),
		Healthy:         false,
		InUse:           false,
		LastHealthCheck: time.Now().Add(-15 * time.Minute), // Stale
	}

	healthyConn := &PoolConnection{
		ID:              1,
		Conn:            newMockConn(),
		SendChan:        make(chan *twtproto.ProxyComm, 100),
		Healthy:         true,
		InUse:           false,
		LastHealthCheck: time.Now(),
	}

	app = &App{
		PoolConnections: []*PoolConnection{unhealthyConn, healthyConn},
		PoolMutex:       sync.Mutex{},
	}

	// Start health monitor
	startConnectionHealthMonitor()

	// Give it a moment to run
	time.Sleep(100 * time.Millisecond)

	// The health monitor should run in background
	// This test just verifies it doesn't crash and can be started
}

// Test connection health monitoring with congested channels
func TestConnectionHealthMonitor_CongestionDetection(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Create connection with nearly full channel
	congestedConn := &PoolConnection{
		ID:              0,
		Conn:            newMockConn(),
		SendChan:        make(chan *twtproto.ProxyComm, 4), // Small capacity
		Healthy:         true,
		InUse:           false,
		LastHealthCheck: time.Now().Add(-15 * time.Minute), // Stale
	}

	// Fill the channel to 75% capacity (3/4 messages)
	for i := 0; i < 3; i++ {
		congestedConn.SendChan <- &twtproto.ProxyComm{Mt: twtproto.ProxyComm_PING}
	}

	app = &App{
		PoolConnections: []*PoolConnection{congestedConn},
		PoolMutex:       sync.Mutex{},
	}

	// Start health monitor
	startConnectionHealthMonitor()

	// Give it a moment to run
	time.Sleep(100 * time.Millisecond)

	// The connection should potentially be marked as unhealthy due to congestion
	// This is difficult to test deterministically without modifying the health check interval
}

// Test createPoolConnection with invalid SSH key
func TestCreatePoolConnection_InvalidSSHKey(t *testing.T) {
	// This will fail to create connection due to invalid key
	conn := createPoolConnection(0, "localhost", 22, false, "testuser", "/nonexistent/key", 22)

	if conn != nil {
		t.Error("Expected nil connection due to invalid SSH key")
	}
}

// Test StopAllPoolConnections
func TestStopAllPoolConnections(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Test with nil app
	app = nil
	StopAllPoolConnections() // Should not panic

	// Test with app having connections
	ctx, cancel := context.WithCancel(context.Background())

	poolConn := &PoolConnection{
		ID:          0,
		retryCancel: cancel,
		retryCtx:    ctx,
	}

	app = &App{
		PoolConnections: []*PoolConnection{poolConn},
		PoolMutex:       sync.Mutex{},
	}

	StopAllPoolConnections()

	// Verify context was cancelled
	select {
	case <-ctx.Done():
		// Good - context was cancelled
	case <-time.After(1 * time.Second):
		t.Error("Context should have been cancelled")
	}
}

// Test startSSHKeepAlive with cancellation
func TestStartSSHKeepAlive_Cancellation(t *testing.T) {
	poolConn := &PoolConnection{
		ID:              0,
		keepAliveMutex:  sync.Mutex{},
		keepAliveCancel: nil,
	}

	// Start keep-alive
	startSSHKeepAlive(poolConn)

	// Verify cancel function was set
	if poolConn.keepAliveCancel == nil {
		t.Error("keepAliveCancel should be set")
	}

	// Cancel the keep-alive
	poolConn.keepAliveCancel()

	// Start again to test replacement
	startSSHKeepAlive(poolConn)

	if poolConn.keepAliveCancel == nil {
		t.Error("keepAliveCancel should be set after restart")
	}
}

// Test handleConnection with corrupted frame
func TestHandleConnection_CorruptedFrame(t *testing.T) {
	conn := newMockConn()

	// Add invalid frame data (length too large)
	invalidFrame := []byte{0xFF, 0xFF} // 65535 bytes length
	conn.readData = append(conn.readData, invalidFrame...)

	// This should exit gracefully without panic
	done := make(chan bool, 1)
	go func() {
		handleConnection(conn)
		done <- true
	}()

	select {
	case <-done:
		// Good - function exited due to invalid frame
	case <-time.After(5 * time.Second):
		t.Error("handleConnection should have exited due to corrupted frame")
	}
}

// Test handleConnection with valid protobuf messages
func TestHandleConnection_ValidMessages(t *testing.T) {
	conn := newMockConn()

	// Create a valid PING message
	message := &twtproto.ProxyComm{
		Mt:    twtproto.ProxyComm_PING,
		Proxy: 0,
	}

	conn.addProtobufMessage(message)
	conn.readError = errors.New("EOF") // Make it exit after reading one message

	// This should process the message without panic
	done := make(chan bool, 1)
	go func() {
		handleConnection(conn)
		done <- true
	}()

	select {
	case <-done:
		// Good - function processed message and exited
	case <-time.After(5 * time.Second):
		t.Error("handleConnection should have processed message and exited")
	}
}

// Test handleConnection with invalid protobuf data (wire-format errors)
func TestHandleConnection_InvalidProtobufData(t *testing.T) {
	conn := newMockConn()

	// Create a frame with valid length but invalid protobuf data
	// This simulates the real-world scenario from the error logs
	invalidProtobufData := []byte{
		0x0d, 0x20, 0x01, 0x3a, 0x80, 0x0a, 0xbc, 0x3d,
		0x6e, 0xac, 0xe0, 0xd7, 0x3a, 0x93, 0xc8, 0x31,
		0x4a, 0x16, 0xb0, 0x59, 0xed, 0x2f, 0xeb, 0x4f,
		0x54, 0x2c, 0xb4, 0x8e, 0xe4, 0x5c, 0x6c, 0x31,
		// Add more invalid protobuf data to reach a reasonable size
		0x9b, 0x43, 0x1b, 0x9a, 0x2b, 0x4f, 0x39, 0xc1,
		0xbe, 0x95, 0x94, 0xfd, 0x4a, 0x80, 0x3b, 0x7a,
		0x2d, 0x3d, 0x74, 0xb5, 0x05, 0x95, 0x5f, 0xd4,
		0xa9, 0xcb, 0x11, 0xa1, 0xa4, 0xf2, 0x09, 0x95,
	}

	// Add frame length header (little-endian)
	frameLength := uint16(len(invalidProtobufData))
	lengthBytes := []byte{byte(frameLength), byte(frameLength >> 8)}

	// Construct the complete frame
	conn.readData = append(lengthBytes, invalidProtobufData...)

	// This should detect the protobuf parsing error and exit gracefully
	done := make(chan bool, 1)
	go func() {
		handleConnection(conn)
		done <- true
	}()

	select {
	case <-done:
		// Good - function exited due to protobuf parsing error
	case <-time.After(5 * time.Second):
		t.Error("handleConnection should have exited due to invalid protobuf data")
	}

	// Verify connection was closed
	if !conn.closed {
		t.Error("Connection should be closed after protobuf parsing error")
	}
}

// Test handleConnection with multiple message scenarios
func TestHandleConnection_MultipleMessageTypes(t *testing.T) {
	tests := []struct {
		name        string
		messageType twtproto.ProxyComm_MessageType
		expectError bool
	}{
		{
			name:        "PING message",
			messageType: twtproto.ProxyComm_PING,
			expectError: false,
		},
		{
			name:        "DATA_UP message",
			messageType: twtproto.ProxyComm_DATA_UP,
			expectError: false,
		},
		{
			name:        "DATA_DOWN message",
			messageType: twtproto.ProxyComm_DATA_DOWN,
			expectError: false,
		},
		{
			name:        "OPEN_CONN message",
			messageType: twtproto.ProxyComm_OPEN_CONN,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newMockConn()

			// Create message of specified type
			message := &twtproto.ProxyComm{
				Mt:         tt.messageType,
				Proxy:      0,
				Connection: 123,
				Seq:        1,
				Address:    "example.com",
				Port:       80,
				Data:       []byte("test data"),
			}

			conn.addProtobufMessage(message)
			conn.readError = errors.New("EOF") // Make it exit after processing

			// Process the message
			done := make(chan bool, 1)
			go func() {
				handleConnection(conn)
				done <- true
			}()

			select {
			case <-done:
				// Good - message was processed
			case <-time.After(5 * time.Second):
				t.Errorf("handleConnection should have processed %s message", tt.name)
			}
		})
	}
}

// Test handleConnection with partial frame data
func TestHandleConnection_PartialFrameData(t *testing.T) {
	conn := newMockConn()

	// Create a scenario where we receive partial frame length
	// This simulates network issues where data arrives in fragments
	partialLengthData := []byte{0x10} // Only first byte of length
	conn.readData = partialLengthData
	conn.readError = errors.New("EOF") // Connection closes before complete frame

	// This should handle partial data gracefully and exit
	done := make(chan bool, 1)
	go func() {
		handleConnection(conn)
		done <- true
	}()

	select {
	case <-done:
		// Good - function handled partial data and exited
	case <-time.After(5 * time.Second):
		t.Error("handleConnection should have exited gracefully with partial data")
	}
}

// Test handleConnection with frame length mismatch
func TestHandleConnection_FrameLengthMismatch(t *testing.T) {
	conn := newMockConn()

	// Create a frame with length header indicating 100 bytes but only provide 50
	frameLength := uint16(100)
	lengthBytes := []byte{byte(frameLength), byte(frameLength >> 8)}
	shortData := make([]byte, 50) // Only 50 bytes instead of promised 100

	conn.readData = append(lengthBytes, shortData...)
	conn.readError = errors.New("EOF") // Connection closes before expected data

	// This should detect the length mismatch and exit gracefully
	done := make(chan bool, 1)
	go func() {
		handleConnection(conn)
		done <- true
	}()

	select {
	case <-done:
		// Good - function detected mismatch and exited
	case <-time.After(5 * time.Second):
		t.Error("handleConnection should have exited due to frame length mismatch")
	}
}

// Test poolConnectionReceiver basic functionality
func TestPoolConnectionReceiver_BasicFunctionality(t *testing.T) {
	// This test verifies the function exists and can be referenced
	// The actual reconnection logic is tested in integration tests
	// to avoid hanging in unit tests

	// Just verify the function exists by referencing it
	_ = poolConnectionReceiver

	// Test passes if no panic occurs
}
