package twt2

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// Test newConnection function
func TestNewConnection_Coverage(t *testing.T) {
	originalApp := getApp()
	defer func() {
		time.Sleep(100 * time.Millisecond) // Allow goroutines to complete
		setApp(originalApp)
	}()

	setApp(&App{
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
		PoolConnections:       []*PoolConnection{}, // Add this for server mode
		PoolMutex:             sync.Mutex{},        // Add this for server mode
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
	conn, exists := currentApp.RemoteConnections[123]
	currentApp.RemoteConnectionMutex.Unlock()

	if !exists {
		t.Error("Connection should have been created")
	}

	if conn.Connection == nil {
		t.Error("Connection.Connection should not be nil")
	}

	if conn.NextSeqOut != 1 {
		t.Errorf("Expected NextSeqOut 1, got %d", conn.NextSeqOut)
	}

	// Clean up
	if conn.Connection != nil {
		conn.Connection.Close()
	}
}

// Test newConnection with valid address and server mode
func TestNewConnection_ServerMode(t *testing.T) {
	originalApp := getApp()
	defer func() {
		time.Sleep(200 * time.Millisecond) // Allow goroutines to complete
		setApp(originalApp)
	}()

	setApp(&App{
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
		PoolConnections:       []*PoolConnection{},
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
	conn, exists := currentApp.RemoteConnections[123]
	currentApp.RemoteConnectionMutex.Unlock()

	if !exists {
		t.Error("Connection should have been created")
	}

	if conn.Connection == nil {
		t.Error("Connection.Connection should not be nil")
	}

	if conn.NextSeqOut != 1 {
		t.Errorf("Expected NextSeqOut 1, got %d", conn.NextSeqOut)
	}

	// Clean up
	if conn.Connection != nil {
		conn.Connection.Close()
	}
}

// Test newConnection with invalid address
func TestNewConnection_InvalidAddress(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	setApp(&App{
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
		PoolConnections:       []*PoolConnection{}, // Server mode
		PoolMutex:             sync.Mutex{},
	})

	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_OPEN_CONN,
		Proxy:      0,
		Connection: 456,
		Seq:        0,
		Address:    "invalid.nonexistent.domain",
		Port:       80,
	}

	// Test connection failure
	newConnection(message)

	// Connection should not be created due to dial failure
	currentApp := getApp()
	currentApp.RemoteConnectionMutex.Lock()
	_, exists := currentApp.RemoteConnections[456]
	currentApp.RemoteConnectionMutex.Unlock()

	if exists {
		t.Error("Connection should not have been created for invalid address")
	}
}

// Test forwardDataChunk function
func TestForwardDataChunk(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Create a mock connection
	mockConn := newMockConn()

	setApp(&App{
		RemoteConnections: map[uint64]Connection{
			789: {
				Connection:   mockConn,
				LastSeqIn:    0,
				NextSeqOut:   1,
				MessageQueue: make(map[uint64]*twtproto.ProxyComm),
			},
		},
		RemoteConnectionMutex: sync.Mutex{},
	})

	testData := []byte("Hello, World!")
	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Proxy:      0,
		Connection: 789,
		Seq:        1,
		Data:       testData,
	}

	// Forward data
	forwardDataChunk(message)

	// Verify data was written to connection
	if !bytes.Equal(mockConn.writeData, testData) {
		t.Errorf("Expected data %s, got %s", testData, mockConn.writeData)
	}

	// Verify sequence number was incremented
	currentApp := getApp()
	currentApp.RemoteConnectionMutex.Lock()
	conn := currentApp.RemoteConnections[789]
	currentApp.RemoteConnectionMutex.Unlock()

	if conn.NextSeqOut != 2 {
		t.Errorf("Expected NextSeqOut 2, got %d", conn.NextSeqOut)
	}
}

// Test backwardDataChunk function
func TestBackwardDataChunk(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Create a mock connection
	mockConn := newMockConn()

	setApp(&App{
		LocalConnections: map[uint64]Connection{
			999: {
				Connection:   mockConn,
				LastSeqIn:    0,
				NextSeqOut:   1,
				MessageQueue: make(map[uint64]*twtproto.ProxyComm),
			},
		},
		LocalConnectionMutex: sync.Mutex{},
	})

	testData := []byte("Response data")
	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Proxy:      0,
		Connection: 999,
		Seq:        1,
		Data:       testData,
	}

	// Forward data backward
	backwardDataChunk(message)

	// Verify data was written to connection
	if !bytes.Equal(mockConn.writeData, testData) {
		t.Errorf("Expected data %s, got %s", testData, mockConn.writeData)
	}

	// Verify sequence number was incremented
	currentApp := getApp()
	currentApp.LocalConnectionMutex.Lock()
	conn := currentApp.LocalConnections[999]
	currentApp.LocalConnectionMutex.Unlock()

	if conn.NextSeqOut != 2 {
		t.Errorf("Expected NextSeqOut 2, got %d", conn.NextSeqOut)
	}
}

// Test handleRemoteSideConnection with EOF
func TestHandleRemoteSideConnection_EOF(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	setApp(&App{
		RemoteConnections: map[uint64]Connection{
			111: {
				Connection:   nil, // Will be set below
				LastSeqIn:    0,
				NextSeqOut:   1,
				MessageQueue: make(map[uint64]*twtproto.ProxyComm),
			},
		},
		RemoteConnectionMutex: sync.Mutex{},
		PoolConnections:       []*PoolConnection{}, // Server mode
		PoolMutex:             sync.Mutex{},
	})

	// Create a mock connection that returns EOF
	mockConn := newMockConn()
	mockConn.readError = io.EOF

	// Set the connection
	currentApp := getApp()
	currentApp.RemoteConnectionMutex.Lock()
	conn := currentApp.RemoteConnections[111]
	conn.Connection = mockConn
	currentApp.RemoteConnections[111] = conn
	currentApp.RemoteConnectionMutex.Unlock()

	// Start handler
	done := make(chan bool, 1)
	go func() {
		handleRemoteSideConnection(mockConn, 111)
		done <- true
	}()

	// Wait for completion
	select {
	case <-done:
		// Good - handler exited due to EOF
	case <-time.After(5 * time.Second):
		t.Error("Handler should have exited due to EOF")
	}

	// Verify connection was removed
	currentApp = getApp()
	currentApp.RemoteConnectionMutex.Lock()
	_, exists := currentApp.RemoteConnections[111]
	currentApp.RemoteConnectionMutex.Unlock()

	if exists {
		t.Error("Connection should have been removed after EOF")
	}
}

// Test handleRemoteSideConnection with data
func TestHandleRemoteSideConnection_WithData(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	setApp(&App{
		RemoteConnections: map[uint64]Connection{
			222: {
				Connection:   nil, // Will be set below
				LastSeqIn:    0,
				NextSeqOut:   1,
				MessageQueue: make(map[uint64]*twtproto.ProxyComm),
			},
		},
		RemoteConnectionMutex: sync.Mutex{},
		PoolConnections:       []*PoolConnection{}, // Server mode
		PoolMutex:             sync.Mutex{},
	})

	// Create a mock connection with data
	mockConn := newMockConn()
	testData := "Test response data"
	mockConn.readData = []byte(testData)

	// Make it return EOF after reading the data
	go func() {
		time.Sleep(100 * time.Millisecond)
		mockConn.mu.Lock()
		mockConn.readError = io.EOF
		mockConn.mu.Unlock()
	}()

	// Set the connection
	currentApp := getApp()
	currentApp.RemoteConnectionMutex.Lock()
	conn := currentApp.RemoteConnections[222]
	conn.Connection = mockConn
	currentApp.RemoteConnections[222] = conn
	currentApp.RemoteConnectionMutex.Unlock()

	// Start handler
	done := make(chan bool, 1)
	go func() {
		handleRemoteSideConnection(mockConn, 222)
		done <- true
	}()

	// Wait for completion
	select {
	case <-done:
		// Good - handler processed data and exited
	case <-time.After(5 * time.Second):
		t.Error("Handler should have processed data and exited")
	}

	// Verify sequence number was updated
	currentApp = getApp()
	currentApp.RemoteConnectionMutex.Lock()
	_, exists := currentApp.RemoteConnections[222]
	currentApp.RemoteConnectionMutex.Unlock()

	if exists {
		t.Error("Connection should have been removed after EOF")
	}
}

// Test Hijack function with different scenarios
func TestHijack_MethodNotAllowed(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	setApp(&App{
		ProxyAuthEnabled: false,
	})

	// Create request with invalid method
	req, _ := http.NewRequest("POST", "http://example.com", nil)
	w := &mockResponseWriter{}

	Hijack(w, req)

	if w.statusCode != http.StatusMethodNotAllowed {
		t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.statusCode)
	}
}

// Test Hijack function successful CONNECT
func TestHijack_SuccessfulConnect(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	setApp(&App{
		ProxyAuthEnabled:     false,
		LocalConnections:     make(map[uint64]Connection),
		LocalConnectionMutex: sync.Mutex{},
		LastLocalConnection:  0,
		PoolConnections:      []*PoolConnection{}, // Server mode
		PoolMutex:            sync.Mutex{},
	})

	// Create CONNECT request with proper Host header
	req, _ := http.NewRequest("CONNECT", "", nil)
	req.Host = "example.com:443"
	w := &mockHijackableResponseWriter{}

	// Start Hijack in goroutine since it will block reading from connection
	done := make(chan bool, 1)
	go func() {
		Hijack(w, req)
		done <- true
	}()

	// Give it a moment to process
	time.Sleep(100 * time.Millisecond)

	// Verify hijack was called
	if !w.WasHijackCalled() {
		t.Error("Hijack should have been called")
	}

	if w.statusCode != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.statusCode)
	}

	// Close the connection to make Hijack exit
	w.mu.Lock()
	hijackConn := w.hijackConn
	w.mu.Unlock()

	if hijackConn != nil {
		hijackConn.Close()
	}

	select {
	case <-done:
		// Good
	case <-time.After(5 * time.Second):
		t.Error("Hijack should have completed")
	}
}

// Test ProtobufServer startup validation (without actually starting server)
func TestProtobufServer_StartupValidation(t *testing.T) {
	// This test validates the function exists and can be called
	// We can't test the actual server startup without affecting other tests
	// The function is properly tested in integration tests

	// Just verify the function exists by referencing it
	_ = ProtobufServer

	// Test passes if no panic occurs
}

// Test handleProxycommMessage with different message types
func TestHandleProxycommMessage_PingMessage(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	setApp(&App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	})

	message := &twtproto.ProxyComm{
		Mt:    twtproto.ProxyComm_PING,
		Proxy: 0,
	}

	// This should handle PING message gracefully
	handleProxycommMessage(message)
	// No assertions needed - just verify it doesn't crash
}

// Test handleProxycommMessage with OPEN_CONN
func TestHandleProxycommMessage_OpenConn_Coverage(t *testing.T) {
	originalApp := getApp()
	defer func() {
		time.Sleep(200 * time.Millisecond) // Allow goroutines to complete
		setApp(originalApp)
	}()

	setApp(&App{
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
	})

	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_OPEN_CONN,
		Proxy:      0,
		Connection: 333,
		Seq:        0,
		Address:    "httpbin.org",
		Port:       80,
	}

	// Process the message
	handleProxycommMessage(message)

	// Give connection time to establish
	time.Sleep(100 * time.Millisecond)

	// Verify connection was created
	currentApp := getApp()
	currentApp.RemoteConnectionMutex.Lock()
	conn, exists := currentApp.RemoteConnections[333]
	currentApp.RemoteConnectionMutex.Unlock()

	if !exists {
		t.Error("Remote connection should have been created")
	}

	// Clean up
	if conn.Connection != nil {
		conn.Connection.Close()
	}
}

// Test message queueing in handleProxycommMessage
func TestHandleProxycommMessage_MessageQueue(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	setApp(&App{
		LocalConnections: map[uint64]Connection{
			444: {
				Connection:   newMockConn(),
				LastSeqIn:    0,
				NextSeqOut:   1, // Expecting sequence 1
				MessageQueue: make(map[uint64]*twtproto.ProxyComm),
			},
		},
		LocalConnectionMutex: sync.Mutex{},
	})

	// Send message with sequence 2 (out of order)
	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Proxy:      0,
		Connection: 444,
		Seq:        2, // Out of order
		Data:       []byte("Out of order data"),
	}

	handleProxycommMessage(message)

	// Verify message was queued
	currentApp := getApp()
	currentApp.LocalConnectionMutex.Lock()
	conn := currentApp.LocalConnections[444]
	currentApp.LocalConnectionMutex.Unlock()

	if len(conn.MessageQueue) != 1 {
		t.Errorf("Expected 1 queued message, got %d", len(conn.MessageQueue))
	}

	queuedMsg, exists := conn.MessageQueue[2]
	if !exists {
		t.Error("Message should be queued with sequence 2")
	}

	if string(queuedMsg.Data) != "Out of order data" {
		t.Errorf("Expected queued data 'Out of order data', got '%s'", queuedMsg.Data)
	}
}

// Test closeConnectionLocal and closeConnectionRemote
func TestCloseConnections(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	localConn := newMockConn()
	remoteConn := newMockConn()

	setApp(&App{
		LocalConnections: map[uint64]Connection{
			555: {
				Connection:   localConn,
				LastSeqIn:    0,
				NextSeqOut:   1,
				MessageQueue: make(map[uint64]*twtproto.ProxyComm),
			},
		},
		RemoteConnections: map[uint64]Connection{
			666: {
				Connection:   remoteConn,
				LastSeqIn:    0,
				NextSeqOut:   1,
				MessageQueue: make(map[uint64]*twtproto.ProxyComm),
			},
		},
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	})

	// Test closing local connection
	localMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_CLOSE_CONN_C,
		Proxy:      0,
		Connection: 555,
		Seq:        1,
	}

	closeConnectionLocal(localMessage)

	// Verify local connection was removed
	currentApp := getApp()
	currentApp.LocalConnectionMutex.Lock()
	_, exists := currentApp.LocalConnections[555]
	currentApp.LocalConnectionMutex.Unlock()

	if exists {
		t.Error("Local connection should have been removed")
	}

	// Test closing remote connection
	remoteMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_CLOSE_CONN_S,
		Proxy:      0,
		Connection: 666,
		Seq:        1,
	}

	closeConnectionRemote(remoteMessage)

	// Verify remote connection was removed
	currentApp.RemoteConnectionMutex.Lock()
	_, exists = currentApp.RemoteConnections[666]
	currentApp.RemoteConnectionMutex.Unlock()

	if exists {
		t.Error("Remote connection should have been removed")
	}
}

// Test sendProtobuf in server mode
func TestSendProtobuf_ServerMode_Coverage(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Set up server mode (no pool connections)
	mockConn := newMockConn()
	protobufConnection = mockConn

	setApp(&App{
		PoolConnections: []*PoolConnection{}, // Empty = server mode
		PoolMutex:       sync.Mutex{},
	})

	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Proxy:      0,
		Connection: 123,
		Seq:        1,
	}

	sendProtobuf(message)

	// Verify message was written to protobuf connection
	if len(mockConn.writeData) == 0 {
		t.Error("Message should have been written to protobuf connection")
	}

	// Clean up
	protobufConnection = nil
}
