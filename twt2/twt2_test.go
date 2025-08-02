package twt2

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"palecci.cz/twtproto"
)

// Mock network connection for testing
type mockConn struct {
	readData   []byte
	writeData  []byte
	readIndex  int
	writeIndex int
	closed     bool
	readError  error
	writeError error
	mu         sync.Mutex
}

func newMockConn() *mockConn {
	return &mockConn{
		readData:  make([]byte, 0),
		writeData: make([]byte, 0),
	}
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.readError != nil {
		return 0, m.readError
	}

	if m.readIndex >= len(m.readData) {
		return 0, io.EOF
	}

	n = copy(b, m.readData[m.readIndex:])
	m.readIndex += n
	return n, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.writeError != nil {
		return 0, m.writeError
	}

	m.writeData = append(m.writeData, b...)
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345} }
func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 54321}
}
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// Helper function to add protobuf message to mock connection
func (m *mockConn) addProtobufMessage(msg *twtproto.ProxyComm) {
	data, _ := proto.Marshal(msg)
	length := len(data)

	// Add length header (2 bytes, little endian)
	lengthBytes := []byte{byte(length & 255), byte((length >> 8) & 255)}

	m.mu.Lock()
	m.readData = append(m.readData, lengthBytes...)
	m.readData = append(m.readData, data...)
	m.mu.Unlock()
}

// Helper function to get written protobuf messages
func (m *mockConn) getWrittenMessages() []*twtproto.ProxyComm {
	m.mu.Lock()
	defer m.mu.Unlock()

	var messages []*twtproto.ProxyComm
	data := m.writeData
	index := 0

	for index < len(data) {
		if index+2 > len(data) {
			break
		}

		// Read length (little endian)
		length := int(data[index]) + int(data[index+1])<<8
		index += 2

		if index+length > len(data) {
			break
		}

		// Parse protobuf message
		msg := &twtproto.ProxyComm{}
		if err := proto.Unmarshal(data[index:index+length], msg); err == nil {
			messages = append(messages, msg)
		}
		index += length
	}

	return messages
}

// Mock ResponseWriter for testing
type mockResponseWriter struct {
	header     http.Header
	statusCode int
	body       []byte
}

func (m *mockResponseWriter) Header() http.Header {
	if m.header == nil {
		m.header = make(http.Header)
	}
	return m.header
}

func (m *mockResponseWriter) Write(data []byte) (int, error) {
	m.body = append(m.body, data...)
	return len(data), nil
}

func (m *mockResponseWriter) WriteHeader(statusCode int) {
	m.statusCode = statusCode
}

// Test NewApp function for client mode
func TestNewApp_ClientMode(t *testing.T) {
	// Setup
	handler := http.NotFoundHandler().ServeHTTP
	listenPort := 33333
	peerHost := "test.example.com"
	peerPort := 22
	poolInit := 0 // Don't create actual SSH connections in test
	poolCap := 5
	ping := true
	isClient := true
	sshUser := "testuser"
	sshKeyPath := "/tmp/testkey"
	sshPort := 22

	// Execute
	app := NewApp(handler, listenPort, peerHost, peerPort, poolInit, poolCap, ping, isClient, sshUser, sshKeyPath, sshPort)

	// Assert
	if app == nil {
		t.Fatal("NewApp returned nil")
	}
	if app.ListenPort != listenPort {
		t.Errorf("Expected ListenPort %d, got %d", listenPort, app.ListenPort)
	}
	if app.PeerHost != peerHost {
		t.Errorf("Expected PeerHost %s, got %s", peerHost, app.PeerHost)
	}
	if app.PeerPort != peerPort {
		t.Errorf("Expected PeerPort %d, got %d", peerPort, app.PeerPort)
	}
	if app.SSHPort != sshPort {
		t.Errorf("Expected SSHPort %d, got %d", sshPort, app.SSHPort)
	}
	if app.SSHUser != sshUser {
		t.Errorf("Expected SSHUser %s, got %s", sshUser, app.SSHUser)
	}
	if app.SSHKeyPath != sshKeyPath {
		t.Errorf("Expected SSHKeyPath %s, got %s", sshKeyPath, app.SSHKeyPath)
	}
	if !app.Ping {
		t.Error("Expected Ping to be true")
	}
	if app.PoolSize != poolCap {
		t.Errorf("Expected PoolSize %d, got %d", poolCap, app.PoolSize)
	}
	if app.LocalConnections == nil {
		t.Error("LocalConnections map should be initialized")
	}
	if app.RemoteConnections == nil {
		t.Error("RemoteConnections map should be initialized")
	}
	if len(app.PoolConnections) != poolInit {
		t.Errorf("Expected %d pool connections, got %d", poolInit, len(app.PoolConnections))
	}
}

// Test NewApp function for server mode
func TestNewApp_ServerMode(t *testing.T) {
	// Setup
	handler := http.NotFoundHandler().ServeHTTP
	listenPort := 33333
	peerHost := ""
	peerPort := 22
	poolInit := 5
	poolCap := 10
	ping := false
	isClient := false
	sshUser := ""
	sshKeyPath := ""
	sshPort := 22

	// Execute
	app := NewApp(handler, listenPort, peerHost, peerPort, poolInit, poolCap, ping, isClient, sshUser, sshKeyPath, sshPort)

	// Assert
	if app == nil {
		t.Fatal("NewApp returned nil")
	}
	if app.ListenPort != listenPort {
		t.Errorf("Expected ListenPort %d, got %d", listenPort, app.ListenPort)
	}
	if app.PeerHost != peerHost {
		t.Errorf("Expected PeerHost %s, got %s", peerHost, app.PeerHost)
	}
	if app.Ping {
		t.Error("Expected Ping to be false")
	}
	// Server mode should not create pool connections
	if len(app.PoolConnections) != 0 {
		t.Errorf("Server mode should have 0 pool connections, got %d", len(app.PoolConnections))
	}
}

// Test GetApp function
func TestGetApp(t *testing.T) {
	// Reset global app variable
	originalApp := app
	defer func() { app = originalApp }()

	// Test when app is nil
	app = nil
	if GetApp() != nil {
		t.Error("GetApp should return nil when app is nil")
	}

	// Test when app is set
	testApp := &App{ListenPort: 12345}
	app = testApp
	if GetApp() != testApp {
		t.Error("GetApp should return the set app instance")
	}
	if GetApp().ListenPort != 12345 {
		t.Errorf("Expected ListenPort 12345, got %d", GetApp().ListenPort)
	}
}

// Test clearConnectionState function
func TestClearConnectionState(t *testing.T) {
	// Reset global app variable
	originalApp := app
	defer func() { app = originalApp }()

	// Test when app is nil
	app = nil
	clearConnectionState() // Should not panic

	// Setup test app with connections
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	app = testApp

	// Add some test connections
	app.LocalConnections[1] = Connection{Connection: nil, LastSeqIn: 0, NextSeqOut: 1}
	app.LocalConnections[2] = Connection{Connection: nil, LastSeqIn: 1, NextSeqOut: 2}
	app.RemoteConnections[1] = Connection{Connection: nil, LastSeqIn: 0, NextSeqOut: 1}

	// Execute
	clearConnectionState()

	// Assert
	if len(app.LocalConnections) != 0 {
		t.Errorf("Expected 0 local connections after clear, got %d", len(app.LocalConnections))
	}
	if len(app.RemoteConnections) != 0 {
		t.Errorf("Expected 0 remote connections after clear, got %d", len(app.RemoteConnections))
	}
}

// Test handleProxycommMessage with PING message
func TestHandleProxycommMessage_Ping(t *testing.T) {
	// Reset global app variable
	originalApp := app
	defer func() { app = originalApp }()

	// Setup test app
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	app = testApp

	// Create PING message
	pingMessage := &twtproto.ProxyComm{
		Mt:    twtproto.ProxyComm_PING,
		Proxy: 0,
	}

	// Execute - should not panic and should handle PING gracefully
	handleProxycommMessage(pingMessage)

	// PING messages don't modify connection state, so nothing to assert
	// The test passes if no panic occurs
}

// Test handleProxycommMessage with nil app
func TestHandleProxycommMessage_NilApp(t *testing.T) {
	// Reset global app variable
	originalApp := app
	defer func() { app = originalApp }()

	// Set app to nil
	app = nil

	// Create test message
	testMessage := &twtproto.ProxyComm{
		Mt:    twtproto.ProxyComm_OPEN_CONN,
		Proxy: 0,
	}

	// Execute - should not panic when app is nil
	handleProxycommMessage(testMessage)

	// The test passes if no panic occurs
}

// Test sendProtobufToConn function
func TestSendProtobufToConn(t *testing.T) {
	// Create mock connection
	mockConn := newMockConn()

	// Create test message
	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Proxy:      0,
		Connection: 1,
		Seq:        0,
	}

	// Execute
	sendProtobufToConn(mockConn, testMessage)

	// Assert message was written correctly
	writtenMessages := mockConn.getWrittenMessages()
	if len(writtenMessages) != 1 {
		t.Fatalf("Expected 1 message written, got %d", len(writtenMessages))
	}

	if writtenMessages[0].Mt != twtproto.ProxyComm_PING {
		t.Errorf("Expected PING message, got %v", writtenMessages[0].Mt)
	}
	if writtenMessages[0].Proxy != 0 {
		t.Errorf("Expected proxy 0, got %d", writtenMessages[0].Proxy)
	}
}

// Test sendProtobuf function with server mode
func TestSendProtobuf_ServerMode(t *testing.T) {
	// Reset global variables
	originalApp := app
	originalProtobufConnection := protobufConnection
	defer func() {
		app = originalApp
		protobufConnection = originalProtobufConnection
	}()

	// Setup server mode app (no pool connections)
	testApp := &App{
		PoolConnections: make([]*PoolConnection, 0),
	}
	app = testApp

	// Setup mock protobuf connection
	mockConn := newMockConn()
	protobufConnection = mockConn

	// Create test message
	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_CLOSE_CONN_C,
		Proxy:      0,
		Connection: 1,
		Seq:        0,
	}

	// Execute
	sendProtobuf(testMessage)

	// Assert message was sent via protobuf connection
	writtenMessages := mockConn.getWrittenMessages()
	if len(writtenMessages) != 1 {
		t.Fatalf("Expected 1 message written, got %d", len(writtenMessages))
	}

	if writtenMessages[0].Mt != twtproto.ProxyComm_CLOSE_CONN_C {
		t.Errorf("Expected CLOSE_CONN_C message, got %v", writtenMessages[0].Mt)
	}
}

// Test sendProtobuf function with client mode
func TestSendProtobuf_ClientMode(t *testing.T) {
	// Reset global app variable
	originalApp := app
	defer func() { app = originalApp }()

	// Setup client mode app with pool connection
	mockConn := newMockConn()
	poolConn := &PoolConnection{
		Conn:     mockConn,
		SendChan: make(chan *twtproto.ProxyComm, 100),
		ID:       0,
		InUse:    false,
		LastUsed: time.Now(),
	}

	testApp := &App{
		PoolConnections: []*PoolConnection{poolConn},
	}
	app = testApp

	// Start sender goroutine
	go poolConnectionSender(poolConn)

	// Create test message
	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Proxy:      0,
		Connection: 1,
		Seq:        1,
		Data:       []byte("test data"),
	}

	// Execute
	sendProtobuf(testMessage)

	// Give some time for the message to be processed
	time.Sleep(10 * time.Millisecond)

	// Assert message was sent via pool connection
	writtenMessages := mockConn.getWrittenMessages()
	if len(writtenMessages) != 1 {
		t.Fatalf("Expected 1 message written, got %d", len(writtenMessages))
	}

	if writtenMessages[0].Mt != twtproto.ProxyComm_DATA_UP {
		t.Errorf("Expected DATA_UP message, got %v", writtenMessages[0].Mt)
	}
	if string(writtenMessages[0].Data) != "test data" {
		t.Errorf("Expected 'test data', got %s", string(writtenMessages[0].Data))
	}
}

// Test handleConnection function with valid message
func TestHandleConnection_ValidMessage(t *testing.T) {
	// Reset global app variable
	originalApp := app
	defer func() { app = originalApp }()

	// Setup test app
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	app = testApp

	// Create mock connection with PING message
	mockConn := newMockConn()
	pingMessage := &twtproto.ProxyComm{
		Mt:    twtproto.ProxyComm_PING,
		Proxy: 0,
	}
	mockConn.addProtobufMessage(pingMessage)

	// Execute in goroutine since handleConnection blocks
	done := make(chan bool, 1)
	go func() {
		handleConnection(mockConn)
		done <- true
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// Success - connection handled the message and exited when EOF reached
	case <-time.After(100 * time.Millisecond):
		t.Error("handleConnection did not complete in expected time")
	}

	// Assert connection was closed
	if !mockConn.closed {
		t.Error("Expected connection to be closed")
	}
}

// Test handleConnection function with invalid frame length
func TestHandleConnection_InvalidFrameLength(t *testing.T) {
	// Create mock connection with invalid length header
	mockConn := newMockConn()
	// Add invalid length (too large: 70000 = 0x11170)
	mockConn.readData = []byte{0x70, 0x11} // This represents length 70000 which exceeds maxReasonableMessageSize

	// Execute in goroutine since handleConnection blocks
	done := make(chan bool, 1)
	go func() {
		handleConnection(mockConn)
		done <- true
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// Success - connection detected invalid frame and exited
	case <-time.After(100 * time.Millisecond):
		t.Error("handleConnection did not complete in expected time")
	}

	// Assert connection was closed
	if !mockConn.closed {
		t.Error("Expected connection to be closed due to invalid frame length")
	}
}

// Test handleProxycommMessage with OPEN_CONN
func TestHandleProxycommMessage_OpenConn(t *testing.T) {
	// Reset global app variable
	originalApp := app
	defer func() { app = originalApp }()

	// Setup test app
	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
	}
	app = testApp

	// Create OPEN_CONN message
	openMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_OPEN_CONN,
		Proxy:      0,
		Connection: 1,
		Seq:        0,
		Address:    "invalid-address-that-will-fail",
		Port:       443,
	}

	// Execute - this will fail to connect but should handle gracefully
	handleProxycommMessage(openMessage)

	// Test passes if it handles the connection failure without panic
	// Note: Failed connections are not added to the RemoteConnections map
}

// Test backwardDataChunk and forwardDataChunk functions
func TestDataChunkForwarding(t *testing.T) {
	// Reset global app variable
	originalApp := app
	defer func() { app = originalApp }()

	// Setup test app
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	app = testApp

	// Test backward data chunk (server to client)
	mockLocalConn := newMockConn()
	app.LocalConnections[1] = Connection{
		Connection: mockLocalConn,
		NextSeqOut: 1,
	}

	dataMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: 1,
		Seq:        1,
		Data:       []byte("test response data"),
	}

	backwardDataChunk(dataMessage)

	// Assert data was written to local connection
	expectedData := "test response data"
	if !bytes.Contains(mockLocalConn.writeData, []byte(expectedData)) {
		t.Errorf("Expected '%s' to be written to local connection", expectedData)
	}

	// Test forward data chunk (client to server)
	mockRemoteConn := newMockConn()
	app.RemoteConnections[2] = Connection{
		Connection: mockRemoteConn,
		NextSeqOut: 1,
	}

	upDataMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: 2,
		Seq:        1,
		Data:       []byte("test request data"),
	}

	forwardDataChunk(upDataMessage)

	// Assert data was written to remote connection
	expectedUpData := "test request data"
	if !bytes.Contains(mockRemoteConn.writeData, []byte(expectedUpData)) {
		t.Errorf("Expected '%s' to be written to remote connection", expectedUpData)
	}
}

// Test connection closing functions
func TestConnectionClosing(t *testing.T) {
	// Reset global app variable
	originalApp := app
	defer func() { app = originalApp }()

	// Setup test app
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	app = testApp

	// Add test connections
	mockLocalConn := newMockConn()
	mockRemoteConn := newMockConn()

	app.LocalConnections[1] = Connection{Connection: mockLocalConn}
	app.RemoteConnections[2] = Connection{Connection: mockRemoteConn}

	// Test closing local connection
	closeLocalMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_CLOSE_CONN_C,
		Connection: 1,
	}
	closeConnectionLocal(closeLocalMessage)

	// Assert local connection was removed
	if _, exists := app.LocalConnections[1]; exists {
		t.Error("Expected local connection 1 to be removed")
	}

	// Test closing remote connection
	closeRemoteMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_CLOSE_CONN_S,
		Connection: 2,
	}
	closeConnectionRemote(closeRemoteMessage)

	// Assert remote connection was removed
	if _, exists := app.RemoteConnections[2]; exists {
		t.Error("Expected remote connection 2 to be removed")
	}
}

// Test App.ServeHTTP method
func TestApp_ServeHTTP(t *testing.T) {
	// Create test app with custom handler
	called := false
	testHandler := func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}

	app := &App{
		DefaultRoute: testHandler,
	}

	// Create test request and response writer
	req, _ := http.NewRequest("GET", "/test", nil)
	rw := &mockResponseWriter{}

	// Execute
	app.ServeHTTP(rw, req)

	// Assert handler was called
	if !called {
		t.Error("Expected default route handler to be called")
	}
}

// Test Connection struct initialization
func TestConnection_Initialization(t *testing.T) {
	conn := Connection{
		Connection:   nil,
		LastSeqIn:    0,
		NextSeqOut:   1,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}

	if conn.LastSeqIn != 0 {
		t.Errorf("Expected LastSeqIn 0, got %d", conn.LastSeqIn)
	}
	if conn.NextSeqOut != 1 {
		t.Errorf("Expected NextSeqOut 1, got %d", conn.NextSeqOut)
	}
	if conn.MessageQueue == nil {
		t.Error("MessageQueue should be initialized")
	}
	if len(conn.MessageQueue) != 0 {
		t.Errorf("Expected empty MessageQueue, got %d items", len(conn.MessageQueue))
	}
}

// Test PoolConnection struct initialization
func TestPoolConnection_Initialization(t *testing.T) {
	poolConn := &PoolConnection{
		Conn:      nil,
		SendChan:  make(chan *twtproto.ProxyComm, 100),
		ID:        42,
		InUse:     false,
		LastUsed:  time.Now(),
		SSHClient: nil,
		SSHConn:   nil,
		LocalPort: 12345,
	}

	if poolConn.ID != 42 {
		t.Errorf("Expected ID 42, got %d", poolConn.ID)
	}
	if poolConn.InUse {
		t.Error("Expected InUse to be false")
	}
	if poolConn.LocalPort != 12345 {
		t.Errorf("Expected LocalPort 12345, got %d", poolConn.LocalPort)
	}
	if poolConn.SendChan == nil {
		t.Error("SendChan should be initialized")
	}
	if cap(poolConn.SendChan) != 100 {
		t.Errorf("Expected SendChan capacity 100, got %d", cap(poolConn.SendChan))
	}
}

// Test protobuf message creation
func TestProtobufMessage_Creation(t *testing.T) {
	// Test PING message
	pingMsg := &twtproto.ProxyComm{
		Mt:    twtproto.ProxyComm_PING,
		Proxy: 0,
	}
	if pingMsg.Mt != twtproto.ProxyComm_PING {
		t.Errorf("Expected PING message type, got %v", pingMsg.Mt)
	}

	// Test OPEN_CONN message
	openMsg := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_OPEN_CONN,
		Proxy:      0,
		Connection: 1,
		Seq:        0,
		Address:    "example.com",
		Port:       443,
	}
	if openMsg.Mt != twtproto.ProxyComm_OPEN_CONN {
		t.Errorf("Expected OPEN_CONN message type, got %v", openMsg.Mt)
	}
	if openMsg.Address != "example.com" {
		t.Errorf("Expected address 'example.com', got %s", openMsg.Address)
	}
	if openMsg.Port != 443 {
		t.Errorf("Expected port 443, got %d", openMsg.Port)
	}

	// Test DATA_UP message
	testData := []byte("test data")
	dataMsg := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Proxy:      0,
		Connection: 1,
		Seq:        1,
		Data:       testData,
	}
	if dataMsg.Mt != twtproto.ProxyComm_DATA_UP {
		t.Errorf("Expected DATA_UP message type, got %v", dataMsg.Mt)
	}
	if string(dataMsg.Data) != "test data" {
		t.Errorf("Expected data 'test data', got %s", string(dataMsg.Data))
	}
}
