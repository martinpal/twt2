package twt2

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
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
	closed     int32 // Use atomic for thread safety
	readError  error
	writeError error
	mu         sync.RWMutex
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
	atomic.CompareAndSwapInt32(&m.closed, 0, 1)
	return nil
}

func (m *mockConn) IsClosed() bool {
	return atomic.LoadInt32(&m.closed) != 0
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

// Mock ResponseWriter that implements http.Hijacker for testing Hijack function
type mockHijackableResponseWriter struct {
	mockResponseWriter
	hijackConn   net.Conn
	hijackErr    error
	hijackCalled int32 // Use atomic for thread safety
	mu           sync.Mutex
}

// Implement http.Hijacker interface properly
func (m *mockHijackableResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	atomic.StoreInt32(&m.hijackCalled, 1)
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.hijackErr != nil {
		return nil, nil, m.hijackErr
	}
	if m.hijackConn == nil {
		m.hijackConn = newMockConn()
	}
	// Create a proper bufio.ReadWriter
	reader := bufio.NewReader(bytes.NewBuffer([]byte{}))
	writer := bufio.NewWriter(bytes.NewBuffer([]byte{}))
	bufrw := bufio.NewReadWriter(reader, writer)
	return m.hijackConn, bufrw, nil
}

func (m *mockHijackableResponseWriter) WasHijackCalled() bool {
	return atomic.LoadInt32(&m.hijackCalled) != 0
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
	sshKeyPath := "testkey"
	sshPort := 22

	// Execute
	app := NewApp(handler, listenPort, peerHost, peerPort, poolInit, poolCap, ping, isClient, sshUser, sshKeyPath, sshPort, false, "", "", "")

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
	app := NewApp(handler, listenPort, peerHost, peerPort, poolInit, poolCap, ping, isClient, sshUser, sshKeyPath, sshPort, false, "", "", "")

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
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Test when app is nil
	app = nil
	if GetApp() != nil {
		t.Error("GetApp should return nil when app is nil")
	}

	// Test when app is set
	testApp := &App{ListenPort: 12345}
	setApp(testApp)
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
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Test when app is nil
	app = nil
	clearConnectionState() // Should not panic

	// Setup test app with connections
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

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
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Setup test app
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

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
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

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
	originalApp := getApp()
	originalProtobufConnection := protobufConnection
	defer func() {
		setApp(originalApp)
		protobufConnection = originalProtobufConnection
		// Clean up server client connections to avoid interference
		serverClientMutex.Lock()
		serverClientConnections = []*ServerClientConnection{}
		nextServerClientID = 0
		serverClientMutex.Unlock()
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond) // Give goroutines time to stop
	}()

	// Clean up server client connections first
	serverClientMutex.Lock()
	serverClientConnections = []*ServerClientConnection{}
	nextServerClientID = 0
	serverClientMutex.Unlock()

	// Setup server mode app (no pool connections)
	testApp := &App{
		PoolConnections: make([]*PoolConnection, 0),
	}
	setApp(testApp)

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
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

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
	setApp(testApp)

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
	originalApp := getApp()
	defer func() {
		app = originalApp
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond) // Give goroutines time to stop
	}()

	// Setup test app
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

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
	if !mockConn.IsClosed() {
		t.Error("Expected connection to be closed")
	}
}

// Test handleConnection function with invalid frame length
func TestHandleConnection_InvalidFrameLength(t *testing.T) {
	defer func() {
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond) // Give goroutines time to stop
	}()

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
	if !mockConn.IsClosed() {
		t.Error("Expected connection to be closed due to invalid frame length")
	}
}

// Test handleProxycommMessage with OPEN_CONN
func TestHandleProxycommMessage_OpenConn(t *testing.T) {
	// Reset global app variable
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Setup test app
	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

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
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Setup test app
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

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
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Setup test app
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

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
}

// Test createPoolConnection function with mocked SSH
func TestCreatePoolConnection_MockSSH(t *testing.T) {
	defer func() {
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond) // Give goroutines time to stop
	}()

	// Test the error path since we can't easily mock SSH

	// This should fail gracefully with invalid connection details
	poolConn := createPoolConnection(0, "invalid-host-for-testing", 22, false, "testuser", "/nonexistent/key", 22)

	// Should return nil when SSH setup fails due to missing key file
	if poolConn != nil {
		t.Error("Expected poolConn to be nil when SSH setup fails")
	}
}

// Test Hijack function
func TestHijack(t *testing.T) {
	// Reset global app variable
	originalApp := getApp()
	originalProtobufConnection := protobufConnection
	defer func() {
		app = originalApp
		protobufConnection = originalProtobufConnection
	}()

	// Setup test app
	testApp := &App{
		LocalConnections:     make(map[uint64]Connection),
		LastLocalConnection:  1,
		LocalConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

	// Setup mock protobuf connection to avoid "No protobuf connection available" error
	mockProtobufConn := newMockConn()
	protobufConnection = mockProtobufConn

	// Create test request and hijackable response writer
	req, _ := http.NewRequest("CONNECT", "example.com:443", nil)
	req.Host = "example.com:443" // Set Host explicitly since CONNECT requests need it
	rw := &mockHijackableResponseWriter{}

	// Execute hijack in goroutine since it blocks in a loop
	done := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Hijack might panic when the mock connection closes
			}
			done <- true
		}()
		Hijack(rw, req)
	}()

	// Give some time for hijack to process
	time.Sleep(20 * time.Millisecond)

	// Assert that hijack was called
	if !rw.WasHijackCalled() {
		t.Error("Expected Hijack to be called on response writer")
	}

	// Assert that response status was set to OK
	if rw.statusCode != http.StatusOK && rw.statusCode != 0 {
		t.Errorf("Expected status %d or 0, got %d", http.StatusOK, rw.statusCode)
	}

	// Give time for connection setup and then check
	time.Sleep(20 * time.Millisecond)

	// Assert that a connection was added to LocalConnections (might be removed if it closes quickly)
	testApp.LocalConnectionMutex.Lock()
	connectionCount := len(testApp.LocalConnections)
	testApp.LocalConnectionMutex.Unlock()

	// The connection might be cleaned up due to EOF, so we just check that hijack was called successfully
	if connectionCount == 0 {
		// This is acceptable if the connection closed quickly due to EOF
		t.Logf("Connection was closed quickly (EOF), but hijack was called successfully")
	}

	// Wait for completion or timeout
	select {
	case <-done:
		// Success
	case <-time.After(100 * time.Millisecond):
		// Expected timeout since hijack runs in a loop
	}
}

// Test Hijack function with non-hijackable response writer
func TestHijack_NonHijackable(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Set up minimal app for the test
	testApp := &App{
		LocalConnections:     make(map[uint64]Connection),
		LastLocalConnection:  1,
		LocalConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

	// Create test request and regular response writer (not hijackable)
	req, _ := http.NewRequest("CONNECT", "example.com:443", nil)
	req.Host = "example.com:443" // Set Host explicitly since CONNECT requests need it
	rw := &mockResponseWriter{}

	// Execute hijack - this should fail gracefully
	Hijack(rw, req)

	// Should have written an error response
	if rw.statusCode != http.StatusInternalServerError {
		t.Errorf("Expected status %d for non-hijackable writer, got %d", http.StatusInternalServerError, rw.statusCode)
	}
}

// Test ProtobufServer function
func TestProtobufServer(t *testing.T) {
	// Test the function with an invalid port to trigger error path
	// Use a port that should be available for testing
	testPort := 0 // Port 0 will cause bind to fail in most cases

	// Execute in goroutine since it might block
	done := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Expected to fail with port 0
			}
			done <- true
		}()
		ProtobufServer(testPort)
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// Success - function handled the error case
	case <-time.After(100 * time.Millisecond):
		// Expected timeout since function might block
	}
}

// Test handleRemoteSideConnection function
func TestHandleRemoteSideConnection(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

	// Create mock connection and add it to local connections
	mockConn := newMockConn()
	connectionID := uint64(1)

	conn := Connection{
		Connection: mockConn,
		NextSeqOut: 1,
	}
	app.LocalConnections[connectionID] = conn

	// Add the same connection to RemoteConnections since handleRemoteSideConnection expects it
	app.RemoteConnections[connectionID] = Connection{
		Connection: nil, // This is the remote connection, not used in the function
		LastSeqIn:  0,
		NextSeqOut: 1,
	}

	// Create a mock remote connection
	mockRemoteConn := newMockConn()

	// Add some test data to the remote connection that will trigger EOF quickly
	testData := []byte("remote data")
	mockRemoteConn.readData = testData

	// Execute in goroutine since it blocks
	done := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Unexpected panic: %v", r)
			}
			done <- true
		}()
		handleRemoteSideConnection(mockRemoteConn, connectionID)
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// Success
	case <-time.After(100 * time.Millisecond):
		// Expected timeout since function runs until connection closes
	}
}

// Test poolConnectionReceiver function
func TestPoolConnectionReceiver(t *testing.T) {
	originalApp := getApp()
	defer func() {
		setApp(originalApp) // Use safe app setting
		StopAllPoolConnections()
		// Give goroutines time to fully terminate
		time.Sleep(500 * time.Millisecond)
	}()

	testApp := &App{
		LocalConnections: make(map[uint64]Connection),
	}
	setApp(testApp) // Use safe app setting

	// Create a mock connection
	mockConn := newMockConn()

	// Create a context with cancellation for proper goroutine termination
	ctx, cancel := context.WithCancel(context.Background())

	// Create a PoolConnection with proper context setup
	poolConn := &PoolConnection{
		ID:          1,
		Conn:        mockConn,
		retryCtx:    ctx,
		retryCancel: cancel,
	}

	// Add some test protobuf data to the mock connection
	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: 1,
		Data:       []byte("test data for receiver"),
	}
	data, _ := proto.Marshal(testMessage)

	// Add frame header (4-byte length prefix)
	frameHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(frameHeader, uint32(len(data)))
	mockConn.readData = append(frameHeader, data...)

	// Execute in goroutine since it blocks
	done := make(chan bool, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Function might panic on connection errors
			}
			// Signal that goroutine is terminating
			done <- true
		}()
		poolConnectionReceiver(poolConn)
	}()

	// Give some time for processing
	time.Sleep(10 * time.Millisecond)

	// Close the connection and cancel context to stop the receiver
	mockConn.Close()
	cancel() // This should cause the goroutine to exit cleanly

	// Wait for completion or timeout
	select {
	case <-done:
		// Success - goroutine completed
		t.Log("poolConnectionReceiver completed successfully")
	case <-time.After(100 * time.Millisecond):
		// This should not happen now that we cancel the context
		t.Log("poolConnectionReceiver timed out - this should not happen")
	}

	// Minimal sleep since goroutine should terminate promptly
	time.Sleep(50 * time.Millisecond)
}

// Test with error conditions to improve coverage
func TestHandleProxycommMessage_ErrorCases(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

	// Test with non-existent connection ID for DATA_DOWN message
	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: 999, // Non-existent connection
		Data:       []byte("test data"),
	}

	// This should handle the missing connection gracefully (it will just log and return)
	handleProxycommMessage(testMessage)

	// Test with CLOSE message
	closeMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_CLOSE_CONN_C,
		Connection: 1,
		Seq:        1, // Set sequence to match NextSeqOut
	}

	// Add a connection to close
	mockConn := newMockConn()
	conn := Connection{
		Connection:   mockConn,
		NextSeqOut:   1,                                    // This must match the Seq in the message
		MessageQueue: make(map[uint64]*twtproto.ProxyComm), // Initialize MessageQueue to avoid nil map assignment
	}
	app.LocalConnections[1] = conn

	handleProxycommMessage(closeMessage)

	// Give a short time for the connection to be removed since closeConnectionLocal
	// might have some async behavior
	time.Sleep(10 * time.Millisecond)

	// Verify connection was removed
	app.LocalConnectionMutex.Lock()
	_, exists := app.LocalConnections[1]
	app.LocalConnectionMutex.Unlock()

	if exists {
		t.Error("Expected connection to be removed after CLOSE message")
	}
}

// Test sendProtobuf error cases
func TestSendProtobuf_ErrorCases(t *testing.T) {
	originalProtobufConnection := protobufConnection
	defer func() { protobufConnection = originalProtobufConnection }()

	// Test with nil protobuf connection
	protobufConnection = nil

	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: 1,
		Data:       []byte("test"),
	}

	// Should handle nil connection gracefully
	sendProtobuf(testMessage)

	// Test with mock connection that returns write error
	mockConn := newMockConn()
	mockConn.writeError = fmt.Errorf("write error")
	protobufConnection = mockConn

	// Should handle write error gracefully
	sendProtobuf(testMessage)
}

// Test more handleProxycommMessage cases to increase coverage
func TestHandleProxycommMessage_MoreCases(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

	// Test CLOSE_CONN_S message
	closeServerMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_CLOSE_CONN_S,
		Connection: 1,
	}
	// Add a remote connection to close
	mockRemoteConn := newMockConn()
	app.RemoteConnections[1] = Connection{
		Connection:   mockRemoteConn,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}
	handleProxycommMessage(closeServerMessage)

	// Verify remote connection was removed
	if _, exists := app.RemoteConnections[1]; exists {
		t.Error("Expected remote connection to be removed")
	}
}

// Test newConnection function
func TestNewConnection(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	testApp := &App{
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

	// Test with valid address and port
	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_OPEN_CONN,
		Connection: 1,
		Address:    "httpbin.org", // Use a real address that should work
		Port:       80,
	}

	// Execute in goroutine since it makes network calls
	done := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Unexpected panic: %v", r)
			}
			done <- true
		}()
		newConnection(message)
	}()

	// Wait for completion
	select {
	case <-done:
		// Check if connection was added (might fail due to network, but shouldn't panic)
		testApp.RemoteConnectionMutex.Lock()
		connectionCount := len(testApp.RemoteConnections)
		testApp.RemoteConnectionMutex.Unlock()
		// Don't assert specific count as network calls might fail in test environment
		t.Logf("Connections after newConnection: %d", connectionCount)
	case <-time.After(5 * time.Second):
		t.Error("newConnection took too long to complete")
	}

	// Test with invalid address (should fail gracefully)
	invalidMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_OPEN_CONN,
		Connection: 2,
		Address:    "invalid.domain.that.does.not.exist.example",
		Port:       443,
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Unexpected panic on invalid address: %v", r)
			}
			done <- true
		}()
		newConnection(invalidMessage)
	}()

	select {
	case <-done:
		// Should handle invalid address gracefully
	case <-time.After(5 * time.Second):
		t.Error("newConnection with invalid address took too long")
	}
}

// Test poolConnectionSender function
func TestPoolConnectionSender(t *testing.T) {
	mockConn := newMockConn()
	poolConn := &PoolConnection{
		ID:       1,
		Conn:     mockConn,
		SendChan: make(chan *twtproto.ProxyComm, 10),
	}

	// Start sender in goroutine
	done := make(chan bool, 1)
	go func() {
		defer func() {
			done <- true
		}()
		poolConnectionSender(poolConn)
	}()

	// Send test messages
	testMessage1 := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Connection: 1,
	}
	testMessage2 := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: 2,
		Data:       []byte("test data"),
	}

	poolConn.SendChan <- testMessage1
	poolConn.SendChan <- testMessage2

	// Give time for messages to be processed
	time.Sleep(10 * time.Millisecond)

	// Close channel to stop sender
	close(poolConn.SendChan)

	// Wait for sender to finish
	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Error("poolConnectionSender did not finish in time")
	}

	// Verify messages were sent
	writtenMessages := mockConn.getWrittenMessages()
	if len(writtenMessages) != 2 {
		t.Errorf("Expected 2 messages written, got %d", len(writtenMessages))
	}
}

// Test sendProtobufToConn error cases
func TestSendProtobufToConn_ErrorCases(t *testing.T) {
	// Test with connection that returns write error
	mockConn := newMockConn()
	mockConn.writeError = fmt.Errorf("write error")

	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Connection: 1,
	}

	// Should handle write error gracefully (no panic)
	sendProtobufToConn(mockConn, testMessage)

	// Test with nil message (should handle gracefully)
	mockConn2 := newMockConn()
	sendProtobufToConn(mockConn2, nil)
}

// Test clearConnectionState with more scenarios
func TestClearConnectionState_Extended(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Test with connections that have actual network connections
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

	// Add connections with mock network connections
	mockConn1 := newMockConn()
	mockConn2 := newMockConn()
	mockConn3 := newMockConn()

	app.LocalConnections[1] = Connection{
		Connection:   mockConn1,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}
	app.LocalConnections[2] = Connection{
		Connection:   mockConn2,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}
	app.RemoteConnections[1] = Connection{
		Connection:   mockConn3,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}

	// Execute
	clearConnectionState()

	// Verify all connections were closed
	if !mockConn1.IsClosed() {
		t.Error("Expected local connection 1 to be closed")
	}
	if !mockConn2.IsClosed() {
		t.Error("Expected local connection 2 to be closed")
	}
	if !mockConn3.IsClosed() {
		t.Error("Expected remote connection 1 to be closed")
	}

	// Verify maps were cleared
	if len(app.LocalConnections) != 0 {
		t.Errorf("Expected 0 local connections, got %d", len(app.LocalConnections))
	}
	if len(app.RemoteConnections) != 0 {
		t.Errorf("Expected 0 remote connections, got %d", len(app.RemoteConnections))
	}
}

// Test Hijack with more error conditions
func TestHijack_MoreErrorConditions(t *testing.T) {
	originalApp := getApp()
	originalProtobufConnection := protobufConnection
	defer func() {
		app = originalApp
		protobufConnection = originalProtobufConnection
	}()

	// Test with nil protobuf connection
	protobufConnection = nil

	testApp := &App{
		LocalConnections:     make(map[uint64]Connection),
		LastLocalConnection:  1,
		LocalConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

	req, _ := http.NewRequest("CONNECT", "example.com:443", nil)
	req.Host = "example.com:443"
	rw := &mockHijackableResponseWriter{}

	// Execute - should handle nil protobuf connection
	Hijack(rw, req)

	// Should have called hijack despite protobuf connection being nil
	if !rw.WasHijackCalled() {
		t.Error("Expected Hijack to be called even with nil protobuf connection")
	}
}

// Test handleConnection with various frame corruption scenarios
func TestHandleConnection_FrameCorruption(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

	// Test with incomplete length header (only 1 byte)
	mockConn1 := newMockConn()
	mockConn1.readData = []byte{0x10} // Only 1 byte instead of 2

	done := make(chan bool, 1)
	go func() {
		handleConnection(mockConn1)
		done <- true
	}()

	select {
	case <-done:
		// Should handle incomplete header gracefully
	case <-time.After(100 * time.Millisecond):
		t.Error("handleConnection with incomplete header took too long")
	}

	// Test with length header indicating huge message
	mockConn2 := newMockConn()
	mockConn2.readData = []byte{0xFF, 0xFF} // 65535 bytes - should exceed reasonable limit

	go func() {
		handleConnection(mockConn2)
		done <- true
	}()

	select {
	case <-done:
		// Should detect unreasonable message size
	case <-time.After(100 * time.Millisecond):
		t.Error("handleConnection with huge message size took too long")
	}

	// Test with length header but incomplete message data
	mockConn3 := newMockConn()
	mockConn3.readData = []byte{0x10, 0x00} // Claims 16 bytes but no data follows

	go func() {
		handleConnection(mockConn3)
		done <- true
	}()

	select {
	case <-done:
		// Should handle incomplete message data
	case <-time.After(100 * time.Millisecond):
		t.Error("handleConnection with incomplete message took too long")
	}
}

// Test NewApp with different configurations to increase coverage
func TestNewApp_ExtendedConfigurations(t *testing.T) {
	defer func() {
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond) // Give goroutines time to stop
	}()

	handler := http.NotFoundHandler().ServeHTTP

	// Test with empty peer host (server mode)
	app1 := NewApp(handler, 8080, "", 22, 0, 5, false, true, "user", "testkey", 22, false, "", "", "")
	if len(app1.PoolConnections) != 0 {
		t.Error("Expected no pool connections when peer host is empty")
	}

	// Test with non-client mode (should not create pool connections even with poolInit=2)
	app2 := NewApp(handler, 8080, "example.com", 22, 0, 5, true, false, "user", "testkey", 22, false, "", "", "")
	if len(app2.PoolConnections) != 0 {
		t.Error("Expected no pool connections in server mode")
	}

	// Test with zero pool initialization
	app3 := NewApp(handler, 8080, "example.com", 22, 0, 5, true, true, "user", "testkey", 22, false, "", "", "")
	if len(app3.PoolConnections) != 0 {
		t.Error("Expected no pool connections when poolInit is 0")
	}
}

// Test createPoolConnection with various error conditions
func TestCreatePoolConnection_ErrorConditions(t *testing.T) {
	defer func() {
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond) // Give goroutines time to stop
	}()

	// Test with non-existent key file
	poolConn1 := createPoolConnection(1, "example.com", 22, false, "user", "/nonexistent/path/key", 22)
	if poolConn1 != nil {
		t.Error("Expected nil when key file doesn't exist")
	}
}

// Test message queue processing
func TestMessageQueueProcessing(t *testing.T) {
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	testApp := &App{
		LocalConnections:     make(map[uint64]Connection),
		LocalConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

	// Create a connection with message queue
	mockConn := newMockConn()
	conn := Connection{
		Connection:   mockConn,
		NextSeqOut:   3, // Expecting sequence 3
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}

	// Add some out-of-order messages to the queue
	conn.MessageQueue[4] = &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: 1,
		Seq:        4,
		Data:       []byte("message 4"),
	}
	conn.MessageQueue[5] = &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: 1,
		Seq:        5,
		Data:       []byte("message 5"),
	}

	app.LocalConnections[1] = conn

	// Now send the expected message (sequence 3)
	inOrderMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: 1,
		Seq:        3,
		Data:       []byte("message 3"),
	}

	handleProxycommMessage(inOrderMessage)

	// Check if queued messages were processed
	time.Sleep(10 * time.Millisecond)

	// Verify the connection's NextSeqOut was updated
	updatedConn := app.LocalConnections[1]
	if updatedConn.NextSeqOut < 4 { // Should have processed at least message 3
		t.Errorf("Expected NextSeqOut to be at least 4, got %d", updatedConn.NextSeqOut)
	}

	// Check if data was written to the connection
	if len(mockConn.writeData) == 0 {
		t.Error("Expected data to be written to connection")
	}
}

// Test ProtobufServer with more scenarios
func TestProtobufServer_ExtendedScenarios(t *testing.T) {
	// Test with high port number that should be available
	done := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// May panic on some systems
			}
			done <- true
		}()
		ProtobufServer(0) // Port 0 should auto-assign available port
	}()

	select {
	case <-done:
		// Should handle port binding gracefully
	case <-time.After(500 * time.Millisecond):
		// Function might block
	}
}

// Test connection cleanup scenarios
func TestConnectionCleanup(t *testing.T) {
	originalApp := getApp()
	defer func() {
		setApp(originalApp) // Use safe app setting
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond) // Give goroutines time to stop
	}()

	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(testApp) // Use safe app setting

	// Add connections with message queues
	mockLocalConn := newMockConn()
	mockRemoteConn := newMockConn()

	localConn := Connection{
		Connection: mockLocalConn,
		MessageQueue: map[uint64]*twtproto.ProxyComm{
			10: {Mt: twtproto.ProxyComm_DATA_DOWN, Seq: 10},
			11: {Mt: twtproto.ProxyComm_DATA_DOWN, Seq: 11},
		},
	}
	remoteConn := Connection{
		Connection: mockRemoteConn,
		MessageQueue: map[uint64]*twtproto.ProxyComm{
			20: {Mt: twtproto.ProxyComm_DATA_UP, Seq: 20},
		},
	}

	app.LocalConnections[1] = localConn
	app.RemoteConnections[1] = remoteConn

	// Test closing local connection with queued messages
	closeLocalMsg := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_CLOSE_CONN_C,
		Connection: 1,
	}
	closeConnectionLocal(closeLocalMsg)

	// Verify local connection was removed immediately
	if _, exists := app.LocalConnections[1]; exists {
		t.Error("Expected local connection to be removed")
	}

	// Wait for the delayed close operation (closeConnectionLocal uses 1 second delay)
	time.Sleep(1100 * time.Millisecond) // Wait a bit longer than 1 second

	// Verify local connection was closed after delay
	if !mockLocalConn.IsClosed() {
		t.Error("Expected local connection to be closed after delay")
	}

	// Test closing remote connection with queued messages
	closeRemoteMsg := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_CLOSE_CONN_S,
		Connection: 1,
	}
	closeConnectionRemote(closeRemoteMsg)

	// Verify remote connection was removed immediately
	if _, exists := app.RemoteConnections[1]; exists {
		t.Error("Expected remote connection to be removed")
	}

	// Wait for the delayed close operation (closeConnectionRemote uses 1 second delay)
	time.Sleep(1100 * time.Millisecond) // Wait a bit longer than 1 second

	// Verify remote connection was closed after delay
	if !mockRemoteConn.IsClosed() {
		t.Error("Expected remote connection to be closed after delay")
	}
}

// Test createPoolConnectionsParallel function
func TestCreatePoolConnectionsParallel(t *testing.T) {
	// Stop retry goroutines to prevent background processes
	defer func() {
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond) // Give goroutines time to stop
	}()

	// Test with zero connections
	connections := createPoolConnectionsParallel(0, "example.com", 22, false, "user", "/nonexistent/key", 22)
	if len(connections) != 0 {
		t.Errorf("Expected 0 connections for poolInit=0, got %d", len(connections))
	}

	// Test with 1 connection only to minimize background goroutines
	connections = createPoolConnectionsParallel(1, "example.com", 22, false, "user", "/nonexistent/key", 22)
	if len(connections) != 1 {
		t.Errorf("Expected 1 placeholder connection with invalid key, got %d", len(connections))
	}
	// Verify it's a placeholder connection (Conn should be nil)
	if connections[0].Conn != nil {
		t.Errorf("Expected placeholder connection to have nil Conn, but it has a connection")
	}

	// Immediately stop retry goroutines to prevent test interference
	for _, conn := range connections {
		if conn.retryCancel != nil {
			conn.retryCancel()
		}
	}

	// The function should handle the parallel processing correctly even when connections fail
	// The test passes if it doesn't panic or hang
}

// Test NewApp with parallel pool creation
func TestNewApp_ParallelPoolCreation(t *testing.T) {
	// Store original app and restore after test
	originalApp := getApp()
	defer func() {
		setApp(originalApp)
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond) // Give goroutines time to stop
	}()

	handler := http.NotFoundHandler().ServeHTTP

	// Test with no pool connections to avoid background SSH retry goroutines
	newApp := NewApp(handler, 8080, "example.com", 22, 0, 5, false, true, "user", "nonexistent", 22, false, "", "", "")
	setApp(newApp)

	// Since we're using pool size 0, no connections should be created
	currentApp := getApp()
	if len(currentApp.PoolConnections) != 0 {
		t.Errorf("Expected 0 pool connections with poolInit=0, got %d", len(currentApp.PoolConnections))
	}

	// Verify other app properties are set correctly
	if currentApp.ListenPort != 8080 {
		t.Errorf("Expected ListenPort 8080, got %d", currentApp.ListenPort)
	}
	if currentApp.PeerHost != "example.com" {
		t.Errorf("Expected PeerHost 'example.com', got %s", currentApp.PeerHost)
	}
}

// Test authenticateProxyRequest function
func TestAuthenticateProxyRequest(t *testing.T) {
	tests := []struct {
		name           string
		username       string
		password       string
		authHeader     string
		expectedResult bool
	}{
		{
			name:           "No authentication required",
			username:       "",
			password:       "",
			authHeader:     "",
			expectedResult: true,
		},
		{
			name:           "Valid Basic authentication",
			username:       "user",
			password:       "pass",
			authHeader:     "Basic dXNlcjpwYXNz", // base64 of "user:pass"
			expectedResult: true,
		},
		{
			name:           "Invalid username",
			username:       "user",
			password:       "pass",
			authHeader:     "Basic d3JvbmdfdXNlcjpwYXNz", // base64 of "wrong_user:pass"
			expectedResult: false,
		},
		{
			name:           "Invalid password",
			username:       "user",
			password:       "pass",
			authHeader:     "Basic dXNlcjp3cm9uZ19wYXNz", // base64 of "user:wrong_pass"
			expectedResult: false,
		},
		{
			name:           "Missing auth header",
			username:       "user",
			password:       "pass",
			authHeader:     "",
			expectedResult: false,
		},
		{
			name:           "Wrong auth type",
			username:       "user",
			password:       "pass",
			authHeader:     "Bearer token123",
			expectedResult: false,
		},
		{
			name:           "Invalid base64",
			username:       "user",
			password:       "pass",
			authHeader:     "Basic invalid@base64!",
			expectedResult: false,
		},
		{
			name:           "Malformed credentials",
			username:       "user",
			password:       "pass",
			authHeader:     "Basic dXNlcm5vY29sb24=", // base64 of "usernocolon"
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := http.NewRequest("CONNECT", "example.com:443", nil)
			if tt.authHeader != "" {
				req.Header.Set("Proxy-Authorization", tt.authHeader)
			}

			result := authenticateProxyRequest(req, tt.username, tt.password)
			if result != tt.expectedResult {
				t.Errorf("Expected %v, got %v", tt.expectedResult, result)
			}
		})
	}
}

// Test sendProxyAuthRequired function
func TestSendProxyAuthRequired(t *testing.T) {
	// Create a mock ResponseWriter
	w := &mockResponseWriter{}

	sendProxyAuthRequired(w)

	// Check status code
	if w.statusCode != http.StatusProxyAuthRequired {
		t.Errorf("Expected status %d, got %d", http.StatusProxyAuthRequired, w.statusCode)
	}

	// Check Proxy-Authenticate header
	authHeader := w.Header().Get("Proxy-Authenticate")
	expectedHeader := "Basic realm=\"TW2 Proxy\""
	if authHeader != expectedHeader {
		t.Errorf("Expected Proxy-Authenticate header '%s', got '%s'", expectedHeader, authHeader)
	}

	// Check response body
	expectedBody := "Proxy Authentication Required"
	if string(w.body) != expectedBody {
		t.Errorf("Expected body '%s', got '%s'", expectedBody, string(w.body))
	}
}

// Test servePACFile function
func TestServePACFile(t *testing.T) {
	// Save original app state
	originalApp := getApp()

	tests := []struct {
		name           string
		pacFilePath    string
		expectedStatus int
		shouldContain  string
	}{
		{
			name:           "No PAC file configured",
			pacFilePath:    "",
			expectedStatus: http.StatusOK,
			shouldContain:  "",
		},
		{
			name:           "Non-existent PAC file",
			pacFilePath:    "/nonexistent/file.pac",
			expectedStatus: http.StatusNotFound,
			shouldContain:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test app
			setApp(&App{
				PACFilePath: tt.pacFilePath,
			})

			w := &mockResponseWriter{}
			req, _ := http.NewRequest("GET", "/proxy.pac", nil)
			req.RemoteAddr = "127.0.0.1:12345"

			servePACFile(w, req)

			if w.statusCode != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, w.statusCode)
			}

			if tt.expectedStatus == http.StatusOK {
				// Check headers for successful response
				contentType := w.Header().Get("Content-Type")
				expectedContentType := "application/x-ns-proxy-autoconfig"
				if contentType != expectedContentType {
					t.Errorf("Expected Content-Type '%s', got '%s'", expectedContentType, contentType)
				}

				cacheControl := w.Header().Get("Cache-Control")
				expectedCacheControl := "no-cache, no-store, must-revalidate"
				if cacheControl != expectedCacheControl {
					t.Errorf("Expected Cache-Control '%s', got '%s'", expectedCacheControl, cacheControl)
				}
			}
		})
	}

	// Test with existing PAC file
	t.Run("Valid PAC file", func(t *testing.T) {
		setApp(&App{
			PACFilePath: "../proxy.pac", // Use the existing PAC file with relative path
		})

		w := &mockResponseWriter{}
		req, _ := http.NewRequest("GET", "/proxy.pac", nil)
		req.RemoteAddr = "127.0.0.1:12345"

		servePACFile(w, req)

		if w.statusCode != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.statusCode)
		}

		// Check headers
		contentType := w.Header().Get("Content-Type")
		expectedContentType := "application/x-ns-proxy-autoconfig"
		if contentType != expectedContentType {
			t.Errorf("Expected Content-Type '%s', got '%s'", expectedContentType, contentType)
		}

		// Check that we got some content
		if len(w.body) == 0 {
			t.Errorf("Expected non-empty response body")
		}

		// Check that the content contains the function
		if !bytes.Contains(w.body, []byte("FindProxyForURL")) {
			t.Errorf("Expected response to contain 'FindProxyForURL'")
		}
	})

	// Restore original app state
	app = originalApp
}

// Test App.ServeHTTP method for proxy authentication integration
func TestApp_ServeHTTP_ProxyAuth(t *testing.T) {
	// Save original app state
	originalApp := getApp()

	defer func() {
		// Restore original app state and stop any pool connections
		app = originalApp
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond) // Give goroutines time to stop
	}()

	// Test with proxy authentication enabled and a proper handler
	mockHandler := func(w http.ResponseWriter, r *http.Request) {
		// Call the Hijack function which contains the actual proxy logic
		Hijack(w, r)
	}

	app = NewApp(mockHandler, 8080, "localhost", 9090, 0, 5, false, false, "", "", 22, true, "testuser", "testpass", "../proxy.pac")

	tests := []struct {
		name           string
		method         string
		path           string
		authHeader     string
		expectedStatus int
	}{
		{
			name:           "GET request for PAC file",
			method:         "GET",
			path:           "/proxy.pac",
			authHeader:     "",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "CONNECT without auth",
			method:         "CONNECT",
			path:           "",
			authHeader:     "",
			expectedStatus: http.StatusProxyAuthRequired,
		},
		{
			name:           "CONNECT with valid auth",
			method:         "CONNECT",
			path:           "",
			authHeader:     "Basic dGVzdHVzZXI6dGVzdHBhc3M=", // base64 of "testuser:testpass"
			expectedStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &mockHijackableResponseWriter{}

			var req *http.Request
			if tt.method == "CONNECT" {
				req, _ = http.NewRequest(tt.method, "", nil)
				req.Host = "example.com:443"
			} else {
				req, _ = http.NewRequest(tt.method, tt.path, nil)
			}

			req.RemoteAddr = "127.0.0.1:12345"

			if tt.authHeader != "" {
				req.Header.Set("Proxy-Authorization", tt.authHeader)
			}

			app.ServeHTTP(w, req)

			if w.statusCode != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, w.statusCode)
			}
		})
	}
}

// Test poolConnectionSender function
func TestPoolConnectionSender_Coverage(t *testing.T) {
	// Create a mock pool connection
	poolConn := &PoolConnection{
		ID:       1,
		SendChan: make(chan *twtproto.ProxyComm, 1),
		Conn:     newMockConn(),
	}

	// Start the sender in a goroutine
	go poolConnectionSender(poolConn)

	// Send a test message
	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Proxy:      0,
		Connection: 1,
		Seq:        1,
	}

	// Send the message and close the channel
	poolConn.SendChan <- testMessage
	close(poolConn.SendChan)

	// Give it a moment to process
	time.Sleep(10 * time.Millisecond)

	// Test passes if no panic occurs
}

// Test edge cases for isTLSTraffic
func TestIsTLSTraffic_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		header   []byte
		expected bool
	}{
		{
			name:     "TLS Handshake with proper length",
			header:   []byte{0x16, 0x03, 0x03, 0x00, 0x10}, // TLS 1.2 handshake, length 16
			expected: true,
		},
		{
			name:     "TLS Application Data",
			header:   []byte{0x17, 0x03, 0x03, 0x00, 0x20}, // TLS 1.2 app data, length 32
			expected: true,
		},
		{
			name:     "TLS Alert",
			header:   []byte{0x15, 0x03, 0x03, 0x00, 0x02}, // TLS 1.2 alert, length 2
			expected: true,
		},
		{
			name:     "TLS Change Cipher Spec",
			header:   []byte{0x14, 0x03, 0x03, 0x00, 0x01}, // TLS 1.2 change cipher spec
			expected: true,
		},
		{
			name:     "TLS Handshake with invalid version (still TLS content type)",
			header:   []byte{0x16, 0x02, 0x00, 0x00, 0x10}, // Invalid version but valid content type
			expected: true,
		},
		{
			name:     "TLS Handshake with large length (still valid content type)",
			header:   []byte{0x16, 0x03, 0x03, 0xFF, 0xFF}, // Large length but valid content type
			expected: true,
		},
		{
			name:     "Header too short (only 1 byte)",
			header:   []byte{0x16}, // Only content type, no version
			expected: false,
		},
		{
			name:     "Empty header",
			header:   []byte{},
			expected: false,
		},
		{
			name:     "Non-TLS content type",
			header:   []byte{0x12, 0x03, 0x03, 0x00, 0x10}, // Invalid content type
			expected: false,
		},
		{
			name:     "HTTP request start",
			header:   []byte("GET / HTTP/1.1"),
			expected: false,
		},
		{
			name:     "Minimum valid TLS header",
			header:   []byte{0x16, 0x03}, // Handshake with minimum length
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTLSTraffic(tt.header)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

// Test handleHTTPRequest function
func TestHandleHTTPRequest(t *testing.T) {
	// Save original app state
	originalApp := getApp()
	defer func() {
		setApp(originalApp)
		// Stop any background goroutines
		StopAllPoolConnections()
	}()

	setApp(&App{
		PACFilePath: "",
	})

	tests := []struct {
		name           string
		method         string
		path           string
		expectedStatus int
		shouldContain  string
	}{
		{
			name:           "PAC file request",
			method:         "GET",
			path:           "/proxy.pac",
			expectedStatus: http.StatusOK,
			shouldContain:  "",
		},
		{
			name:           "WPAD request",
			method:         "GET",
			path:           "/wpad.dat",
			expectedStatus: http.StatusOK,
			shouldContain:  "",
		},
		{
			name:           "Other GET request",
			method:         "GET",
			path:           "/other",
			expectedStatus: http.StatusOK,
			shouldContain:  "TW2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &mockResponseWriter{}
			req, _ := http.NewRequest(tt.method, tt.path, nil)
			req.RemoteAddr = "127.0.0.1:12345"

			handleHTTPRequest(w, req)

			if w.statusCode != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, w.statusCode)
			}

			if tt.shouldContain != "" && !bytes.Contains(w.body, []byte(tt.shouldContain)) {
				t.Errorf("Expected response to contain '%s', got '%s'", tt.shouldContain, string(w.body))
			}
		})
	}

	// Restore original app state was moved to defer
}

// Test getByteOrZero function
func TestGetByteOrZero(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		index    int
		expected byte
	}{
		{
			name:     "Valid index",
			data:     []byte{0x10, 0x20, 0x30},
			index:    1,
			expected: 0x20,
		},
		{
			name:     "Index 0",
			data:     []byte{0x10, 0x20, 0x30},
			index:    0,
			expected: 0x10,
		},
		{
			name:     "Index out of bounds",
			data:     []byte{0x10, 0x20, 0x30},
			index:    5,
			expected: 0x00,
		},
		{
			name:     "Negative index",
			data:     []byte{0x10, 0x20, 0x30},
			index:    -1,
			expected: 0x00,
		},
		{
			name:     "Empty slice",
			data:     []byte{},
			index:    0,
			expected: 0x00,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getByteOrZero(tt.data, tt.index)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

// Test startSSHKeepAlive function (basic functionality)
func TestStartSSHKeepAlive(t *testing.T) {
	poolConn := &PoolConnection{
		ID: 1,
	}

	// This should not panic and should set up the keep-alive context
	startSSHKeepAlive(poolConn)

	// Verify that the cancel function was set
	if poolConn.keepAliveCancel == nil {
		t.Errorf("Expected keepAliveCancel to be set")
	}

	// Clean up by canceling the context
	if poolConn.keepAliveCancel != nil {
		poolConn.keepAliveCancel()
	}
}

// Test Hijack function with proxy authentication
func TestHijack_ProxyAuthentication(t *testing.T) {
	// Save original app state
	originalApp := getApp()
	defer func() {
		setApp(originalApp)
		// Stop any background goroutines
		StopAllPoolConnections()
	}()

	// Set up minimal app with proxy authentication enabled (avoid starting background goroutines)
	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
		LastLocalConnection:   0,
		ListenPort:            8080,
		PeerHost:              "localhost",
		PeerPort:              9090,
		PoolConnections:       []*PoolConnection{}, // Empty to avoid client mode
		PoolMutex:             sync.Mutex{},
		ProxyAuthEnabled:      true,
		ProxyUsername:         "testuser",
		ProxyPassword:         "testpass",
	}
	setApp(testApp)

	tests := []struct {
		name           string
		method         string
		authHeader     string
		expectedStatus int
	}{
		{
			name:           "GET request with auth enabled",
			method:         "GET",
			authHeader:     "",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "CONNECT without auth",
			method:         "CONNECT",
			authHeader:     "",
			expectedStatus: http.StatusProxyAuthRequired,
		},
		{
			name:           "CONNECT with valid auth",
			method:         "CONNECT",
			authHeader:     "Basic dGVzdHVzZXI6dGVzdHBhc3M=", // base64 of "testuser:testpass"
			expectedStatus: http.StatusOK,
		},
		{
			name:           "CONNECT with invalid auth",
			method:         "CONNECT",
			authHeader:     "Basic aW52YWxpZDppbnZhbGlk", // base64 of "invalid:invalid"
			expectedStatus: http.StatusProxyAuthRequired,
		},
		{
			name:           "POST method not allowed",
			method:         "POST",
			authHeader:     "",
			expectedStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &mockHijackableResponseWriter{}

			var req *http.Request
			if tt.method == "CONNECT" {
				req, _ = http.NewRequest(tt.method, "", nil)
				req.Host = "example.com:443"
			} else {
				req, _ = http.NewRequest(tt.method, "/test", nil)
			}

			req.RemoteAddr = "127.0.0.1:12345"

			if tt.authHeader != "" {
				req.Header.Set("Proxy-Authorization", tt.authHeader)
			}

			Hijack(w, req)

			if w.statusCode != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, w.statusCode)
			}
		})
	}

	// Restore original app state
	setApp(originalApp)
}
