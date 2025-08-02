package twt2

import (
	"net/http"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

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
