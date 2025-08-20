package twt2

import (
	"net"
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// TestPoolConnectionFailurePreservesLogicalConnections tests that when a pool connection fails,
// ALL logical connections should be preserved and automatically rerouted to healthy pool connections.
// Pool connections are just transport mechanisms - logical connection state should never be lost.
func TestPoolConnectionFailurePreservesLogicalConnections(t *testing.T) {
	// Create a test app with multiple pool connections
	app := &App{
		PoolConnections:   make([]*PoolConnection, 0),
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(app)
	defer func() { setApp(nil) }()

	// Create mock connections for testing
	conn1, conn2 := net.Pipe()
	conn3, conn4 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()
	defer conn3.Close()
	defer conn4.Close()

	// Create two pool connections
	poolConn1 := &PoolConnection{
		ID:              1,
		Conn:            conn1,
		SendChan:        make(chan *twtproto.ProxyComm, 10),
		PriorityChan:    make(chan *twtproto.ProxyComm, 10),
		Healthy:         true,
		Stats:           NewPoolConnectionStats(),
		LastHealthCheck: time.Now(),
	}

	poolConn2 := &PoolConnection{
		ID:              2,
		Conn:            conn3,
		SendChan:        make(chan *twtproto.ProxyComm, 10),
		PriorityChan:    make(chan *twtproto.ProxyComm, 10),
		Healthy:         true,
		Stats:           NewPoolConnectionStats(),
		LastHealthCheck: time.Now(),
	}

	app.PoolConnections = []*PoolConnection{poolConn1, poolConn2}

	// Create some active local connections (simulating ongoing transfers)
	app.LocalConnections[100] = Connection{
		Connection:   conn2, // This is paired with poolConn1
		LastSeqIn:    5,
		NextSeqOut:   6,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*PendingAck),
	}

	app.LocalConnections[101] = Connection{
		Connection:   conn4, // This is paired with poolConn2
		LastSeqIn:    3,
		NextSeqOut:   4,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*PendingAck),
	}

	// Add some pending messages to simulate active transfers
	testMessage1 := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: 100,
		Seq:        5,
		Data:       []byte("test data 1"),
	}

	testMessage2 := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: 101,
		Seq:        3,
		Data:       []byte("test data 2"),
	}

	// Add pending ACKs to simulate ongoing transfers with pool connection tracking
	conn100 := app.LocalConnections[100]
	conn100.PendingAcks[5] = &PendingAck{Message: testMessage1, PoolConnectionID: 1} // sent via poolConn1
	app.LocalConnections[100] = conn100

	conn101 := app.LocalConnections[101]
	conn101.PendingAcks[3] = &PendingAck{Message: testMessage2, PoolConnectionID: 2} // sent via poolConn2
	app.LocalConnections[101] = conn101

	// Verify initial state
	if len(app.LocalConnections) != 2 {
		t.Fatalf("Expected 2 local connections, got %d", len(app.LocalConnections))
	}

	if len(app.LocalConnections[100].PendingAcks) != 1 {
		t.Fatalf("Expected 1 pending ACK for connection 100, got %d", len(app.LocalConnections[100].PendingAcks))
	}

	if len(app.LocalConnections[101].PendingAcks) != 1 {
		t.Fatalf("Expected 1 pending ACK for connection 101, got %d", len(app.LocalConnections[101].PendingAcks))
	}

	// Now simulate a failure in poolConn1 by calling handlePoolConnectionFailure
	// This is what now happens when poolConnectionReceiver detects a failure
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This simulates what happens when poolConnectionReceiver detects a failure
		handlePoolConnectionFailure(poolConn1)
	}()

	wg.Wait()

	// EXPECTED BEHAVIOR: When a pool connection fails, NO client/server connections should be lost
	// Pool connections are just transport - logical connections should be preserved and rerouted

	// ALL local connections should still be active
	if len(app.LocalConnections) != 2 {
		t.Errorf("BUG: Expected ALL 2 local connections to remain active when pool connection fails, got %d", len(app.LocalConnections))
		t.Errorf("Pool connections are just transport - logical connections should be preserved")
		return
	}

	// Connection 100 should still exist and have its state preserved
	if conn100, exists := app.LocalConnections[100]; !exists {
		t.Errorf("Connection 100 should still exist - pool connection failure shouldn't kill logical connections")
	} else {
		if len(conn100.PendingAcks) != 1 {
			t.Errorf("Connection 100 should still have its pending ACKs preserved, got %d", len(conn100.PendingAcks))
		}
		if conn100.LastSeqIn != 5 {
			t.Errorf("Connection 100 sequence state should be preserved, expected LastSeqIn=5, got %d", conn100.LastSeqIn)
		}
	}

	// Connection 101 should still exist and have its state preserved
	if conn101, exists := app.LocalConnections[101]; !exists {
		t.Errorf("Connection 101 should still exist - pool connection failure shouldn't kill logical connections")
	} else {
		if len(conn101.PendingAcks) != 1 {
			t.Errorf("Connection 101 should still have its pending ACKs preserved, got %d", len(conn101.PendingAcks))
		}
		if conn101.LastSeqIn != 3 {
			t.Errorf("Connection 101 sequence state should be preserved, expected LastSeqIn=3, got %d", conn101.LastSeqIn)
		}
	}

	// When the fix is implemented, these connections should automatically be rerouted
	// to healthy pool connections without any data loss
}

// TestServerClientConnectionFailurePreservesRemoteConnections tests that when a server-side
// client connection fails, ALL remote connections should be preserved and automatically
// rerouted to healthy client connections.
func TestServerClientConnectionFailurePreservesRemoteConnections(t *testing.T) {
	// Create a test app for server mode
	app := &App{
		PoolConnections:   make([]*PoolConnection, 0), // Server mode: no pool connections
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(app)
	defer func() { setApp(nil) }()

	// Create mock client connections for server side
	conn1, conn2 := net.Pipe()
	conn3, conn4 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()
	defer conn3.Close()
	defer conn4.Close()

	// Create two server client connections (incoming from clients)
	clientConn1 := &ServerClientConnection{
		ID:       1,
		Conn:     conn1,
		Active:   true,
		LastUsed: time.Now(),
	}

	clientConn2 := &ServerClientConnection{
		ID:       2,
		Conn:     conn3,
		Active:   true,
		LastUsed: time.Now(),
	}

	// Initialize server client connections
	serverClientConnections = []*ServerClientConnection{clientConn1, clientConn2}

	// Create some active remote connections (simulating ongoing transfers)
	app.RemoteConnections[200] = Connection{
		Connection:   conn2, // This would normally connect to target service
		LastSeqIn:    10,
		NextSeqOut:   11,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*PendingAck),
	}

	app.RemoteConnections[201] = Connection{
		Connection:   conn4, // This would normally connect to target service
		LastSeqIn:    8,
		NextSeqOut:   9,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*PendingAck),
	}

	// Add some pending messages to simulate active transfers
	testMessage1 := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: 200,
		Seq:        10,
		Data:       []byte("server response 1"),
	}

	testMessage2 := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: 201,
		Seq:        8,
		Data:       []byte("server response 2"),
	}

	// Add pending ACKs to simulate ongoing transfers with pool connection tracking
	conn200 := app.RemoteConnections[200]
	conn200.PendingAcks[10] = &PendingAck{Message: testMessage1, PoolConnectionID: 0} // server-side (no pool)
	app.RemoteConnections[200] = conn200

	conn201 := app.RemoteConnections[201]
	conn201.PendingAcks[8] = &PendingAck{Message: testMessage2, PoolConnectionID: 0} // server-side (no pool)
	app.RemoteConnections[201] = conn201

	// Verify initial state
	if len(app.RemoteConnections) != 2 {
		t.Fatalf("Expected 2 remote connections, got %d", len(app.RemoteConnections))
	}

	if len(serverClientConnections) != 2 {
		t.Fatalf("Expected 2 server client connections, got %d", len(serverClientConnections))
	}

	// Now simulate a failure in clientConn1 by calling handleServerClientConnectionFailure
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This simulates what happens when a server-side client connection fails
		handleServerClientConnectionFailure(clientConn1)
	}()

	wg.Wait()

	// EXPECTED BEHAVIOR: When a server client connection fails, NO remote connections should be lost
	// Client connections are just transport - remote connections should be preserved and rerouted

	// ALL remote connections should still be active
	if len(app.RemoteConnections) != 2 {
		t.Errorf("BUG: Expected ALL 2 remote connections to remain active when server client connection fails, got %d", len(app.RemoteConnections))
		t.Errorf("Server client connections are just transport - remote connections should be preserved")
		return
	}

	// Connection 200 should still exist and have its state preserved
	if conn200, exists := app.RemoteConnections[200]; !exists {
		t.Errorf("Remote connection 200 should still exist - server client connection failure shouldn't kill remote connections")
	} else {
		if len(conn200.PendingAcks) != 1 {
			t.Errorf("Remote connection 200 should still have its pending ACKs preserved, got %d", len(conn200.PendingAcks))
		}
		if conn200.LastSeqIn != 10 {
			t.Errorf("Remote connection 200 sequence state should be preserved, expected LastSeqIn=10, got %d", conn200.LastSeqIn)
		}
	}

	// Connection 201 should still exist and have its state preserved
	if conn201, exists := app.RemoteConnections[201]; !exists {
		t.Errorf("Remote connection 201 should still exist - server client connection failure shouldn't kill remote connections")
	} else {
		if len(conn201.PendingAcks) != 1 {
			t.Errorf("Remote connection 201 should still have its pending ACKs preserved, got %d", len(conn201.PendingAcks))
		}
		if conn201.LastSeqIn != 8 {
			t.Errorf("Remote connection 201 sequence state should be preserved, expected LastSeqIn=8, got %d", conn201.LastSeqIn)
		}
	}

	// Verify that healthy server client connections remain
	if len(serverClientConnections) != 1 {
		t.Errorf("Expected 1 healthy server client connection to remain, got %d", len(serverClientConnections))
	}

	// When the fix is working correctly, these remote connections should automatically be rerouted
	// to healthy server client connections without any data loss
}

// TestCurrentBuggyBehavior demonstrates the current incorrect behavior where
// pool connection failure kills all logical connections (this should NOT happen)
func TestCurrentBuggyBehavior(t *testing.T) {
	// Create a test app with connections
	app := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(app)
	defer func() { setApp(nil) }()

	// Create mock connections
	conn1, conn2 := net.Pipe()
	conn3, conn4 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()
	defer conn3.Close()
	defer conn4.Close()

	// Add multiple connections
	app.LocalConnections[100] = Connection{
		Connection:   conn1,
		LastSeqIn:    5,
		NextSeqOut:   6,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*PendingAck),
	}

	app.LocalConnections[101] = Connection{
		Connection:   conn3,
		LastSeqIn:    3,
		NextSeqOut:   4,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*PendingAck),
	}

	// Verify we have connections
	if len(app.LocalConnections) != 2 {
		t.Fatalf("Expected 2 connections, got %d", len(app.LocalConnections))
	}

	// Call clearConnectionState (this is what happens when a pool connection fails)
	clearConnectionState()

	// This test documents the current buggy behavior: ALL connections are incorrectly killed
	if len(app.LocalConnections) != 0 {
		t.Errorf("CURRENT BUG: clearConnectionState() incorrectly kills all connections")
	}

	if len(app.RemoteConnections) != 0 {
		t.Errorf("CURRENT BUG: clearConnectionState() incorrectly kills all remote connections")
	}

	t.Logf("CURRENT BUG DOCUMENTED: clearConnectionState() incorrectly kills ALL connections")
	t.Logf("EXPECTED: Pool connection failure should preserve all logical connections and reroute them")
	t.Logf("RATIONALE: Pool connections are transport layer - logical connections should be independent")
}
