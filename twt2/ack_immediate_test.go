package twt2

import (
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// TestAckImmediateProcessing verifies that ACK messages are processed immediately
// without sequence checking, even when they arrive out of order
func TestAckImmediateProcessing(t *testing.T) {
	// Save original app
	originalApp := app
	defer func() { app = originalApp }()

	// Create test app
	app = &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Set up local connection with NextSeqOut = 0 (expecting sequence 0)
	mockLocalConn := newMockConn()
	app.LocalConnections[1] = Connection{
		Connection:   mockLocalConn,
		NextSeqOut:   0, // This is key - expecting sequence 0
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
	}

	// Set up remote connection with NextSeqOut = 0
	mockRemoteConn := newMockConn()
	app.RemoteConnections[1] = Connection{
		Connection:   mockRemoteConn,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
	}

	// Add some pending ACKs for testing
	app.LocalConnections[1].PendingAcks[5] = &twtproto.ProxyComm{Mt: twtproto.ProxyComm_DATA_UP, Seq: 5}
	app.RemoteConnections[1].PendingAcks[3] = &twtproto.ProxyComm{Mt: twtproto.ProxyComm_DATA_DOWN, Seq: 3}

	// Test 1: ACK_UP with sequence 5 should be processed immediately
	// even though connection expects sequence 0
	ackUpMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_ACK_UP,
		Connection: 1,
		Seq:        5, // Out of order - this should still be processed
	}

	t.Logf("Processing ACK_UP with seq=5 while connection expects seq=0")
	handleProxycommMessageWithPoolConn(ackUpMessage, nil)

	// Verify that ACK_UP was processed (pending ACK should be removed)
	app.LocalConnectionMutex.Lock()
	pendingAcks := app.LocalConnections[1].PendingAcks
	app.LocalConnectionMutex.Unlock()

	if _, exists := pendingAcks[5]; exists {
		t.Errorf("ACK_UP seq=5 was not processed - pending ACK still exists")
	} else {
		t.Logf("✓ ACK_UP seq=5 processed successfully")
	}

	// Test 2: ACK_DOWN with sequence 3 should be processed immediately
	ackDownMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_ACK_DOWN,
		Connection: 1,
		Seq:        3, // Out of order - this should still be processed
	}

	t.Logf("Processing ACK_DOWN with seq=3 while connection expects seq=0")
	handleProxycommMessageWithPoolConn(ackDownMessage, nil)

	// Verify that ACK_DOWN was processed (pending ACK should be removed)
	app.RemoteConnectionMutex.Lock()
	pendingAcks = app.RemoteConnections[1].PendingAcks
	app.RemoteConnectionMutex.Unlock()

	if _, exists := pendingAcks[3]; exists {
		t.Errorf("ACK_DOWN seq=3 was not processed - pending ACK still exists")
	} else {
		t.Logf("✓ ACK_DOWN seq=3 processed successfully")
	}

	// Test 3: Verify that non-ACK messages are still subject to sequence checking
	// DATA_DOWN with sequence 5 should be queued (not processed) because connection expects sequence 0
	dataDownMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: 1,
		Seq:        5, // Out of order - this should be queued
		Data:       []byte("test data"),
	}

	t.Logf("Processing DATA_DOWN with seq=5 while connection expects seq=0 (should be queued)")
	handleProxycommMessageWithPoolConn(dataDownMessage, nil)

	// Verify that DATA_DOWN was queued (not processed immediately)
	app.LocalConnectionMutex.Lock()
	messageQueue := app.LocalConnections[1].MessageQueue
	app.LocalConnectionMutex.Unlock()

	if _, exists := messageQueue[5]; !exists {
		t.Errorf("DATA_DOWN seq=5 was not queued as expected")
	} else {
		t.Logf("✓ DATA_DOWN seq=5 correctly queued due to sequence mismatch")
	}

	t.Logf("✓ ACK messages processed immediately without sequence checking")
	t.Logf("✓ Non-ACK messages still subject to proper sequence validation")
}
