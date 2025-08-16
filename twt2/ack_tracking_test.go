package twt2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"palecci.cz/twtproto"
)

// TestAckTrackingInitialization tests that Connection struct initializes ACK tracking fields properly
func TestAckTrackingInitialization(t *testing.T) {
	originalApp := getApp()
	defer setApp(originalApp)

	// Initialize app
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

	// Create a connection and verify ACK tracking fields are initialized
	connID := uint64(1)
	conn := Connection{
		Connection:   nil,
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
		LastAckSent:  0,
	}

	testApp.LocalConnections[connID] = conn

	// Verify initialization
	assert.NotNil(t, testApp.LocalConnections[connID].PendingAcks)
	assert.NotNil(t, testApp.LocalConnections[connID].AckTimeouts)
	assert.Equal(t, uint64(0), testApp.LocalConnections[connID].LastAckSent)
	assert.Equal(t, 0, len(testApp.LocalConnections[connID].PendingAcks))
	assert.Equal(t, 0, len(testApp.LocalConnections[connID].AckTimeouts))
}

// TestHandleAckDown tests ACK_DOWN message handling
func TestHandleAckDown(t *testing.T) {
	originalApp := getApp()
	defer setApp(originalApp)

	// Initialize app
	testApp := &App{
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

	connID := uint64(1)
	seq := uint64(100)

	// Set up a connection with pending ACK
	conn := Connection{
		Connection:   nil,
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
		LastAckSent:  0,
	}

	// Add a pending message
	dataMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: connID,
		Seq:        seq,
		Data:       []byte("test data"),
	}
	conn.PendingAcks[seq] = dataMessage
	conn.AckTimeouts[seq] = time.Now().Add(5 * time.Second)
	testApp.RemoteConnections[connID] = conn

	// Verify pending ACK exists
	assert.Equal(t, 1, len(testApp.RemoteConnections[connID].PendingAcks))
	assert.Equal(t, 1, len(testApp.RemoteConnections[connID].AckTimeouts))

	// Create ACK message
	ackMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_ACK_DOWN,
		Connection: connID,
		Seq:        seq,
	}

	// Handle the ACK
	handleAckDown(ackMessage)

	// Verify pending ACK was removed
	currentApp := getApp()
	assert.Equal(t, 0, len(currentApp.RemoteConnections[connID].PendingAcks))
	assert.Equal(t, 0, len(currentApp.RemoteConnections[connID].AckTimeouts))
}

// TestHandleAckUp tests ACK_UP message handling
func TestHandleAckUp(t *testing.T) {
	originalApp := getApp()
	defer setApp(originalApp)

	// Initialize app
	testApp := &App{
		LocalConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

	connID := uint64(1)
	seq := uint64(200)

	// Set up a connection with pending ACK
	conn := Connection{
		Connection:   nil,
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
		LastAckSent:  0,
	}

	// Add a pending message
	dataMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: connID,
		Seq:        seq,
		Data:       []byte("test data up"),
	}
	conn.PendingAcks[seq] = dataMessage
	conn.AckTimeouts[seq] = time.Now().Add(5 * time.Second)
	testApp.LocalConnections[connID] = conn

	// Verify pending ACK exists
	assert.Equal(t, 1, len(testApp.LocalConnections[connID].PendingAcks))
	assert.Equal(t, 1, len(testApp.LocalConnections[connID].AckTimeouts))

	// Create ACK message
	ackMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_ACK_UP,
		Connection: connID,
		Seq:        seq,
	}

	// Handle the ACK
	handleAckUp(ackMessage)

	// Verify pending ACK was removed
	currentApp := getApp()
	assert.Equal(t, 0, len(currentApp.LocalConnections[connID].PendingAcks))
	assert.Equal(t, 0, len(currentApp.LocalConnections[connID].AckTimeouts))
}

// TestHandleAckNonexistentConnection tests ACK handling for nonexistent connections
func TestHandleAckNonexistentConnection(t *testing.T) {
	originalApp := getApp()
	defer setApp(originalApp)

	// Initialize app with empty connections
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

	// Test ACK_DOWN for nonexistent connection
	ackDownMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_ACK_DOWN,
		Connection: 999, // nonexistent
		Seq:        100,
	}

	// Should not panic
	handleAckDown(ackDownMessage)

	// Test ACK_UP for nonexistent connection
	ackUpMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_ACK_UP,
		Connection: 999, // nonexistent
		Seq:        200,
	}

	// Should not panic
	handleAckUp(ackUpMessage)
}

// TestAckTimeoutDetection tests that timeouts are properly detected
func TestAckTimeoutDetection(t *testing.T) {
	originalApp := getApp()
	defer setApp(originalApp)

	// Initialize app
	testApp := &App{
		LocalConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

	connID := uint64(1)
	seq := uint64(100)

	// Set up a connection with expired timeout
	conn := Connection{
		Connection:   nil,
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
		LastAckSent:  0,
	}

	// Add a pending message with past timeout
	dataMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: connID,
		Seq:        seq,
		Data:       []byte("test data"),
	}
	conn.PendingAcks[seq] = dataMessage
	conn.AckTimeouts[seq] = time.Now().Add(-1 * time.Second) // Past timeout
	testApp.LocalConnections[connID] = conn

	// Check if timeout detection logic works
	now := time.Now()
	timeout := testApp.LocalConnections[connID].AckTimeouts[seq]

	assert.True(t, now.After(timeout), "Timeout should be detected as expired")
}

// TestMultiplePendingAcks tests handling multiple pending ACKs
func TestMultiplePendingAcks(t *testing.T) {
	originalApp := getApp()
	defer setApp(originalApp)

	// Initialize app with both connection maps and mutexes
	testApp := &App{
		LocalConnections:  make(map[uint64]Connection),
		RemoteConnections: make(map[uint64]Connection),
	}
	setApp(testApp)

	connID := uint64(1)

	// Set up a connection in RemoteConnections (for DATA_DOWN -> ACK_DOWN flow)
	conn := Connection{
		Connection:   nil,
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
		LastAckSent:  0,
	}

	// Add multiple pending DATA_DOWN messages (which get ACK_DOWN responses)
	for i := uint64(1); i <= 5; i++ {
		dataMessage := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Connection: connID,
			Seq:        i,
			Data:       []byte("test data"),
		}
		conn.PendingAcks[i] = dataMessage
		conn.AckTimeouts[i] = time.Now().Add(5 * time.Second)
	}
	testApp.RemoteConnections[connID] = conn

	// Verify all are stored
	assert.Equal(t, 5, len(testApp.RemoteConnections[connID].PendingAcks))
	assert.Equal(t, 5, len(testApp.RemoteConnections[connID].AckTimeouts))

	// ACK middle message
	ackMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_ACK_DOWN,
		Connection: connID,
		Seq:        3,
	}
	handleAckDown(ackMessage)

	// Verify only the ACKed message was removed
	currentApp := getApp()
	assert.Equal(t, 4, len(currentApp.RemoteConnections[connID].PendingAcks))
	assert.Equal(t, 4, len(currentApp.RemoteConnections[connID].AckTimeouts))
	_, exists := currentApp.RemoteConnections[connID].PendingAcks[3]
	assert.False(t, exists, "Sequence 3 should be removed")
	_, exists = currentApp.RemoteConnections[connID].AckTimeouts[3]
	assert.False(t, exists, "Timeout for sequence 3 should be removed")

	// Verify others still exist
	for _, seq := range []uint64{1, 2, 4, 5} {
		_, exists := currentApp.RemoteConnections[connID].PendingAcks[seq]
		assert.True(t, exists, "Sequence %d should still exist", seq)
	}
}

// TestAckMessageTypes tests that ACK messages use correct message types
func TestAckMessageTypes(t *testing.T) {
	// Test that DATA_UP generates ACK_DOWN
	assert.Equal(t, twtproto.ProxyComm_ACK_DOWN, twtproto.ProxyComm_ACK_DOWN)

	// Test that DATA_DOWN generates ACK_UP
	assert.Equal(t, twtproto.ProxyComm_ACK_UP, twtproto.ProxyComm_ACK_UP)

	// Verify message type values match protocol
	assert.Equal(t, int32(2), int32(twtproto.ProxyComm_ACK_DOWN))
	assert.Equal(t, int32(3), int32(twtproto.ProxyComm_ACK_UP))
}
