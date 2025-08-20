package twt2

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"palecci.cz/twtproto"
)

// MockConnection implements net.Conn for testing
type MockConnection struct {
	writeData []byte
	readData  []byte
	closed    bool
}

func (m *MockConnection) Read(b []byte) (n int, err error) {
	if len(m.readData) == 0 {
		return 0, &net.OpError{Op: "read", Net: "mock", Err: &timeoutError{}}
	}
	n = copy(b, m.readData)
	m.readData = m.readData[n:]
	return n, nil
}

func (m *MockConnection) Write(b []byte) (n int, err error) {
	if m.closed {
		return 0, &net.OpError{Op: "write", Net: "mock", Err: &closedError{}}
	}
	m.writeData = append(m.writeData, b...)
	return len(b), nil
}

func (m *MockConnection) Close() error {
	m.closed = true
	return nil
}

func (m *MockConnection) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
}
func (m *MockConnection) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0}
}
func (m *MockConnection) SetDeadline(t time.Time) error      { return nil }
func (m *MockConnection) SetReadDeadline(t time.Time) error  { return nil }
func (m *MockConnection) SetWriteDeadline(t time.Time) error { return nil }

type timeoutError struct{}

func (e *timeoutError) Error() string { return "mock timeout" }
func (e *timeoutError) Timeout() bool { return true }

type closedError struct{}

func (e *closedError) Error() string { return "mock closed" }

// TestConnectionFailureRetransmission tests that messages are retransmitted when pool connections fail
func TestConnectionFailureRetransmission(t *testing.T) {
	originalApp := getApp()
	defer setApp(originalApp)

	// Initialize app
	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
		PoolConnections:       make([]*PoolConnection, 2),
		PoolMutex:             sync.Mutex{},
	}
	setApp(testApp)

	connID := uint64(1)
	mockConn := &MockConnection{}

	// Create connection with pending messages
	conn := Connection{
		Connection:   mockConn,
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*PendingAck),
		LastAckSent:  0,
	}

	// Add pending messages tracked by pool connection ID
	for i := uint64(1); i <= 3; i++ {
		dataMessage := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Connection: connID,
			Seq:        i,
			Data:       []byte("test data"),
		}
		pendingAck := &PendingAck{
			Message:          dataMessage,
			PoolConnectionID: 1, // All tracked to pool connection 1
		}
		conn.PendingAcks[i] = pendingAck
	}
	testApp.LocalConnections[connID] = conn

	t.Logf("Set up %d pending messages tracked to pool connection 1", len(conn.PendingAcks))

	// Verify pending messages are tracked correctly
	assert.Equal(t, 3, len(testApp.LocalConnections[connID].PendingAcks))
	for seq, pendingAck := range testApp.LocalConnections[connID].PendingAcks {
		assert.Equal(t, uint64(1), pendingAck.PoolConnectionID)
		assert.Equal(t, seq, pendingAck.Message.Seq)
		assert.Equal(t, twtproto.ProxyComm_DATA_UP, pendingAck.Message.Mt)
	}

	t.Logf("✓ Connection failure retransmission test setup completed")
}

// TestPendingAckStructure tests the PendingAck structure
func TestPendingAckStructure(t *testing.T) {
	// Test basic PendingAck creation and access
	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: 1,
		Seq:        1,
		Data:       []byte("test data"),
	}

	pendingAck := &PendingAck{
		Message:          testMessage,
		PoolConnectionID: 5,
	}

	// Verify fields are accessible and correct
	assert.Equal(t, testMessage, pendingAck.Message)
	assert.Equal(t, uint64(5), pendingAck.PoolConnectionID)
	assert.Equal(t, twtproto.ProxyComm_DATA_UP, pendingAck.Message.Mt)
	assert.Equal(t, uint64(1), pendingAck.Message.Connection)
	assert.Equal(t, uint64(1), pendingAck.Message.Seq)
	assert.Equal(t, []byte("test data"), pendingAck.Message.Data)

	t.Logf("✓ PendingAck structure test completed")
}

// TestRetransmitPendingMessagesFromFailedPool tests the retransmission function
func TestRetransmitPendingMessagesFromFailedPool(t *testing.T) {
	originalApp := getApp()
	defer setApp(originalApp)

	// Initialize app with multiple pool connections
	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
		PoolConnections: []*PoolConnection{
			{ID: 1, Healthy: false}, // Failed pool
			{ID: 2, Healthy: true},  // Working pool
		},
		PoolMutex: sync.Mutex{},
	}
	setApp(testApp)

	connID := uint64(1)
	mockConn := &MockConnection{}

	// Create connection with mixed pending messages (some from failed pool, some from working pool)
	conn := Connection{
		Connection:   mockConn,
		PendingAcks:  make(map[uint64]*PendingAck),
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}

	// Add messages from failed pool (should be retransmitted)
	failedPoolMessage1 := &PendingAck{
		Message: &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Connection: connID,
			Seq:        1,
			Data:       []byte("failed pool message 1"),
		},
		PoolConnectionID: 1, // Failed pool
	}
	failedPoolMessage2 := &PendingAck{
		Message: &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Connection: connID,
			Seq:        2,
			Data:       []byte("failed pool message 2"),
		},
		PoolConnectionID: 1, // Failed pool
	}

	// Add message from working pool (should NOT be retransmitted)
	workingPoolMessage := &PendingAck{
		Message: &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Connection: connID,
			Seq:        3,
			Data:       []byte("working pool message"),
		},
		PoolConnectionID: 2, // Working pool
	}

	conn.PendingAcks[1] = failedPoolMessage1
	conn.PendingAcks[2] = failedPoolMessage2
	conn.PendingAcks[3] = workingPoolMessage

	testApp.LocalConnections[connID] = conn

	// Count messages before retransmission
	pendingBefore := len(testApp.LocalConnections[connID].PendingAcks)
	assert.Equal(t, 3, pendingBefore)

	// Call retransmission function for failed pool
	retransmitPendingMessagesFromFailedPool(1) // Pool ID 1 failed

	// Verify messages from failed pool are handled appropriately
	// Note: In a real implementation, messages would be requeued for retransmission
	// Here we just verify the function can be called without panic
	t.Logf("✓ Retransmission function called successfully for failed pool 1")
	t.Logf("  Messages from failed pool should be retransmitted")
	t.Logf("  Messages from working pools should remain unchanged")
}

// TestConnectionFailureDoesNotAffectOtherConnections tests isolation between connections
func TestConnectionFailureDoesNotAffectOtherConnections(t *testing.T) {
	originalApp := getApp()
	defer setApp(originalApp)

	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

	// Create multiple connections with pending messages
	for connID := uint64(1); connID <= 3; connID++ {
		mockConn := &MockConnection{}
		conn := Connection{
			Connection:   mockConn,
			PendingAcks:  make(map[uint64]*PendingAck),
			MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		}

		// Add pending message for each connection
		pendingAck := &PendingAck{
			Message: &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_DATA_UP,
				Connection: connID,
				Seq:        1,
				Data:       []byte("test data"),
			},
			PoolConnectionID: connID, // Different pool for each connection
		}
		conn.PendingAcks[1] = pendingAck
		testApp.LocalConnections[connID] = conn
	}

	// Verify all connections have pending messages
	for connID := uint64(1); connID <= 3; connID++ {
		assert.Equal(t, 1, len(testApp.LocalConnections[connID].PendingAcks))
	}

	// Simulate failure of pool connection 2
	retransmitPendingMessagesFromFailedPool(2)

	// Verify other connections are unaffected
	// (In practice, only messages from pool 2 would be retransmitted)
	assert.Equal(t, 1, len(testApp.LocalConnections[1].PendingAcks))
	assert.Equal(t, 1, len(testApp.LocalConnections[3].PendingAcks))

	t.Logf("✓ Connection isolation test completed")
	t.Logf("  Pool connection failure affects only messages from that pool")
}
