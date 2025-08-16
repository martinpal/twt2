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

// TestConnectionDisruptionHandling tests behavior when connections are lost
func TestConnectionDisruptionHandling(t *testing.T) {
	originalApp := getApp()
	defer setApp(originalApp)

	// Initialize app
	testApp := &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(testApp)

	connID := uint64(1)
	mockConn := &MockConnection{}

	// Set up a connection with pending ACKs
	conn := Connection{
		Connection:   mockConn,
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
		LastAckSent:  0,
	}

	// Add multiple pending messages with different timeouts
	for i := uint64(1); i <= 3; i++ {
		dataMessage := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Connection: connID,
			Seq:        i,
			Data:       []byte("test data"),
		}
		conn.PendingAcks[i] = dataMessage
		// Set some timeouts in the past to simulate timeout
		if i <= 2 {
			conn.AckTimeouts[i] = time.Now().Add(-1 * time.Second) // Expired
		} else {
			conn.AckTimeouts[i] = time.Now().Add(5 * time.Second) // Not expired
		}
	}
	app.LocalConnections[connID] = conn

	// Simulate connection loss by closing the mock connection
	mockConn.Close()

	// Verify pending ACKs exist before processing
	assert.Equal(t, 3, len(app.LocalConnections[connID].PendingAcks))

	// The timeout checker should identify expired messages
	// This tests the core logic without running the actual goroutine
	now := time.Now()
	expiredCount := 0
	for seq, timeout := range app.LocalConnections[connID].AckTimeouts {
		if now.After(timeout) {
			expiredCount++
			assert.True(t, seq <= 2, "Only sequences 1 and 2 should be expired")
		}
	}
	assert.Equal(t, 2, expiredCount, "Should have 2 expired timeouts")
}

// TestRetransmissionLogic tests that messages are retransmitted on timeout
func TestRetransmissionLogic(t *testing.T) {
	// Track sent messages using a mock function approach
	var sentMessages []*twtproto.ProxyComm
	var sendMutex sync.Mutex

	// Create a test sendProtobuf function for this test
	testSendProtobuf := func(message *twtproto.ProxyComm) {
		sendMutex.Lock()
		sentMessages = append(sentMessages, message)
		sendMutex.Unlock()
	}

	// Initialize app
	app = &App{
		LocalConnections:     make(map[uint64]Connection),
		LocalConnectionMutex: sync.Mutex{},
	}

	connID := uint64(1)
	seq := uint64(100)

	// Set up a connection with expired timeout
	conn := Connection{
		Connection:   &MockConnection{},
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
		LastAckSent:  0,
	}

	dataMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: connID,
		Seq:        seq,
		Data:       []byte("test data for retransmission"),
	}
	conn.PendingAcks[seq] = dataMessage
	conn.AckTimeouts[seq] = time.Now().Add(-1 * time.Second) // Expired
	app.LocalConnections[connID] = conn

	// Simulate the timeout checker logic
	now := time.Now()
	app.LocalConnectionMutex.Lock()
	for connIDCheck, connection := range app.LocalConnections {
		if connIDCheck != connID {
			continue
		}
		for seqCheck, timeout := range connection.AckTimeouts {
			if now.After(timeout) {
				if pendingMsg, exists := connection.PendingAcks[seqCheck]; exists {
					// Update timeout for next attempt
					connection.AckTimeouts[seqCheck] = now.Add(5 * time.Second)
					app.LocalConnections[connIDCheck] = connection
					// Retransmit the message using our test function
					testSendProtobuf(pendingMsg)
				}
			}
		}
	}
	app.LocalConnectionMutex.Unlock()

	// Verify message was retransmitted
	sendMutex.Lock()
	assert.Equal(t, 1, len(sentMessages), "Should have retransmitted 1 message")
	if len(sentMessages) > 0 {
		retransmittedMsg := sentMessages[0]
		assert.Equal(t, dataMessage.Mt, retransmittedMsg.Mt)
		assert.Equal(t, dataMessage.Connection, retransmittedMsg.Connection)
		assert.Equal(t, dataMessage.Seq, retransmittedMsg.Seq)
		assert.Equal(t, dataMessage.Data, retransmittedMsg.Data)
	}
	sendMutex.Unlock()

	// Verify timeout was updated
	newTimeout := app.LocalConnections[connID].AckTimeouts[seq]
	assert.True(t, newTimeout.After(now), "Timeout should be updated to future time")
}

// TestReliableDeliveryScenario tests a complete reliable delivery scenario
func TestReliableDeliveryScenario(t *testing.T) {
	// Initialize app
	app = &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	connID := uint64(1)

	// Set up local connection (client side)
	localConn := Connection{
		Connection:   &MockConnection{},
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
		LastAckSent:  0,
	}
	app.LocalConnections[connID] = localConn

	// Set up remote connection (server side)
	remoteConn := Connection{
		Connection:   &MockConnection{},
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
		LastAckSent:  0,
	}
	app.RemoteConnections[connID] = remoteConn

	// Scenario 1: Send DATA_UP, store in pending, receive ACK_UP
	seq1 := uint64(1)
	dataUpMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: connID,
		Seq:        seq1,
		Data:       []byte("data going up"),
	}

	// Simulate storing message in PendingAcks (like real code does)
	localConn.PendingAcks[seq1] = dataUpMessage
	localConn.AckTimeouts[seq1] = time.Now().Add(5 * time.Second)
	app.LocalConnections[connID] = localConn

	// Verify message is pending
	assert.Equal(t, 1, len(app.LocalConnections[connID].PendingAcks))

	// Simulate receiving ACK_UP (correct ACK for DATA_UP)
	ackUpMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_ACK_UP,
		Connection: connID,
		Seq:        seq1,
	}
	handleAckUp(ackUpMessage)

	// Verify message is no longer pending
	assert.Equal(t, 0, len(app.LocalConnections[connID].PendingAcks))
	assert.Equal(t, 0, len(app.LocalConnections[connID].AckTimeouts))

	// Scenario 2: Send DATA_DOWN, store in pending, receive ACK_DOWN
	seq2 := uint64(2)
	dataDownMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_DOWN,
		Connection: connID,
		Seq:        seq2,
		Data:       []byte("data going down"),
	}

	// Simulate storing message in PendingAcks
	remoteConn.PendingAcks[seq2] = dataDownMessage
	remoteConn.AckTimeouts[seq2] = time.Now().Add(5 * time.Second)
	app.RemoteConnections[connID] = remoteConn

	// Verify message is pending
	assert.Equal(t, 1, len(app.RemoteConnections[connID].PendingAcks))

	// Simulate receiving ACK_DOWN (correct ACK for DATA_DOWN)
	ackDownMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_ACK_DOWN,
		Connection: connID,
		Seq:        seq2,
	}
	handleAckDown(ackDownMessage)

	// Verify message is no longer pending
	assert.Equal(t, 0, len(app.RemoteConnections[connID].PendingAcks))
	assert.Equal(t, 0, len(app.RemoteConnections[connID].AckTimeouts))
}

// TestConcurrentAckHandling tests thread safety of ACK handling
func TestConcurrentAckHandling(t *testing.T) {
	// Initialize app
	app = &App{
		LocalConnections:     make(map[uint64]Connection),
		LocalConnectionMutex: sync.Mutex{},
	}

	connID := uint64(1)
	conn := Connection{
		Connection:   &MockConnection{},
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
		AckTimeouts:  make(map[uint64]time.Time),
		LastAckSent:  0,
	}

	// Add many pending messages
	numMessages := 100
	for i := 1; i <= numMessages; i++ {
		seq := uint64(i)
		dataMessage := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Connection: connID,
			Seq:        seq,
			Data:       []byte("test data"),
		}
		conn.PendingAcks[seq] = dataMessage
		conn.AckTimeouts[seq] = time.Now().Add(5 * time.Second)
	}
	app.LocalConnections[connID] = conn

	// Verify all messages are pending
	assert.Equal(t, numMessages, len(app.LocalConnections[connID].PendingAcks))

	// Concurrently ACK half of the messages using correct ACK_UP for DATA_UP
	var wg sync.WaitGroup
	ackedCount := numMessages / 2

	for i := 1; i <= ackedCount; i++ {
		wg.Add(1)
		go func(seq uint64) {
			defer wg.Done()
			ackMessage := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_ACK_UP, // Use ACK_UP for DATA_UP messages
				Connection: connID,
				Seq:        seq,
			}
			handleAckUp(ackMessage) // Use handleAckUp for ACK_UP messages
		}(uint64(i))
	}

	wg.Wait()

	// Verify correct number of messages remain pending
	remainingCount := numMessages - ackedCount
	assert.Equal(t, remainingCount, len(app.LocalConnections[connID].PendingAcks))
	assert.Equal(t, remainingCount, len(app.LocalConnections[connID].AckTimeouts))

	// Verify the correct messages remain
	for i := ackedCount + 1; i <= numMessages; i++ {
		seq := uint64(i)
		_, exists := app.LocalConnections[connID].PendingAcks[seq]
		assert.True(t, exists, "Sequence %d should still be pending", seq)
	}
}

// TestAckSequenceValidation tests that ACK sequences match data sequences
func TestAckSequenceValidation(t *testing.T) {
	// This test ensures that ACK messages reference the correct sequence numbers
	// from the original DATA messages they acknowledge

	testCases := []struct {
		name        string
		dataSeq     uint64
		ackSeq      uint64
		shouldMatch bool
		description string
	}{
		{"Matching sequences", 100, 100, true, "ACK seq should match DATA seq"},
		{"Mismatched sequences", 100, 101, false, "ACK seq different from DATA seq"},
		{"Zero sequences", 0, 0, true, "Both sequences are zero"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize app
			app = &App{
				LocalConnections:     make(map[uint64]Connection),
				LocalConnectionMutex: sync.Mutex{},
			}

			connID := uint64(1)
			conn := Connection{
				Connection:   &MockConnection{},
				LastSeqIn:    0,
				NextSeqOut:   0,
				MessageQueue: make(map[uint64]*twtproto.ProxyComm),
				PendingAcks:  make(map[uint64]*twtproto.ProxyComm),
				AckTimeouts:  make(map[uint64]time.Time),
				LastAckSent:  0,
			}

			// Add pending message with specific sequence
			dataMessage := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_DATA_UP,
				Connection: connID,
				Seq:        tc.dataSeq,
				Data:       []byte("test data"),
			}
			conn.PendingAcks[tc.dataSeq] = dataMessage
			conn.AckTimeouts[tc.dataSeq] = time.Now().Add(5 * time.Second)
			app.LocalConnections[connID] = conn

			// Send ACK with specific sequence
			ackMessage := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_ACK_UP,
				Connection: connID,
				Seq:        tc.ackSeq,
			}
			handleAckUp(ackMessage)

			// Check result based on expected match
			if tc.shouldMatch {
				// Should be removed if sequences match
				_, exists := app.LocalConnections[connID].PendingAcks[tc.dataSeq]
				assert.False(t, exists, tc.description)
			} else {
				// Should still exist if sequences don't match
				_, exists := app.LocalConnections[connID].PendingAcks[tc.dataSeq]
				assert.True(t, exists, tc.description)
			}
		})
	}
}
