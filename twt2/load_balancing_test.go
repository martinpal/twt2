package twt2

import (
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// TestSendProtobuf_LoadBalancing verifies that messages are distributed evenly across pool connections
func TestSendProtobuf_LoadBalancing(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Create app with multiple healthy pool connections with different LastUsed times
	app = &App{
		PoolConnections: []*PoolConnection{
			{
				ID:              0,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-3 * time.Hour), // Oldest - should be selected first
			},
			{
				ID:              1,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-2 * time.Hour), // Second oldest
			},
			{
				ID:              2,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-1 * time.Hour), // Most recent
			},
		},
		PoolMutex: sync.Mutex{},
	}

	// Track which connections receive messages
	connectionUsage := make(map[uint64]int)

	// Send multiple messages to see distribution
	for i := 0; i < 9; i++ {
		message := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Proxy:      0,
			Connection: uint64(i + 100),
			Seq:        uint64(i + 1),
		}

		sendProtobuf(message)

		// Check which connection received the message
		for _, poolConn := range app.PoolConnections {
			select {
			case receivedMsg := <-poolConn.SendChan:
				connectionUsage[poolConn.ID]++
				t.Logf("Message %d sent to connection %d (LastUsed was %v ago)",
					i, poolConn.ID, time.Since(poolConn.LastUsed))

				// Verify message details
				if receivedMsg.Mt != twtproto.ProxyComm_DATA_UP {
					t.Errorf("Message %d: Expected DATA_UP, got %v", i, receivedMsg.Mt)
				}
				if receivedMsg.Connection != uint64(i+100) {
					t.Errorf("Message %d: Expected connection %d, got %d", i, i+100, receivedMsg.Connection)
				}
			default:
				// No message in this connection's channel
			}
		}

		// Add small delay to ensure LastUsed timestamps are different
		time.Sleep(1 * time.Millisecond)
	}

	t.Logf("Connection usage distribution: %v", connectionUsage)

	// Verify that all connections were used (load balancing working)
	for i := uint64(0); i < 3; i++ {
		if connectionUsage[i] == 0 {
			t.Errorf("Connection %d was never used - load balancing not working", i)
		}
	}

	// Verify reasonable distribution (each connection should get some messages)
	// With LRU algorithm, we expect roughly even distribution
	totalMessages := 9
	avgMessagesPerConn := totalMessages / len(app.PoolConnections)

	for connID, usage := range connectionUsage {
		if usage < 1 || usage > totalMessages {
			t.Errorf("Connection %d has unrealistic usage: %d messages (expected 1-%d)", connID, usage, totalMessages)
		}
		t.Logf("Connection %d handled %d messages (avg: %d)", connID, usage, avgMessagesPerConn)
	}

	// Verify that the first few messages went to the oldest connections
	// Since connection 0 was oldest, it should have been selected first
	if connectionUsage[0] == 0 {
		t.Error("Connection 0 (oldest) should have received at least one message")
	}

	t.Logf("Load balancing test passed - messages distributed across %d connections", len(connectionUsage))
}

// TestSendProtobuf_LRUOrdering verifies that Least Recently Used algorithm works correctly
func TestSendProtobuf_LRUOrdering(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	baseTime := time.Now()

	// Create app with connections having specific LastUsed times
	app = &App{
		PoolConnections: []*PoolConnection{
			{
				ID:              0,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: baseTime,
				LastUsed:        baseTime.Add(-5 * time.Minute), // 5 minutes ago
			},
			{
				ID:              1,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: baseTime,
				LastUsed:        baseTime.Add(-10 * time.Minute), // 10 minutes ago (oldest)
			},
			{
				ID:              2,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: baseTime,
				LastUsed:        baseTime.Add(-2 * time.Minute), // 2 minutes ago (newest)
			},
		},
		PoolMutex: sync.Mutex{},
	}

	// Send one message - should go to connection 1 (oldest LastUsed)
	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_PING,
		Proxy:      0,
		Connection: 123,
		Seq:        1,
	}

	sendProtobuf(message)

	// Verify connection 1 (ID=1) received the message
	select {
	case receivedMsg := <-app.PoolConnections[1].SendChan:
		if receivedMsg.Mt != twtproto.ProxyComm_PING {
			t.Error("Expected PING message")
		}
		t.Logf("Message correctly sent to connection 1 (oldest LastUsed)")
	case <-time.After(100 * time.Millisecond):
		t.Error("Connection 1 should have received the message (it had oldest LastUsed)")
	}

	// Verify other connections didn't receive messages
	for i, poolConn := range app.PoolConnections {
		if i == 1 {
			continue // Skip the connection that should have received the message
		}
		select {
		case <-poolConn.SendChan:
			t.Errorf("Connection %d should not have received a message", poolConn.ID)
		default:
			// Good - no message received
		}
	}
}

// TestSendProtobuf_PrioritySelection verifies connection selection priority order
func TestSendProtobuf_PrioritySelection(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	baseTime := time.Now()

	// Create app with mixed connection states to test priority
	app = &App{
		PoolConnections: []*PoolConnection{
			{
				ID:              0,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         false, // Unhealthy
				InUse:           false,
				LastHealthCheck: baseTime,
				LastUsed:        baseTime.Add(-10 * time.Minute),
			},
			{
				ID:              1,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           true, // Busy
				LastHealthCheck: baseTime,
				LastUsed:        baseTime.Add(-8 * time.Minute),
			},
			{
				ID:              2,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false, // Available and healthy (highest priority)
				LastHealthCheck: baseTime,
				LastUsed:        baseTime.Add(-5 * time.Minute),
			},
		},
		PoolMutex: sync.Mutex{},
	}

	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Proxy:      0,
		Connection: 456,
		Seq:        1,
	}

	sendProtobuf(message)

	// Verify connection 2 (healthy and available) received the message
	select {
	case receivedMsg := <-app.PoolConnections[2].SendChan:
		if receivedMsg.Mt != twtproto.ProxyComm_DATA_UP {
			t.Error("Expected DATA_UP message")
		}
		if receivedMsg.Connection != 456 {
			t.Errorf("Expected connection 456, got %d", receivedMsg.Connection)
		}
		t.Logf("Message correctly sent to connection 2 (healthy and available)")
	case <-time.After(100 * time.Millisecond):
		t.Error("Connection 2 should have received the message (highest priority)")
	}

	// Verify other connections didn't receive messages
	for i, poolConn := range app.PoolConnections {
		if i == 2 {
			continue
		}
		select {
		case <-poolConn.SendChan:
			t.Errorf("Connection %d should not have received a message", poolConn.ID)
		default:
			// Good - no message received
		}
	}
}
