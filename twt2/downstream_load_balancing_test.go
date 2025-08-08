package twt2

import (
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// TestDownstreamLoadBalancing verifies that DATA_DOWN messages are distributed across pool connections
func TestDownstreamLoadBalancing(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Create app with multiple healthy pool connections
	app = &App{
		PoolConnections: []*PoolConnection{
			{
				ID:              0,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-3 * time.Hour), // Oldest
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

	// Track which connections receive DATA_DOWN messages
	connectionUsage := make(map[uint64]int)

	// Simulate multiple DATA_DOWN messages from the same remote connection
	// This simulates what happens when server receives data from a remote connection
	for i := 0; i < 9; i++ {
		dataDownMessage := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Proxy:      0,
			Connection: 100, // Same connection ID - this is the key test
			Seq:        uint64(i),
			Data:       []byte("downstream data chunk"),
		}

		// This is what the server does when it receives data from remote connection
		sendProtobuf(dataDownMessage)

		// Check which pool connection received the message
		for _, poolConn := range app.PoolConnections {
			select {
			case receivedMsg := <-poolConn.SendChan:
				connectionUsage[poolConn.ID]++
				t.Logf("DATA_DOWN message %d sent to pool connection %d", i, poolConn.ID)

				// Verify message details
				if receivedMsg.Mt != twtproto.ProxyComm_DATA_DOWN {
					t.Errorf("Message %d: Expected DATA_DOWN, got %v", i, receivedMsg.Mt)
				}
				if receivedMsg.Connection != 100 {
					t.Errorf("Message %d: Expected connection 100, got %d", i, receivedMsg.Connection)
				}
			default:
				// No message in this connection's channel
			}
		}

		// Add small delay to ensure LastUsed timestamps are different
		time.Sleep(1 * time.Millisecond)
	}

	t.Logf("Downstream connection usage distribution: %v", connectionUsage)

	// Verify that all pool connections were used for DATA_DOWN messages
	for i := uint64(0); i < 3; i++ {
		if connectionUsage[i] == 0 {
			t.Errorf("Pool connection %d was never used for DATA_DOWN - downstream load balancing not working", i)
		}
	}

	// Verify reasonably even distribution
	totalMessages := 9
	expectedMinPerConn := 2 // At least 2 messages per connection with 9 messages across 3 connections

	for connID, usage := range connectionUsage {
		if usage < expectedMinPerConn {
			t.Errorf("Pool connection %d only handled %d DATA_DOWN messages (expected at least %d)",
				connID, usage, expectedMinPerConn)
		}
		t.Logf("Pool connection %d handled %d DATA_DOWN messages", connID, usage)
	}

	t.Logf("Processed %d total DATA_DOWN messages", totalMessages)

	t.Logf("Downstream load balancing test passed - DATA_DOWN messages distributed across %d pool connections",
		len(connectionUsage))
}

// TestMultipleRemoteConnectionsDownstream tests downstream balancing across multiple remote connections
func TestMultipleRemoteConnectionsDownstream(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Create app with multiple healthy pool connections
	app = &App{
		PoolConnections: []*PoolConnection{
			{
				ID:              0,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-3 * time.Hour),
			},
			{
				ID:              1,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-2 * time.Hour),
			},
			{
				ID:              2,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-1 * time.Hour),
			},
		},
		PoolMutex: sync.Mutex{},
	}

	// Track which connections receive DATA_DOWN messages
	connectionUsage := make(map[uint64]int)

	// Simulate DATA_DOWN messages from 3 different remote connections
	for remoteConnID := uint64(100); remoteConnID < 103; remoteConnID++ {
		for i := 0; i < 3; i++ {
			dataDownMessage := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_DATA_DOWN,
				Proxy:      0,
				Connection: remoteConnID,
				Seq:        uint64(i),
				Data:       []byte("downstream data"),
			}

			sendProtobuf(dataDownMessage)

			// Check which pool connection received the message
			for _, poolConn := range app.PoolConnections {
				select {
				case receivedMsg := <-poolConn.SendChan:
					connectionUsage[poolConn.ID]++
					t.Logf("DATA_DOWN from remote %d message %d sent to pool connection %d",
						remoteConnID, i, poolConn.ID)

					if receivedMsg.Connection != remoteConnID {
						t.Errorf("Expected connection %d, got %d", remoteConnID, receivedMsg.Connection)
					}
				default:
					// No message in this connection's channel
				}
			}

			time.Sleep(1 * time.Millisecond)
		}
	}

	t.Logf("Multi-remote downstream usage distribution: %v", connectionUsage)

	// With proper load balancing, all pool connections should be used
	for i := uint64(0); i < 3; i++ {
		if connectionUsage[i] == 0 {
			t.Errorf("Pool connection %d was never used - multi-remote downstream load balancing failed", i)
		}
	}

	// Each pool connection should handle multiple messages
	totalMessages := 9 // 3 remote connections × 3 messages each
	avgPerConn := totalMessages / 3

	for connID, usage := range connectionUsage {
		t.Logf("Pool connection %d handled %d DATA_DOWN messages (avg: %d)", connID, usage, avgPerConn)
	}

	t.Logf("Multi-remote downstream load balancing completed - %d total messages distributed", totalMessages)
}
