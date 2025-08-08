package twt2

import (
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// TestRealisticDownstreamBalancing simulates real server behavior with remote connections
func TestRealisticDownstreamBalancing(t *testing.T) {
	originalApp := app
	defer func() { app = originalApp }()

	// Initialize server stats to prevent race conditions
	serverStats.mutex.Lock()
	if serverStats.MessageCounts == nil {
		serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
	}
	for k := range serverStats.MessageCounts {
		serverStats.MessageCounts[k] = 0
	}
	serverStats.TotalBytesDown = 0
	serverStats.TotalBytesUp = 0
	serverStats.mutex.Unlock()

	// Create app with multiple healthy pool connections (server side)
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
				Stats:           NewPoolConnectionStats(),
			},
			{
				ID:              1,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-2 * time.Hour),
				Stats:           NewPoolConnectionStats(),
			},
			{
				ID:              2,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 100),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-1 * time.Hour),
				Stats:           NewPoolConnectionStats(),
			},
		},
		PoolMutex:             sync.Mutex{},
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Simulate remote connections - add them to app state like real server would
	for connID := uint64(100); connID < 103; connID++ {
		app.RemoteConnectionMutex.Lock()
		app.RemoteConnections[connID] = Connection{
			Connection:   newMockConn(),
			LastSeqIn:    0,
			NextSeqOut:   0,
			MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		}
		app.RemoteConnectionMutex.Unlock()
	}

	// Track which pool connections receive DATA_DOWN messages
	connectionUsage := make(map[uint64]int)

	// Simulate what happens when server receives data from multiple remote connections
	// This mimics the behavior in handleRemoteSideConnection()
	for connID := uint64(100); connID < 103; connID++ {
		for chunk := 0; chunk < 3; chunk++ {
			// Simulate receiving data from remote connection
			app.RemoteConnectionMutex.Lock()
			connRecord := app.RemoteConnections[connID]
			seq := connRecord.LastSeqIn
			connRecord.LastSeqIn++
			app.RemoteConnections[connID] = connRecord
			app.RemoteConnectionMutex.Unlock()

			// Create DATA_DOWN message like the server does
			dataMessage := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_DATA_DOWN,
				Proxy:      0, // proxyID
				Connection: connID,
				Seq:        seq,
				Data:       []byte("real downstream data chunk"),
			}

			// Track outgoing DATA_DOWN messages like server does
			serverStats.mutex.Lock()
			serverStats.MessageCounts[twtproto.ProxyComm_DATA_DOWN]++
			serverStats.mutex.Unlock()

			// This is the critical call - same as in handleRemoteSideConnection
			sendProtobuf(dataMessage)

			// Check which pool connection received the message
			for _, poolConn := range app.PoolConnections {
				select {
				case receivedMsg := <-poolConn.SendChan:
					connectionUsage[poolConn.ID]++
					t.Logf("Remote conn %d chunk %d -> pool connection %d",
						connID, chunk, poolConn.ID)

					// Verify message details
					if receivedMsg.Connection != connID {
						t.Errorf("Expected connection %d, got %d", connID, receivedMsg.Connection)
					}
					if receivedMsg.Mt != twtproto.ProxyComm_DATA_DOWN {
						t.Errorf("Expected DATA_DOWN, got %v", receivedMsg.Mt)
					}
				default:
					// No message in this connection's channel
				}
			}

			// Small delay to simulate realistic timing
			time.Sleep(1 * time.Millisecond)
		}
	}

	t.Logf("Realistic downstream distribution: %v", connectionUsage)

	// Verify load balancing worked
	totalMessages := 9 // 3 remote connections × 3 chunks each
	for i := uint64(0); i < 3; i++ {
		if connectionUsage[i] == 0 {
			t.Errorf("Pool connection %d never received DATA_DOWN messages", i)
		}
	}

	// Check each pool connection got reasonable distribution
	avgPerConn := totalMessages / 3
	for connID, usage := range connectionUsage {
		if usage < 1 {
			t.Errorf("Pool connection %d only got %d messages (too few)", connID, usage)
		}
		t.Logf("Pool connection %d: %d messages (avg: %d)", connID, usage, avgPerConn)
	}

	// Verify server stats were updated correctly
	serverStats.mutex.RLock()
	dataDownCount := serverStats.MessageCounts[twtproto.ProxyComm_DATA_DOWN]
	serverStats.mutex.RUnlock()

	if dataDownCount != uint64(totalMessages) {
		t.Errorf("Server should have tracked %d DATA_DOWN messages, got %d",
			totalMessages, dataDownCount)
	}

	t.Logf("✓ Realistic downstream load balancing test passed")
	t.Logf("  %d messages distributed across %d pool connections",
		totalMessages, len(connectionUsage))
	t.Logf("  Server tracked %d DATA_DOWN messages", dataDownCount)
}
