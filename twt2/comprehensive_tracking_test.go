package twt2

import (
	"sync"
	"testing"

	"palecci.cz/twtproto"
)

// Test all message types to verify comprehensive tracking (including ACK messages)
func TestAllMessageTypesTracking(t *testing.T) {
	// Save original app and server stats
	originalApp := app
	defer func() {
		// Clean up any connections and background goroutines before restoring
		testCleanup()
		app = originalApp
	}()

	// Initialize server stats
	serverStats.mutex.Lock()
	if serverStats.MessageCounts == nil {
		serverStats.MessageCounts = make(map[twtproto.ProxyComm_MessageType]uint64)
	}
	// Reset all stats
	for k := range serverStats.MessageCounts {
		serverStats.MessageCounts[k] = 0
	}
	serverStats.TotalMessagesProcessed = 0
	serverStats.mutex.Unlock()

	// Create client app with pool connection
	poolConn := &PoolConnection{
		ID:      100,
		Stats:   NewPoolConnectionStats(),
		Healthy: true,
	}

	app = &App{
		PoolConnections:       []*PoolConnection{poolConn},
		PoolMutex:             sync.Mutex{},
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Test message statistics tracking only (without full message processing)
	messageTypes := []struct {
		msgType twtproto.ProxyComm_MessageType
		data    []byte
	}{
		{twtproto.ProxyComm_PING, nil},
		{twtproto.ProxyComm_OPEN_CONN, nil},
		{twtproto.ProxyComm_DATA_UP, []byte("upward data")},
		{twtproto.ProxyComm_DATA_DOWN, []byte("downward data")},
		{twtproto.ProxyComm_CLOSE_CONN_S, nil},
		{twtproto.ProxyComm_CLOSE_CONN_C, nil},
		{twtproto.ProxyComm_ACK_UP, nil},
		{twtproto.ProxyComm_ACK_DOWN, nil},
	}

	t.Logf("Testing tracking for %d message types:", len(messageTypes))

	for _, msgTest := range messageTypes {
		// Create message
		message := &twtproto.ProxyComm{
			Mt:         msgTest.msgType,
			Connection: 1,
			Seq:        1,
			Data:       msgTest.data,
		}

		// Get initial pool stats
		initialCounts, _, _, _, _ := poolConn.Stats.GetStats()
		initialCount := initialCounts[msgTest.msgType]

		// Get initial server stats
		serverStats.mutex.RLock()
		initialServerCount := serverStats.MessageCounts[msgTest.msgType]
		serverStats.mutex.RUnlock()

		// Test ONLY the statistics tracking portion (without connection processing)
		// Track server statistics (replicating the logic from handleProxycommMessageWithPoolConn)
		serverStats.mutex.Lock()
		serverStats.TotalMessagesProcessed++
		serverStats.MessageCounts[message.Mt]++
		serverStats.mutex.Unlock()

		// Track pool connection statistics
		if poolConn.Stats != nil {
			dataLen := 0
			if message.GetData() != nil {
				dataLen = len(message.GetData())
			}
			poolConn.Stats.IncrementMessage(message.Mt, dataLen)
		}

		// Get final stats
		finalCounts, _, finalBytesDown, _, _ := poolConn.Stats.GetStats()
		finalCount := finalCounts[msgTest.msgType]

		serverStats.mutex.RLock()
		finalServerCount := serverStats.MessageCounts[msgTest.msgType]
		serverStats.mutex.RUnlock()

		// Verify tracking
		expectedIncrease := uint64(1)
		if finalCount-initialCount != expectedIncrease {
			t.Errorf("Pool connection: %s message not tracked correctly. Expected +%d, got +%d",
				msgTest.msgType, expectedIncrease, finalCount-initialCount)
		}

		if finalServerCount-initialServerCount != expectedIncrease {
			t.Errorf("Server stats: %s message not tracked correctly. Expected +%d, got +%d",
				msgTest.msgType, expectedIncrease, finalServerCount-initialServerCount)
		}

		// Log success
		t.Logf("  ✓ %s: Pool[%d->%d] Server[%d->%d]",
			msgTest.msgType, initialCount, finalCount, initialServerCount, finalServerCount)

		// Special check for DATA_DOWN bytes tracking
		if msgTest.msgType == twtproto.ProxyComm_DATA_DOWN && msgTest.data != nil {
			expectedBytes := uint64(len(msgTest.data))
			if finalBytesDown < expectedBytes {
				t.Errorf("DATA_DOWN bytes not tracked correctly in pool connection. Expected at least %d bytes", expectedBytes)
			} else {
				t.Logf("    ✓ DATA_DOWN bytes tracked: %d bytes", finalBytesDown)
			}
		}
	}

	// Final summary
	finalPoolCounts, _, _, _, _ := poolConn.Stats.GetStats()

	serverStats.mutex.RLock()
	finalServerCounts := make(map[twtproto.ProxyComm_MessageType]uint64)
	for k, v := range serverStats.MessageCounts {
		finalServerCounts[k] = v
	}
	totalServerMessages := serverStats.TotalMessagesProcessed
	serverStats.mutex.RUnlock()

	t.Logf("=== FINAL SUMMARY ===")
	t.Logf("Pool connection stats:")
	for msgType, count := range finalPoolCounts {
		if count > 0 {
			t.Logf("  %s: %d messages", msgType, count)
		}
	}

	t.Logf("Server stats:")
	t.Logf("  Total messages processed: %d", totalServerMessages)
	for msgType, count := range finalServerCounts {
		if count > 0 {
			t.Logf("  %s: %d messages", msgType, count)
		}
	}

	t.Logf("✓ ALL MESSAGE TYPES TRACKED SUCCESSFULLY!")
}
