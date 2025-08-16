package twt2

import (
	"sync"
	"testing"

	"palecci.cz/twtproto"
)

// Test all message types to verify comprehensive tracking
func TestAllMessageTypesTracking(t *testing.T) {
	// Save original app
	originalApp := app
	defer func() { app = originalApp }()

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

	// Set up connections for message handling
	mockLocalConn := newMockConn()
	mockRemoteConn := newMockConn()

	app.LocalConnections[1] = Connection{
		Connection:   mockLocalConn,
		NextSeqOut:   1,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}

	app.RemoteConnections[1] = Connection{
		Connection:   mockRemoteConn,
		NextSeqOut:   1,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}

	// Test all message types
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
	}

	t.Logf("Testing tracking for %d message types:", len(messageTypes))

	for _, msgTest := range messageTypes {
		// Create message
		message := &twtproto.ProxyComm{
			Mt:         msgTest.msgType,
			Connection: 1,
			Seq:        1,
			Data:       msgTest.data,
			Address:    "127.0.0.1", // For OPEN_CONN - use localhost for fast failure
			Port:       9999,        // For OPEN_CONN - use a port that's likely closed for fast failure
		}

		// Get initial pool stats
		initialCounts, _, _, _, _ := poolConn.Stats.GetStats()
		initialCount := initialCounts[msgTest.msgType]

		// Get initial server stats
		serverStats.mutex.RLock()
		initialServerCount := serverStats.MessageCounts[msgTest.msgType]
		serverStats.mutex.RUnlock()

		// Process message with pool connection context (client side)
		handleProxycommMessageWithPoolConn(message, poolConn)

		// Get final pool stats
		finalCounts, _, finalBytesDown, _, _ := poolConn.Stats.GetStats()
		finalCount := finalCounts[msgTest.msgType]

		// Get final server stats
		serverStats.mutex.RLock()
		finalServerCount := serverStats.MessageCounts[msgTest.msgType]
		serverStats.mutex.RUnlock()

		// Verify pool connection tracking
		expectedPoolIncrease := uint64(1)
		actualPoolIncrease := finalCount - initialCount
		if actualPoolIncrease != expectedPoolIncrease {
			t.Errorf("Pool connection: %s message not tracked correctly. Expected +%d, got +%d",
				msgTest.msgType, expectedPoolIncrease, actualPoolIncrease)
		}

		// Verify server tracking
		expectedServerIncrease := uint64(1)
		actualServerIncrease := finalServerCount - initialServerCount
		if actualServerIncrease != expectedServerIncrease {
			t.Errorf("Server stats: %s message not tracked correctly. Expected +%d, got +%d",
				msgTest.msgType, expectedServerIncrease, actualServerIncrease)
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
