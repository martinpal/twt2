package twt2

import (
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// Benchmark tests for TW2 performance characteristics
// These tests measure the performance of key operations under load

// BenchmarkSendProtobuf measures the throughput of sending protobuf messages
func BenchmarkSendProtobuf(b *testing.B) {
	// Save original app state
	originalApp := app
	defer func() { app = originalApp }()

	// Set up test app with multiple pool connections
	app = &App{
		PoolConnections: []*PoolConnection{
			{
				ID:              0,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 1000),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-1 * time.Hour),
				Stats:           NewPoolConnectionStats(),
			},
			{
				ID:              1,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 1000),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-2 * time.Hour),
				Stats:           NewPoolConnectionStats(),
			},
			{
				ID:              2,
				Conn:            newMockConn(),
				SendChan:        make(chan *twtproto.ProxyComm, 1000),
				Healthy:         true,
				InUse:           false,
				LastHealthCheck: time.Now(),
				LastUsed:        time.Now().Add(-3 * time.Hour),
				Stats:           NewPoolConnectionStats(),
			},
		},
		PoolMutex:             sync.Mutex{},
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Create test message
	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Proxy:      0,
		Connection: 1,
		Seq:        0,
		Data:       []byte("benchmark test data"),
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sendProtobuf(testMessage)
		}
	})
}

// BenchmarkConnectionPoolLookup measures pool connection selection performance
func BenchmarkConnectionPoolLookup(b *testing.B) {
	// Save original app state
	originalApp := app
	defer func() { app = originalApp }()

	// Create large pool for stress testing
	poolSize := 100
	poolConnections := make([]*PoolConnection, poolSize)

	for i := 0; i < poolSize; i++ {
		poolConnections[i] = &PoolConnection{
			ID:              uint64(i),
			Conn:            newMockConn(),
			SendChan:        make(chan *twtproto.ProxyComm, 100),
			Healthy:         true,
			InUse:           false,
			LastHealthCheck: time.Now(),
			LastUsed:        time.Now().Add(-time.Duration(i) * time.Minute),
			Stats:           NewPoolConnectionStats(),
		}
	}

	app = &App{
		PoolConnections:       poolConnections,
		PoolMutex:             sync.Mutex{},
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Proxy:      0,
		Connection: 1,
		Seq:        0,
		Data:       []byte("test"),
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sendProtobuf(testMessage)
		}
	})
}

// BenchmarkMessageQueueProcessing measures message queue throughput
func BenchmarkMessageQueueProcessing(b *testing.B) {
	// Save original app state
	originalApp := app
	defer func() { app = originalApp }()

	// Set up test app
	app = &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Create connection with large message queue
	conn := Connection{
		Connection:   newMockConn(),
		LastSeqIn:    0,
		NextSeqOut:   0,
		MessageQueue: make(map[uint64]*twtproto.ProxyComm),
	}

	// Add connection to app
	app.LocalConnections[1] = conn

	// Create test messages
	messages := make([]*twtproto.ProxyComm, b.N)
	for i := 0; i < b.N; i++ {
		messages[i] = &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Proxy:      0,
			Connection: 1,
			Seq:        uint64(i),
			Data:       []byte("benchmark message data"),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handleProxycommMessage(messages[i])
	}
}

// BenchmarkConcurrentConnectionAccess measures performance under concurrent access
func BenchmarkConcurrentConnectionAccess(b *testing.B) {
	// Save original app state
	originalApp := app
	defer func() { app = originalApp }()

	// Set up test app with connections
	app = &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Add multiple connections
	for i := uint64(0); i < 10; i++ {
		conn := Connection{
			Connection:   newMockConn(),
			LastSeqIn:    0,
			NextSeqOut:   0,
			MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		}
		app.LocalConnections[i] = conn
	}

	messages := make([]*twtproto.ProxyComm, 10)
	for i := 0; i < 10; i++ {
		messages[i] = &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Proxy:      0,
			Connection: uint64(i),
			Seq:        0,
			Data:       []byte("concurrent test data"),
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			handleProxycommMessage(messages[i%10])
			i++
		}
	})
}

// BenchmarkPoolConnectionStats measures stats collection performance
func BenchmarkPoolConnectionStats(b *testing.B) {
	stats := NewPoolConnectionStats()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stats.mutex.Lock()
			stats.MessageCounts[twtproto.ProxyComm_DATA_UP]++
			stats.BytesUp += 100
			stats.LastMessageAt = time.Now()
			stats.mutex.Unlock()
		}
	})
}

// BenchmarkMemoryAllocation measures memory allocation patterns
func BenchmarkMemoryAllocation(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate typical message creation
		msg := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Proxy:      0,
			Connection: uint64(i),
			Seq:        uint64(i),
			Data:       make([]byte, 1024), // 1KB data
		}
		_ = msg
	}
}

// BenchmarkLargeMessageThroughput measures performance with large messages
func BenchmarkLargeMessageThroughput(b *testing.B) {
	// Save original app state
	originalApp := app
	defer func() { app = originalApp }()

	// Set up test app
	app = &App{
		PoolConnections: []*PoolConnection{
			{
				ID:              0,
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
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Create large message (64KB)
	largeData := make([]byte, 64*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	testMessage := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Proxy:      0,
		Connection: 1,
		Seq:        0,
		Data:       largeData,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sendProtobuf(testMessage)
	}
}

// BenchmarkConnectionCreationParallel measures parallel connection creation performance
func BenchmarkConnectionCreationParallel(b *testing.B) {
	// Save original app state
	originalApp := app
	defer func() { app = originalApp }()

	// Set up minimal app
	app = &App{
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		connID := uint64(0)
		for pb.Next() {
			connID++
			conn := Connection{
				Connection:   newMockConn(),
				LastSeqIn:    0,
				NextSeqOut:   0,
				MessageQueue: make(map[uint64]*twtproto.ProxyComm),
				PendingAcks:  make(map[uint64]*PendingAck),
			}

			app.LocalConnectionMutex.Lock()
			app.LocalConnections[connID] = conn
			app.LocalConnectionMutex.Unlock()
		}
	})
}
