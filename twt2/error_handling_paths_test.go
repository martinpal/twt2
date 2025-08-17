package twt2

import (
	"net"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

// Mock connection for error handling testing
type errorTestConn struct {
	net.Conn
	closed bool
	data   []byte
}

func (m *errorTestConn) Read(b []byte) (n int, err error) {
	if len(m.data) == 0 {
		return 0, net.ErrClosed
	}
	n = copy(b, m.data)
	m.data = m.data[n:]
	return n, nil
}

func (m *errorTestConn) Write(b []byte) (n int, err error) {
	if m.closed {
		return 0, net.ErrClosed
	}
	return len(b), nil
}

func (m *errorTestConn) Close() error {
	m.closed = true
	return nil
}

func (m *errorTestConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8080}
}

func (m *errorTestConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9090}
}

func (m *errorTestConn) SetDeadline(t time.Time) error      { return nil }
func (m *errorTestConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *errorTestConn) SetWriteDeadline(t time.Time) error { return nil }

// Test comprehensive error handling paths
func TestErrorHandlingPaths(t *testing.T) {
	tests := []struct {
		name        string
		testFunc    func(t *testing.T)
		expectError bool
	}{
		{
			name: "Nil message handling in sendProtobuf",
			testFunc: func(t *testing.T) {
				// Test sendProtobuf with nil message
				defer func() {
					if r := recover(); r != nil {
						t.Logf("Recovered from panic (expected): %v", r)
					}
				}()
				// This should not panic due to nil check in sendProtobuf
				sendProtobuf(nil)
			},
			expectError: false,
		},
		{
			name: "Invalid protobuf message handling",
			testFunc: func(t *testing.T) {
				// Test with invalid message type enum
				message := &twtproto.ProxyComm{
					Mt: twtproto.ProxyComm_MessageType(999), // Invalid enum
				}
				// Should handle gracefully
				sendProtobuf(message)
			},
			expectError: false,
		},
		{
			name: "Large message handling",
			testFunc: func(t *testing.T) {
				// Test with very large data
				largeData := make([]byte, 1024*1024) // 1MB
				message := &twtproto.ProxyComm{
					Mt:   twtproto.ProxyComm_DATA_UP,
					Data: largeData,
				}
				sendProtobuf(message)
			},
			expectError: false,
		},
		{
			name: "Connection handling with closed connection",
			testFunc: func(t *testing.T) {
				conn := &errorTestConn{closed: true}
				// Should handle closed connection gracefully
				handleConnection(conn)
			},
			expectError: false,
		},
		{
			name: "Connection state cleanup",
			testFunc: func(t *testing.T) {
				// Should not panic or fail
				clearConnectionState()
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil && !tt.expectError {
					t.Errorf("Unexpected panic: %v", r)
				}
			}()
			tt.testFunc(t)
		})
	}
}

// Test error recovery mechanisms
func TestErrorRecoveryMechanisms(t *testing.T) {
	tests := []struct {
		name     string
		testFunc func(t *testing.T)
	}{
		{
			name: "Connection recovery after failure",
			testFunc: func(t *testing.T) {
				// Test multiple connection attempts
				for i := 0; i < 5; i++ {
					conn := &errorTestConn{
						data: []byte("test data"),
					}
					handleConnection(conn)
				}
			},
		},
		{
			name: "State cleanup after multiple operations",
			testFunc: func(t *testing.T) {
				// Perform multiple operations then cleanup
				messages := []*twtproto.ProxyComm{
					{Mt: twtproto.ProxyComm_PING},
					{Mt: twtproto.ProxyComm_DATA_UP, Data: []byte("test")},
					{Mt: twtproto.ProxyComm_DATA_DOWN, Data: []byte("response")},
				}

				for _, msg := range messages {
					sendProtobuf(msg)
				}

				clearConnectionState()
			},
		},
		{
			name: "Graceful degradation under load",
			testFunc: func(t *testing.T) {
				// Simulate high load
				done := make(chan bool, 10)

				for i := 0; i < 10; i++ {
					go func(id int) {
						defer func() { done <- true }()

						msg := &twtproto.ProxyComm{
							Mt:   twtproto.ProxyComm_DATA_UP,
							Seq:  uint64(id),
							Data: []byte("concurrent test"),
						}
						sendProtobuf(msg)
					}(i)
				}

				// Wait for all goroutines
				for i := 0; i < 10; i++ {
					<-done
				}

				clearConnectionState()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Unexpected panic in recovery test: %v", r)
				}
			}()
			tt.testFunc(t)
		})
	}
}

// Test critical path error handling
func TestCriticalPathErrorHandling(t *testing.T) {
	t.Run("Critical state cleanup", func(t *testing.T) {
		// Multiple cleanup calls should be safe
		for i := 0; i < 10; i++ {
			clearConnectionState()
		}
	})

	t.Run("Message type validation", func(t *testing.T) {
		// Test with all valid message types
		validMessages := []*twtproto.ProxyComm{
			{Mt: twtproto.ProxyComm_PING},
			{Mt: twtproto.ProxyComm_DATA_UP, Data: []byte("test")},
			{Mt: twtproto.ProxyComm_DATA_DOWN, Data: []byte("response")},
		}

		for _, msg := range validMessages {
			sendProtobuf(msg)
		}
	})

	t.Run("Resource limit handling", func(t *testing.T) {
		// Test resource limits by handling connections sequentially to avoid race conditions
		// This tests that the system can handle multiple connections without crashing
		connections := make([]*errorTestConn, 5) // Use fewer connections processed sequentially

		for i := 0; i < 5; i++ {
			connections[i] = &errorTestConn{
				data: []byte("test data"),
			}
			// Handle connections one at a time to avoid race conditions
			handleConnection(connections[i])
			// Clean up after each connection to test resource cleanup
			clearConnectionState()
		}

		// Final cleanup
		clearConnectionState()
	})

	t.Run("Timeout error handling", func(t *testing.T) {
		done := make(chan bool, 1)

		go func() {
			// Test operations complete in reasonable time
			msg := &twtproto.ProxyComm{
				Mt:   twtproto.ProxyComm_DATA_UP,
				Data: make([]byte, 1024), // 1KB message
			}
			sendProtobuf(msg)
			clearConnectionState()
			done <- true
		}()

		select {
		case <-done:
			// Operation completed successfully
		case <-time.After(5 * time.Second):
			t.Error("Critical path operations timed out")
		}
	})

	t.Run("Protobuf field validation", func(t *testing.T) {
		// Test with various protobuf field combinations
		testMessages := []*twtproto.ProxyComm{
			{Mt: twtproto.ProxyComm_PING, Seq: 1},
			{Mt: twtproto.ProxyComm_DATA_UP, Proxy: 12345, Seq: 2},
			{Mt: twtproto.ProxyComm_DATA_DOWN, Connection: 67890, Seq: 3},
			{Mt: twtproto.ProxyComm_DATA_UP, Address: "127.0.0.1", Port: 8080, Seq: 4},
		}

		for _, msg := range testMessages {
			sendProtobuf(msg)
		}
	})
}
