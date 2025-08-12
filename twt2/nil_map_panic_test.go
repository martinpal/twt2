package twt2

import (
	"net/http"
	"sync"
	"testing"

	"palecci.cz/twtproto"
)

// TestNilMapPanicFix tests the specific bug fix for "assignment to entry in nil map" panic
// that occurred when trying to queue out-of-order messages to connection MessageQueues.
//
// The original bug was in handleProxycommMessageWithPoolConn() where the code did:
//
//	thisConnection = app.RemoteConnections[message.Connection]  // Gets a COPY of the struct
//	thisConnection.MessageQueue[message.Seq] = message         // Modifies the COPY's map
//
// This caused a panic because:
//  1. Go maps in structs are reference types, but when you copy a struct, you get a new struct
//     with the same map reference
//  2. However, when the map is nil or in certain concurrent conditions, modifying the copy's
//     map field can result in "assignment to entry in nil map" panic
//
// The fix was to directly modify the map in the original struct:
//
//	app.RemoteConnections[message.Connection].MessageQueue[message.Seq] = message
//
// This test validates that:
// 1. Out-of-order messages can be queued without panicking
// 2. The messages are actually stored in the correct connection's MessageQueue
// 3. Special cases like CLOSE_CONN_S (which processes immediately) work correctly
// 4. Both LocalConnections and RemoteConnections are handled properly
func TestNilMapPanicFix(t *testing.T) {
	// Initialize app in client mode (which uses RemoteConnections)
	mockHandler := func(w http.ResponseWriter, r *http.Request) {}
	app = NewApp(mockHandler, 8080, "localhost", 9090, 0, 5, false, true, "", "", 22, false, "", "", "")
	defer func() {
		app = nil
	}()

	tests := []struct {
		name            string
		messageType     twtproto.ProxyComm_MessageType
		connectionType  string
		setupConnection func(connID uint64)
	}{
		{
			name:           "RemoteConnection_DATA_UP_OutOfOrder",
			messageType:    twtproto.ProxyComm_DATA_UP,
			connectionType: "remote",
			setupConnection: func(connID uint64) {
				// Pre-create connection in RemoteConnections with initialized MessageQueue
				app.RemoteConnections[connID] = Connection{
					Connection:   nil,
					LastSeqIn:    0,
					NextSeqOut:   1, // Expect sequence 1, but we'll send 2
					MessageQueue: make(map[uint64]*twtproto.ProxyComm),
				}
			},
		},
		{
			name:           "RemoteConnection_CLOSE_CONN_S_OutOfOrder",
			messageType:    twtproto.ProxyComm_CLOSE_CONN_S,
			connectionType: "remote",
			setupConnection: func(connID uint64) {
				app.RemoteConnections[connID] = Connection{
					Connection:   nil,
					LastSeqIn:    0,
					NextSeqOut:   1,
					MessageQueue: make(map[uint64]*twtproto.ProxyComm),
				}
			},
		},
		{
			name:           "LocalConnection_DATA_DOWN_OutOfOrder",
			messageType:    twtproto.ProxyComm_DATA_DOWN,
			connectionType: "local",
			setupConnection: func(connID uint64) {
				// Pre-create connection in LocalConnections with initialized MessageQueue
				app.LocalConnections[connID] = Connection{
					Connection:   nil,
					LastSeqIn:    0,
					NextSeqOut:   1,
					MessageQueue: make(map[uint64]*twtproto.ProxyComm),
				}
			},
		},
		{
			name:           "LocalConnection_CLOSE_CONN_C_OutOfOrder",
			messageType:    twtproto.ProxyComm_CLOSE_CONN_C,
			connectionType: "local",
			setupConnection: func(connID uint64) {
				app.LocalConnections[connID] = Connection{
					Connection:   nil,
					LastSeqIn:    0,
					NextSeqOut:   1,
					MessageQueue: make(map[uint64]*twtproto.ProxyComm),
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connID := uint64(42)
			outOfOrderSeq := uint64(2) // Send seq 2 when expecting seq 1

			// Setup connection with initialized MessageQueue
			tt.setupConnection(connID)

			// Create out-of-order message that should be queued
			message := &twtproto.ProxyComm{
				Mt:         tt.messageType,
				Connection: connID,
				Seq:        outOfOrderSeq,
				Data:       []byte("test data"),
			}

			// This should not panic - the bug was that it would panic with
			// "assignment to entry in nil map" when trying to queue the message
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("handleProxycommMessageWithPoolConn panicked: %v", r)
				}
			}()

			// Call the function that had the bug
			handleProxycommMessageWithPoolConn(message, nil)

			// Special handling for CLOSE_CONN_S - it processes immediately and deletes the connection
			if tt.messageType == twtproto.ProxyComm_CLOSE_CONN_S {
				// For CLOSE_CONN_S, the connection should be deleted (this is expected behavior)
				var exists bool
				if tt.connectionType == "remote" {
					_, exists = app.RemoteConnections[connID]
				} else {
					_, exists = app.LocalConnections[connID]
				}

				if exists {
					t.Errorf("Expected CLOSE_CONN_S to delete connection %d, but it still exists", connID)
				}
				return // No need to check message queuing for close messages
			}

			// Verify the message was properly queued (for non-close messages)
			var conn Connection
			var ok bool

			if tt.connectionType == "remote" {
				conn, ok = app.RemoteConnections[connID]
			} else {
				conn, ok = app.LocalConnections[connID]
			}

			if !ok {
				t.Errorf("Connection %d not found after processing", connID)
				return
			}

			// Check that the out-of-order message was queued
			queuedMessage, exists := conn.MessageQueue[outOfOrderSeq]
			if !exists {
				t.Errorf("Expected message with seq %d to be queued, but it wasn't", outOfOrderSeq)
				return
			}

			if queuedMessage.Mt != tt.messageType {
				t.Errorf("Expected queued message type %v, got %v", tt.messageType, queuedMessage.Mt)
			}

			if queuedMessage.Connection != connID {
				t.Errorf("Expected queued message connection %d, got %d", connID, queuedMessage.Connection)
			}

			if queuedMessage.Seq != outOfOrderSeq {
				t.Errorf("Expected queued message seq %d, got %d", outOfOrderSeq, queuedMessage.Seq)
			}
		})
	}
}

// TestNilMapPanicFixConcurrent tests the bug fix under concurrent conditions
// to ensure the fix works even with multiple goroutines accessing the maps
func TestNilMapPanicFixConcurrent(t *testing.T) {
	mockHandler := func(w http.ResponseWriter, r *http.Request) {}
	app = NewApp(mockHandler, 8080, "localhost", 9090, 0, 5, false, true, "", "", 22, false, "", "", "")
	defer func() {
		app = nil
	}()

	const numGoroutines = 10
	const numConnections = 5
	var wg sync.WaitGroup

	// Pre-create connections with initialized MessageQueues
	for i := uint64(1); i <= numConnections; i++ {
		app.RemoteConnections[i] = Connection{
			Connection:   nil,
			LastSeqIn:    0,
			NextSeqOut:   1,
			MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		}
		app.LocalConnections[i] = Connection{
			Connection:   nil,
			LastSeqIn:    0,
			NextSeqOut:   1,
			MessageQueue: make(map[uint64]*twtproto.ProxyComm),
		}
	}

	// Launch multiple goroutines that send out-of-order messages
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Goroutine %d panicked: %v", goroutineID, r)
				}
			}()

			for connID := uint64(1); connID <= numConnections; connID++ {
				// Send out-of-order DATA_UP message
				message := &twtproto.ProxyComm{
					Mt:         twtproto.ProxyComm_DATA_UP,
					Connection: connID,
					Seq:        uint64(goroutineID + 2), // Out of order sequence
					Data:       []byte("test data"),
				}
				handleProxycommMessageWithPoolConn(message, nil)

				// Send out-of-order DATA_DOWN message
				message = &twtproto.ProxyComm{
					Mt:         twtproto.ProxyComm_DATA_DOWN,
					Connection: connID,
					Seq:        uint64(goroutineID + 2),
					Data:       []byte("test data"),
				}
				handleProxycommMessageWithPoolConn(message, nil)
			}
		}(i)
	}

	wg.Wait()

	// Verify that messages were queued properly
	for connID := uint64(1); connID <= numConnections; connID++ {
		remoteConn, ok := app.RemoteConnections[connID]
		if !ok {
			t.Errorf("Remote connection %d not found", connID)
			continue
		}

		localConn, ok := app.LocalConnections[connID]
		if !ok {
			t.Errorf("Local connection %d not found", connID)
			continue
		}

		// Each goroutine should have queued one message per connection
		if len(remoteConn.MessageQueue) != numGoroutines {
			t.Errorf("Expected %d queued messages in remote connection %d, got %d",
				numGoroutines, connID, len(remoteConn.MessageQueue))
		}

		if len(localConn.MessageQueue) != numGoroutines {
			t.Errorf("Expected %d queued messages in local connection %d, got %d",
				numGoroutines, connID, len(localConn.MessageQueue))
		}
	}
}

// TestNilMapPanicFixNewConnection tests the case where a connection doesn't exist yet
// and needs to be created (which was working correctly in the original code)
func TestNilMapPanicFixNewConnection(t *testing.T) {
	mockHandler := func(w http.ResponseWriter, r *http.Request) {}
	app = NewApp(mockHandler, 8080, "localhost", 9090, 0, 5, false, true, "", "", 22, false, "", "", "")
	defer func() {
		app = nil
	}()

	connID := uint64(99)
	outOfOrderSeq := uint64(5)

	// Create message for non-existent connection (should create new connection)
	message := &twtproto.ProxyComm{
		Mt:         twtproto.ProxyComm_DATA_UP,
		Connection: connID,
		Seq:        outOfOrderSeq,
		Data:       []byte("test data"),
	}

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("handleProxycommMessageWithPoolConn panicked: %v", r)
		}
	}()

	handleProxycommMessageWithPoolConn(message, nil)

	// Verify the connection was created and message was queued
	conn, ok := app.RemoteConnections[connID]
	if !ok {
		t.Errorf("Expected new connection %d to be created", connID)
		return
	}

	if conn.MessageQueue == nil {
		t.Errorf("Expected MessageQueue to be initialized for new connection")
		return
	}

	queuedMessage, exists := conn.MessageQueue[outOfOrderSeq]
	if !exists {
		t.Errorf("Expected message with seq %d to be queued in new connection", outOfOrderSeq)
		return
	}

	if queuedMessage.Connection != connID {
		t.Errorf("Expected queued message connection %d, got %d", connID, queuedMessage.Connection)
	}
}
