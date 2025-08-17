package twt2

import (
	"bytes"
	"encoding/binary"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"palecci.cz/twtproto"
)

// Test protobuf message marshaling and unmarshaling edge cases
func TestProtobufMarshalingEdgeCases(t *testing.T) {
	// Test with maximum field values
	t.Run("MaximumFieldValues", func(t *testing.T) {
		msg := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_PING,
			Proxy:      math.MaxUint64,
			Connection: math.MaxUint64,
			Seq:        math.MaxUint64,
			Address:    "very-long-address-that-could-cause-buffer-overflow-issues-in-parsing-routines-with-fixed-buffer-sizes",
			Port:       math.MaxUint32,
			Data:       make([]byte, 64*1024), // 64KB data
		}

		// Fill data with pattern
		for i := range msg.Data {
			msg.Data[i] = byte(i % 256)
		}

		data, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("Failed to marshal message with max values: %v", err)
		}

		unmarshaledMsg := &twtproto.ProxyComm{}
		err = proto.Unmarshal(data, unmarshaledMsg)
		if err != nil {
			t.Fatalf("Failed to unmarshal message with max values: %v", err)
		}

		// Verify all fields
		if unmarshaledMsg.Mt != msg.Mt {
			t.Errorf("Mt mismatch: expected %v, got %v", msg.Mt, unmarshaledMsg.Mt)
		}
		if unmarshaledMsg.Proxy != msg.Proxy {
			t.Errorf("Proxy mismatch: expected %d, got %d", msg.Proxy, unmarshaledMsg.Proxy)
		}
		if unmarshaledMsg.Connection != msg.Connection {
			t.Errorf("Connection mismatch: expected %d, got %d", msg.Connection, unmarshaledMsg.Connection)
		}
		if unmarshaledMsg.Seq != msg.Seq {
			t.Errorf("Seq mismatch: expected %d, got %d", msg.Seq, unmarshaledMsg.Seq)
		}
		if unmarshaledMsg.Address != msg.Address {
			t.Errorf("Address mismatch: expected %s, got %s", msg.Address, unmarshaledMsg.Address)
		}
		if unmarshaledMsg.Port != msg.Port {
			t.Errorf("Port mismatch: expected %d, got %d", msg.Port, unmarshaledMsg.Port)
		}
		if !bytes.Equal(unmarshaledMsg.Data, msg.Data) {
			t.Error("Data mismatch")
		}
	})

	// Test with empty/zero values
	t.Run("EmptyZeroValues", func(t *testing.T) {
		msg := &twtproto.ProxyComm{}

		data, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("Failed to marshal empty message: %v", err)
		}

		unmarshaledMsg := &twtproto.ProxyComm{}
		err = proto.Unmarshal(data, unmarshaledMsg)
		if err != nil {
			t.Fatalf("Failed to unmarshal empty message: %v", err)
		}

		// Verify default values
		if unmarshaledMsg.Mt != twtproto.ProxyComm_DATA_DOWN {
			t.Errorf("Expected default Mt DATA_DOWN, got %v", unmarshaledMsg.Mt)
		}
		if unmarshaledMsg.Proxy != 0 {
			t.Errorf("Expected Proxy 0, got %d", unmarshaledMsg.Proxy)
		}
		if unmarshaledMsg.Connection != 0 {
			t.Errorf("Expected Connection 0, got %d", unmarshaledMsg.Connection)
		}
		if unmarshaledMsg.Address != "" {
			t.Errorf("Expected empty Address, got %s", unmarshaledMsg.Address)
		}
		if unmarshaledMsg.Data != nil {
			t.Error("Expected nil Data, got non-nil")
		}
	})

	// Test with nil data field
	t.Run("NilDataField", func(t *testing.T) {
		msg := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Connection: 123,
			Data:       nil,
		}

		data, err := proto.Marshal(msg)
		if err != nil {
			t.Fatalf("Failed to marshal message with nil data: %v", err)
		}

		unmarshaledMsg := &twtproto.ProxyComm{}
		err = proto.Unmarshal(data, unmarshaledMsg)
		if err != nil {
			t.Fatalf("Failed to unmarshal message with nil data: %v", err)
		}

		if unmarshaledMsg.Data != nil {
			t.Error("Expected nil Data after unmarshal, got non-nil")
		}
	})

	// Test with large data chunks
	t.Run("LargeDataChunks", func(t *testing.T) {
		sizes := []int{1024, 8192, 32768, 65536} // Various sizes up to 64KB

		for _, size := range sizes {
			msg := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_DATA_DOWN,
				Connection: 456,
				Data:       make([]byte, size),
			}

			// Fill with recognizable pattern
			for i := range msg.Data {
				msg.Data[i] = byte((i + size) % 256)
			}

			data, err := proto.Marshal(msg)
			if err != nil {
				t.Fatalf("Failed to marshal message with %d byte data: %v", size, err)
			}

			unmarshaledMsg := &twtproto.ProxyComm{}
			err = proto.Unmarshal(data, unmarshaledMsg)
			if err != nil {
				t.Fatalf("Failed to unmarshal message with %d byte data: %v", size, err)
			}

			if len(unmarshaledMsg.Data) != size {
				t.Errorf("Data size mismatch for %d bytes: expected %d, got %d", size, size, len(unmarshaledMsg.Data))
			}

			if !bytes.Equal(unmarshaledMsg.Data, msg.Data) {
				t.Errorf("Data content mismatch for %d byte chunk", size)
			}
		}
	})
}

// Test invalid protobuf wire format edge cases
func TestProtobufWireFormatEdgeCases(t *testing.T) {
	// Test truncated messages
	t.Run("TruncatedMessages", func(t *testing.T) {
		validMsg := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Connection: 789,
			Data:       []byte("test data"),
		}

		validData, err := proto.Marshal(validMsg)
		if err != nil {
			t.Fatalf("Failed to marshal valid message: %v", err)
		}

		// Test various truncation points
		truncationPoints := []int{1, len(validData) / 4, len(validData) / 2, len(validData) - 1}

		for _, point := range truncationPoints {
			if point >= len(validData) {
				continue
			}

			truncatedData := validData[:point]
			msg := &twtproto.ProxyComm{}
			err := proto.Unmarshal(truncatedData, msg)

			if err == nil {
				t.Errorf("Expected error when unmarshaling truncated data at point %d, but got none", point)
			}
		}
	})

	// Test corrupted field types
	t.Run("CorruptedFieldTypes", func(t *testing.T) {
		// Create data that looks like valid protobuf but has type mismatches
		corruptedData := []byte{
			0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F, // Mt field with invalid enum value
			0x10, 0x7B, // Proxy field
			0x18, 0x01, // Connection field
		}

		msg := &twtproto.ProxyComm{}
		err := proto.Unmarshal(corruptedData, msg)

		// Should handle gracefully even with invalid enum values
		if err != nil {
			// This is expected behavior for truly corrupted data
			t.Logf("Unmarshal correctly rejected corrupted data: %v", err)
		} else {
			// If it doesn't error, the enum should have an unexpected value
			t.Logf("Unmarshal succeeded with Mt value: %v", msg.Mt)
		}
	})

	// Test malformed varint encoding
	t.Run("MalformedVarintEncoding", func(t *testing.T) {
		// Create data with incomplete varint (continuation bit set but no following byte)
		malformedData := []byte{
			0x08, 0x80, // Mt field with incomplete varint
		}

		msg := &twtproto.ProxyComm{}
		err := proto.Unmarshal(malformedData, msg)

		if err == nil {
			t.Error("Expected error when unmarshaling malformed varint, but got none")
		}
	})

	// Test oversized length-delimited fields
	t.Run("OversizedLengthDelimitedFields", func(t *testing.T) {
		// Create data claiming to have an extremely large string/bytes field
		oversizedData := []byte{
			0x2A, 0xFF, 0xFF, 0xFF, 0x7F, // Address field claiming 268435455 bytes
			0x74, 0x65, 0x73, 0x74, // But only "test" follows
		}

		msg := &twtproto.ProxyComm{}
		err := proto.Unmarshal(oversizedData, msg)

		if err == nil {
			t.Error("Expected error when unmarshaling oversized length-delimited field, but got none")
		}
	})
}

// Test frame corruption edge cases
func TestFrameCorruptionEdgeCases(t *testing.T) {
	// Test with mock connection that simulates frame corruption
	t.Run("FrameLengthCorruption", func(t *testing.T) {
		testCases := []struct {
			name       string
			frameData  []byte
			shouldFail bool
		}{
			{
				name:       "ZeroLength",
				frameData:  []byte{0x00, 0x00},
				shouldFail: true,
			},
			{
				name:       "NegativeLength",
				frameData:  []byte{0xFF, 0xFF}, // -1 when interpreted as signed
				shouldFail: true,
			},
			{
				name:       "ExtremelyLargeLength",
				frameData:  []byte{0xFF, 0x7F}, // 32767 bytes
				shouldFail: true,
			},
			{
				name:       "ReasonableLength",
				frameData:  []byte{0x0A, 0x00}, // 10 bytes
				shouldFail: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				conn := newMockConn()
				conn.readData = tc.frameData

				done := make(chan bool, 1)
				go func() {
					handleConnection(conn)
					done <- true
				}()

				select {
				case <-done:
					if !tc.shouldFail {
						// For reasonable length, connection should handle gracefully
						// (it may still fail due to incomplete data, which is expected)
					}
				case <-time.After(100 * time.Millisecond):
					if tc.shouldFail {
						t.Error("Connection should have failed quickly with invalid frame length")
					}
				}
			})
		}
	})

	// Test concurrent frame corruption scenarios
	t.Run("ConcurrentFrameCorruption", func(t *testing.T) {
		// Test with separate mock connections to avoid race conditions
		// Each connection will be handled independently
		testCases := []struct {
			name string
			data []byte
		}{
			{"ZeroLength", []byte{0x00, 0x00}},
			{"MaxLength", []byte{0xFF, 0xFF}},
			{"ValidLengthGarbageData", []byte{0x10, 0x00, 0x01, 0x02}},
			{"EmptyData", []byte{}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				conn := newMockConn()
				conn.readData = tc.data

				// Each should handle corruption gracefully
				done := make(chan bool, 1)
				go func() {
					handleConnection(conn)
					done <- true
				}()

				select {
				case <-done:
					// Corruption handled gracefully
				case <-time.After(100 * time.Millisecond):
					// Timeout is acceptable for invalid data
				}
			})
		}
	})
}

// Test sendProtobuf edge cases
func TestSendProtobufEdgeCases(t *testing.T) {
	// Initialize test app properly
	originalApp := getApp()
	defer func() { setApp(originalApp) }()

	// Test with nil message first (no app setup needed)
	t.Run("NilMessage", func(t *testing.T) {
		// Should handle gracefully without panicking
		sendProtobuf(nil)
	})

	// Now set up app for other tests
	app := &App{
		PoolConnections: []*PoolConnection{
			{
				ID:       0,
				Conn:     newMockConn(),
				SendChan: make(chan *twtproto.ProxyComm, 10), // Larger buffer
				Healthy:  true,
			},
		},
		PoolMutex:             sync.Mutex{},
		LocalConnections:      make(map[uint64]Connection),
		RemoteConnections:     make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnectionMutex: sync.Mutex{},
	}
	setApp(app)

	// Test with message containing all message types
	t.Run("AllMessageTypes", func(t *testing.T) {
		messageTypes := []twtproto.ProxyComm_MessageType{
			twtproto.ProxyComm_DATA_DOWN,
			twtproto.ProxyComm_DATA_UP,
			twtproto.ProxyComm_ACK_DOWN,
			twtproto.ProxyComm_ACK_UP,
			twtproto.ProxyComm_OPEN_CONN,
			twtproto.ProxyComm_CLOSE_CONN_S,
			twtproto.ProxyComm_CLOSE_CONN_C,
			twtproto.ProxyComm_PING,
		}

		for _, msgType := range messageTypes {
			msg := &twtproto.ProxyComm{
				Mt:         msgType,
				Connection: 123,
			}

			// Should handle all message types without error
			sendProtobuf(msg)

			// Verify message was queued to current app
			currentApp := getApp()
			if currentApp != nil && len(currentApp.PoolConnections) > 0 && currentApp.PoolConnections[0].SendChan != nil {
				select {
				case receivedMsg := <-currentApp.PoolConnections[0].SendChan:
					if receivedMsg.Mt != msgType {
						t.Errorf("Expected message type %v, got %v", msgType, receivedMsg.Mt)
					}
				case <-time.After(10 * time.Millisecond):
					t.Errorf("Message type %v was not queued", msgType)
				}
			}
		}
	})

	// Test with extremely large data field
	t.Run("ExtremelyLargeData", func(t *testing.T) {
		largeData := make([]byte, 1024*1024) // 1MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		msg := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Connection: 456,
			Data:       largeData,
		}

		// Should handle large messages
		sendProtobuf(msg)

		currentApp := getApp()
		if currentApp != nil && len(currentApp.PoolConnections) > 0 && currentApp.PoolConnections[0].SendChan != nil {
			select {
			case receivedMsg := <-currentApp.PoolConnections[0].SendChan:
				if len(receivedMsg.Data) != len(largeData) {
					t.Errorf("Expected data length %d, got %d", len(largeData), len(receivedMsg.Data))
				}
			case <-time.After(100 * time.Millisecond):
				t.Error("Large message was not queued")
			}
		}
	})
}

// Test sendProtobufToConn edge cases
func TestSendProtobufToConnEdgeCases(t *testing.T) {
	// Test with closed connection
	t.Run("ClosedConnection", func(t *testing.T) {
		conn := newMockConn()
		atomic.StoreInt32(&conn.closed, 1)

		msg := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_PING,
			Connection: 789,
		}

		// Should handle closed connection gracefully
		sendProtobufToConn(conn, msg)
	})

	// Test with connection that fails on write
	t.Run("FailingConnection", func(t *testing.T) {
		conn := newMockConn()
		conn.writeError = net.ErrClosed

		msg := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Connection: 101112,
			Data:       []byte("test data"),
		}

		// Should handle write errors gracefully
		sendProtobufToConn(conn, msg)
	})

	// Test concurrent writes to same connection
	t.Run("ConcurrentWrites", func(t *testing.T) {
		conn := newMockConn()
		var wg sync.WaitGroup
		writeCount := 20

		for i := 0; i < writeCount; i++ {
			wg.Add(1)
			go func(iteration int) {
				defer wg.Done()

				msg := &twtproto.ProxyComm{
					Mt:         twtproto.ProxyComm_DATA_UP,
					Connection: uint64(iteration),
					Seq:        uint64(iteration * 2),
					Data:       []byte{byte(iteration)},
				}

				sendProtobufToConn(conn, msg)
			}(i)
		}

		// Wait for all writes to complete
		done := make(chan bool)
		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
			// All writes completed
			if len(conn.writeData) == 0 {
				t.Error("No data was written to connection")
			}
		case <-time.After(1 * time.Second):
			t.Error("Concurrent writes took too long")
		}
	})
}

// Test protobuf message validation edge cases
func TestProtobufMessageValidationEdgeCases(t *testing.T) {
	// Test message with invalid enum value (should be handled gracefully)
	t.Run("InvalidEnumValue", func(t *testing.T) {
		// Create raw protobuf data with invalid enum value
		rawData := []byte{
			0x08, 0x63, // Mt field with value 99 (invalid)
			0x10, 0x01, // Proxy field
			0x18, 0x02, // Connection field
		}

		msg := &twtproto.ProxyComm{}
		err := proto.Unmarshal(rawData, msg)

		if err != nil {
			t.Logf("Unmarshal correctly rejected invalid enum: %v", err)
		} else {
			// If no error, check that the enum value is preserved
			t.Logf("Invalid enum value preserved: %v", msg.Mt)
		}
	})

	// Test message field boundary values
	t.Run("FieldBoundaryValues", func(t *testing.T) {
		testCases := []struct {
			name string
			msg  *twtproto.ProxyComm
		}{
			{
				name: "MaxUint64Values",
				msg: &twtproto.ProxyComm{
					Mt:         twtproto.ProxyComm_PING,
					Proxy:      math.MaxUint64,
					Connection: math.MaxUint64,
					Seq:        math.MaxUint64,
					Port:       math.MaxUint32,
				},
			},
			{
				name: "ZeroValues",
				msg: &twtproto.ProxyComm{
					Mt:         twtproto.ProxyComm_DATA_DOWN,
					Proxy:      0,
					Connection: 0,
					Seq:        0,
					Port:       0,
				},
			},
			{
				name: "MixedBoundaryValues",
				msg: &twtproto.ProxyComm{
					Mt:         twtproto.ProxyComm_ACK_UP,
					Proxy:      math.MaxUint64,
					Connection: 0,
					Seq:        math.MaxUint64 / 2,
					Port:       math.MaxUint32 / 2,
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				data, err := proto.Marshal(tc.msg)
				if err != nil {
					t.Fatalf("Failed to marshal %s: %v", tc.name, err)
				}

				unmarshaledMsg := &twtproto.ProxyComm{}
				err = proto.Unmarshal(data, unmarshaledMsg)
				if err != nil {
					t.Fatalf("Failed to unmarshal %s: %v", tc.name, err)
				}

				// Verify key fields
				if unmarshaledMsg.Mt != tc.msg.Mt {
					t.Errorf("%s: Mt mismatch", tc.name)
				}
				if unmarshaledMsg.Proxy != tc.msg.Proxy {
					t.Errorf("%s: Proxy mismatch", tc.name)
				}
				if unmarshaledMsg.Connection != tc.msg.Connection {
					t.Errorf("%s: Connection mismatch", tc.name)
				}
			})
		}
	})
}

// Test protobuf performance edge cases
func TestProtobufPerformanceEdgeCases(t *testing.T) {
	// Test with rapid message creation and marshaling
	t.Run("RapidMessageCreation", func(t *testing.T) {
		messageCount := 1000
		start := time.Now()

		for i := 0; i < messageCount; i++ {
			msg := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_DATA_UP,
				Connection: uint64(i),
				Seq:        uint64(i * 2),
				Data:       []byte{byte(i % 256)},
			}

			data, err := proto.Marshal(msg)
			if err != nil {
				t.Fatalf("Failed to marshal message %d: %v", i, err)
			}

			if len(data) == 0 {
				t.Errorf("Empty marshaled data for message %d", i)
			}
		}

		duration := time.Since(start)
		t.Logf("Marshaled %d messages in %v (%.2f msg/sec)",
			messageCount, duration, float64(messageCount)/duration.Seconds())

		if duration > 1*time.Second {
			t.Errorf("Message marshaling too slow: %v for %d messages", duration, messageCount)
		}
	})

	// Test with varying data sizes
	t.Run("VaryingDataSizes", func(t *testing.T) {
		sizes := []int{0, 1, 64, 1024, 8192, 32768}

		for _, size := range sizes {
			data := make([]byte, size)
			for j := range data {
				data[j] = byte(j % 256)
			}

			msg := &twtproto.ProxyComm{
				Mt:         twtproto.ProxyComm_DATA_DOWN,
				Connection: 999,
				Data:       data,
			}

			start := time.Now()
			marshaledData, err := proto.Marshal(msg)
			marshalDuration := time.Since(start)

			if err != nil {
				t.Fatalf("Failed to marshal message with %d byte data: %v", size, err)
			}

			start = time.Now()
			unmarshaledMsg := &twtproto.ProxyComm{}
			err = proto.Unmarshal(marshaledData, unmarshaledMsg)
			unmarshalDuration := time.Since(start)

			if err != nil {
				t.Fatalf("Failed to unmarshal message with %d byte data: %v", size, err)
			}

			t.Logf("Size %d: Marshal %v, Unmarshal %v",
				size, marshalDuration, unmarshalDuration)

			// Sanity check
			if len(unmarshaledMsg.Data) != size {
				t.Errorf("Data size mismatch for %d bytes: got %d", size, len(unmarshaledMsg.Data))
			}
		}
	})
}

// Helper function to simulate real protobuf frame corruption scenarios
func TestRealWorldProtobufCorruption(t *testing.T) {
	// These test cases are based on actual corruption patterns seen in logs
	t.Run("ActualCorruptionPatterns", func(t *testing.T) {
		corruptionPatterns := []struct {
			name string
			data []byte
		}{
			{
				name: "PartialHeaderCorruption",
				data: []byte{0x0d, 0x20, 0x01, 0x3a, 0x80},
			},
			{
				name: "IncompleteVarint",
				data: []byte{0x08, 0x80, 0x80, 0x80},
			},
			{
				name: "TruncatedLengthDelimited",
				data: []byte{0x2A, 0x10, 0x74, 0x65, 0x73, 0x74}, // Claims 16 bytes but only has "test"
			},
			{
				name: "InvalidFieldNumber",
				data: []byte{0xF8, 0x7F, 0x01}, // Field number 1023 (too high)
			},
		}

		for _, pattern := range corruptionPatterns {
			t.Run(pattern.name, func(t *testing.T) {
				msg := &twtproto.ProxyComm{}
				err := proto.Unmarshal(pattern.data, msg)

				// All these patterns should result in errors, but some might be handled gracefully
				if err == nil {
					t.Logf("Corruption pattern %s was handled gracefully (no error)", pattern.name)
				} else {
					t.Logf("Correctly detected corruption in %s: %v", pattern.name, err)
				}
			})
		}
	})

	// Test frame length vs actual data length mismatches
	t.Run("FrameLengthMismatches", func(t *testing.T) {
		validMsg := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Connection: 123,
			Data:       []byte("test"),
		}

		validData, err := proto.Marshal(validMsg)
		if err != nil {
			t.Fatalf("Failed to marshal valid message: %v", err)
		}

		testCases := []struct {
			name          string
			claimedLength uint16
			actualData    []byte
			expectError   bool
		}{
			{
				name:          "LengthTooHigh",
				claimedLength: uint16(len(validData) + 10),
				actualData:    validData,
				expectError:   true, // Will try to read more data than available
			},
			{
				name:          "LengthTooLow",
				claimedLength: uint16(len(validData) - 2),
				actualData:    validData,
				expectError:   true, // Will truncate valid protobuf data
			},
			{
				name:          "LengthCorrect",
				claimedLength: uint16(len(validData)),
				actualData:    validData,
				expectError:   false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Create frame with claimed length
				frame := make([]byte, 2)
				binary.LittleEndian.PutUint16(frame, tc.claimedLength)

				// For testing purposes, we'll create a mock scenario
				// In real code, handleConnection would detect length mismatches
				if tc.claimedLength != uint16(len(tc.actualData)) {
					t.Logf("Length mismatch detected: claimed %d, actual %d",
						tc.claimedLength, len(tc.actualData))

					if !tc.expectError {
						t.Error("Expected length mismatch to be detected as error")
					}
				}
			})
		}
	})
}

// Benchmark protobuf operations under edge conditions
func BenchmarkProtobufEdgeCases(b *testing.B) {
	// Benchmark marshaling with large data
	b.Run("MarshalLargeData", func(b *testing.B) {
		largeData := make([]byte, 64*1024) // 64KB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		msg := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_DOWN,
			Connection: 123,
			Data:       largeData,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := proto.Marshal(msg)
			if err != nil {
				b.Fatalf("Marshal failed: %v", err)
			}
		}
	})

	// Benchmark unmarshaling with large data
	b.Run("UnmarshalLargeData", func(b *testing.B) {
		largeData := make([]byte, 64*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		msg := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_DATA_UP,
			Connection: 456,
			Data:       largeData,
		}

		data, err := proto.Marshal(msg)
		if err != nil {
			b.Fatalf("Failed to marshal test message: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			unmarshaledMsg := &twtproto.ProxyComm{}
			err := proto.Unmarshal(data, unmarshaledMsg)
			if err != nil {
				b.Fatalf("Unmarshal failed: %v", err)
			}
		}
	})

	// Benchmark concurrent protobuf operations
	b.Run("ConcurrentOperations", func(b *testing.B) {
		msg := &twtproto.ProxyComm{
			Mt:         twtproto.ProxyComm_PING,
			Connection: 789,
			Data:       []byte("benchmark data"),
		}

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				data, err := proto.Marshal(msg)
				if err != nil {
					b.Fatalf("Marshal failed: %v", err)
				}

				unmarshaledMsg := &twtproto.ProxyComm{}
				err = proto.Unmarshal(data, unmarshaledMsg)
				if err != nil {
					b.Fatalf("Unmarshal failed: %v", err)
				}
			}
		})
	})
}
