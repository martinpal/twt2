package twt2

import (
	"testing"
)

func TestIsTLSTraffic(t *testing.T) {
	tests := []struct {
		name     string
		header   []byte
		expected bool
	}{
		{
			name:     "TLS Handshake",
			header:   []byte{0x16, 0x03}, // TLS Handshake
			expected: true,
		},
		{
			name:     "TLS Application Data",
			header:   []byte{0x17, 0x03}, // TLS Application Data (from your error)
			expected: true,
		},
		{
			name:     "TLS Alert",
			header:   []byte{0x15, 0x03},
			expected: true,
		},
		{
			name:     "TLS Change Cipher Spec",
			header:   []byte{0x14, 0x03},
			expected: true,
		},
		{
			name:     "Valid Protobuf Length",
			header:   []byte{0x20, 0x00}, // Length 32 (0x0020 little endian)
			expected: false,
		},
		{
			name:     "Small Protobuf Message",
			header:   []byte{0x0A, 0x00}, // Length 10
			expected: false,
		},
		{
			name:     "Empty Header",
			header:   []byte{},
			expected: false,
		},
		{
			name:     "Single Byte Header",
			header:   []byte{0x16},
			expected: false,
		},
		{
			name:     "Non-TLS Traffic",
			header:   []byte{0x00, 0x01},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTLSTraffic(tt.header)
			if result != tt.expected {
				t.Errorf("isTLSTraffic(%v) = %v, expected %v", tt.header, result, tt.expected)
			}
		})
	}
}

func TestHandleConnection_TLSDetection(t *testing.T) {
	// Create a mock connection with TLS handshake data
	mockConn := newMockConn()

	// Add TLS handshake data (ClientHello)
	// This simulates what would happen if someone tries to connect with HTTPS
	tlsHandshake := []byte{
		0x16, 0x03, 0x01, 0x00, 0x64, // TLS Handshake header
		0x01, 0x00, 0x00, 0x60, // ClientHello
		// ... rest of handshake data
	}

	mockConn.mu.Lock()
	mockConn.readData = tlsHandshake
	mockConn.mu.Unlock()

	// Set up a test app to provide context
	testApp := &App{
		ListenPort: 33333,
	}
	app = testApp

	// This should detect TLS and close the connection gracefully
	// The function should return without causing a panic
	handleConnection(mockConn)

	// Verify the connection was closed
	if !mockConn.IsClosed() {
		t.Error("Expected connection to be closed after detecting TLS traffic")
	}
}
