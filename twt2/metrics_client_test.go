package twt2

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"palecci.cz/twtproto"
)

func TestMetricsEndpointWithPoolConnections(t *testing.T) {
	// Create a minimal app instance with pool connections (client-side)
	testApp := &App{
		PoolConnections:       make([]*PoolConnection, 0),
		PoolMutex:             sync.Mutex{},
		LocalConnections:      make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
	}

	// Add some mock pool connections to simulate client-side
	for i := 0; i < 3; i++ {
		stats := NewPoolConnectionStats()
		stats.IncrementMessage(twtproto.ProxyComm_DATA_UP, 100+i*50)
		stats.IncrementMessage(twtproto.ProxyComm_DATA_DOWN, 200+i*30)

		conn := &PoolConnection{
			ID:              uint64(i + 1),
			Healthy:         i < 2,  // First two are healthy
			InUse:           i == 0, // First one is in use
			SendChan:        make(chan *twtproto.ProxyComm, 10),
			Stats:           stats,
			LastHealthCheck: time.Now().Add(-time.Duration(i) * time.Minute),
			LastUsed:        time.Now().Add(-time.Duration(i) * 30 * time.Second),
		}

		// Add some items to the send channel to simulate queue
		if i == 0 {
			conn.SendChan <- &twtproto.ProxyComm{}
			conn.SendChan <- &twtproto.ProxyComm{}
		}

		testApp.PoolConnections = append(testApp.PoolConnections, conn)
	}

	// Set as global app for testing
	originalApp := app
	app = testApp
	defer func() { app = originalApp }() // Restore original after test

	// Create test request
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()

	// Call the metrics handler directly
	serveMetrics(w, req)

	// Check response
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	expectedContentType := "text/plain; version=0.0.4; charset=utf-8"
	if contentType != expectedContentType {
		t.Errorf("Expected content type %s, got %s", expectedContentType, contentType)
	}

	body := w.Body.String()
	if len(body) == 0 {
		t.Error("Expected non-empty metrics output")
	}

	// Verify some expected metrics are present
	expectedMetrics := []string{
		"twt2_pool_connections_total 3",
		"twt2_pool_connections_healthy 2",
		"twt2_pool_connections_in_use 1",
		"twt2_pool_connection_queue_length",
		"twt2_pool_connection_messages_total",
		"twt2_pool_connection_bytes_total",
		"data_up",
		"data_down",
	}

	for _, expected := range expectedMetrics {
		if !containsString(body, expected) {
			t.Errorf("Expected metrics output to contain '%s'", expected)
		}
	}

	fmt.Printf("Client-side metrics output:\n%s\n", body)
}

// Helper function to check if string contains substring
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && func() bool {
		for i := 0; i <= len(s)-len(substr); i++ {
			if s[i:i+len(substr)] == substr {
				return true
			}
		}
		return false
	}()
}
