package twt2

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestMetricsEndpoint(t *testing.T) {
	// Create a minimal app instance
	testApp := &App{
		PoolConnections:       make([]*PoolConnection, 0),
		PoolMutex:             sync.Mutex{},
		LocalConnections:      make(map[uint64]Connection),
		LocalConnectionMutex:  sync.Mutex{},
		RemoteConnections:     make(map[uint64]Connection),
		RemoteConnectionMutex: sync.Mutex{},
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

	fmt.Printf("Metrics output:\n%s\n", body)
}
