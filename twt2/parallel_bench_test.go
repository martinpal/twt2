package twt2

import (
	"testing"
	"time"
)

// Benchmark for sequential pool creation (simulated)
func BenchmarkSequentialPoolCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Simulate sequential creation of 20 connections
		// Each connection would take some time to establish SSH
		connections := make([]*PoolConnection, 0, 20)
		for j := 0; j < 20; j++ {
			// Simulate connection creation time (this would fail quickly due to no key)
			start := time.Now()
			poolConn := createPoolConnection(uint64(j), "example.com", 22, false, "user", "/nonexistent/key", 22)
			if poolConn != nil {
				connections = append(connections, poolConn)
			}
			// Ensure minimum time spent (simulating real SSH connection time)
			elapsed := time.Since(start)
			if elapsed < time.Millisecond {
				time.Sleep(time.Millisecond - elapsed)
			}
		}
	}
}

// Benchmark for parallel pool creation
func BenchmarkParallelPoolCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Use our parallel implementation
		connections := createPoolConnectionsParallel(20, "example.com", 22, false, "user", "/nonexistent/key", 22)
		_ = connections // Use the variable to avoid compiler optimization
	}
}

// Test to demonstrate timing difference
func TestParallelVsSequentialTiming(t *testing.T) {
	// This test demonstrates the timing difference between sequential and parallel approaches
	// Note: Both will fail due to invalid SSH key, but we can measure the parallelization overhead

	// Test sequential approach (simulated)
	start := time.Now()
	sequentialConnections := make([]*PoolConnection, 0, 10)
	for i := 0; i < 10; i++ {
		poolConn := createPoolConnection(uint64(i), "example.com", 22, false, "user", "/nonexistent/key", 22)
		if poolConn != nil {
			sequentialConnections = append(sequentialConnections, poolConn)
		}
	}
	sequentialTime := time.Since(start)

	// Test parallel approach
	start = time.Now()
	parallelConnections := createPoolConnectionsParallel(10, "example.com", 22, false, "user", "/nonexistent/key", 22)
	parallelTime := time.Since(start)

	t.Logf("Sequential creation of 10 connections took: %v", sequentialTime)
	t.Logf("Parallel creation of 10 connections took: %v", parallelTime)

	// Both should create connections in different ways
	// Sequential: returns nil for failed connections
	// Parallel: creates placeholder connections for failed attempts
	if len(sequentialConnections) != 0 {
		t.Errorf("Expected 0 sequential connections (nil for failures), got %d", len(sequentialConnections))
	}
	if len(parallelConnections) != 10 {
		t.Errorf("Expected 10 parallel placeholder connections, got %d", len(parallelConnections))
	}

	// Stop retry goroutines to prevent test interference
	for _, conn := range parallelConnections {
		if conn.retryCancel != nil {
			conn.retryCancel()
		}
	}

	// The parallel approach should generally be faster or at least comparable
	// (though with failing connections, the difference might be minimal)
	t.Logf("Parallel approach efficiency: %.2f%% of sequential time",
		float64(parallelTime)/float64(sequentialTime)*100)
}
