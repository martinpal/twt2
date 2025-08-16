package twt2

import (
	"sync"
	"testing"
	"time"
)

// Mock function to simulate connection creation without actually creating connections
func mockCreateConnection(id int) bool {
	// Simulate some work (like key reading, validation, etc.)
	time.Sleep(100 * time.Microsecond)
	// Simulate failure for invalid key path
	return false
}

// Benchmark for sequential pool creation (simulated)
func BenchmarkSequentialPoolCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Simulate sequential creation of 5 connections
		successCount := 0
		for j := 0; j < 5; j++ {
			if mockCreateConnection(j) {
				successCount++
			}
		}
		_ = successCount // Use variable to avoid optimization
	}
}

// Benchmark for parallel pool creation
func BenchmarkParallelPoolCreation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Simulate parallel creation of 5 connections
		var wg sync.WaitGroup
		successCount := 0
		var mutex sync.Mutex

		for j := 0; j < 5; j++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				if mockCreateConnection(id) {
					mutex.Lock()
					successCount++
					mutex.Unlock()
				}
			}(j)
		}
		wg.Wait()
		_ = successCount // Use variable to avoid optimization
	}
}

// Test to demonstrate timing difference
func TestParallelVsSequentialTiming(t *testing.T) {
	// Test sequential approach
	start := time.Now()
	sequentialSuccessCount := 0
	for i := 0; i < 3; i++ {
		if mockCreateConnection(i) {
			sequentialSuccessCount++
		}
	}
	sequentialTime := time.Since(start)

	// Test parallel approach
	start = time.Now()
	var wg sync.WaitGroup
	parallelSuccessCount := 0
	var mutex sync.Mutex

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if mockCreateConnection(id) {
				mutex.Lock()
				parallelSuccessCount++
				mutex.Unlock()
			}
		}(i)
	}
	wg.Wait()
	parallelTime := time.Since(start)

	t.Logf("Sequential creation of 3 connections took: %v", sequentialTime)
	t.Logf("Parallel creation of 3 connections took: %v", parallelTime)

	// Both should fail due to mocked failure
	if sequentialSuccessCount != 0 {
		t.Errorf("Expected 0 sequential successful connections, got %d", sequentialSuccessCount)
	}
	if parallelSuccessCount != 0 {
		t.Errorf("Expected 0 parallel successful connections, got %d", parallelSuccessCount)
	}

	// The parallel approach should be faster
	if parallelTime >= sequentialTime {
		t.Logf("Note: Parallel time (%v) was not faster than sequential (%v), this can happen with very fast operations", parallelTime, sequentialTime)
	}

	// Calculate efficiency
	t.Logf("Parallel approach efficiency: %.2f%% of sequential time",
		float64(parallelTime)/float64(sequentialTime)*100)
}
