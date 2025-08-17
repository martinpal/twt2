package main

import (
	"fmt"
	"time"

	"palecci.cz/twt2"
)

func main() {
	// Get initial configuration (should be production defaults)
	fmt.Println("Testing configuration in production context...")

	initial, max, retries := twt2.GetConnectionRetryConfigDebug()
	fmt.Printf("Initial delay: %v\n", initial)
	fmt.Printf("Max delay: %v\n", max)
	fmt.Printf("Max retries: %d\n", retries)

	// Production defaults should be: 1s, 30s, -1
	// Test values would be: 100ms, 500ms, 3
	if initial == 1*time.Second && max == 30*time.Second && retries == -1 {
		fmt.Println("✓ SUCCESS: Production defaults are active")
	} else if initial == 100*time.Millisecond && max == 500*time.Millisecond && retries == 3 {
		fmt.Println("✗ ERROR: Test configuration is active in production!")
	} else {
		fmt.Println("? UNKNOWN: Unexpected configuration values")
	}
}
