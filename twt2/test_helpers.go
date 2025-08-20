package twt2

import (
	"runtime"
	"time"
)

// Test helper functions for safe app management during testing

// forceStopAllBackgroundGoroutines aggressively stops all pool connections and background processes
func forceStopAllBackgroundGoroutines() {
	// Stop all pool connections multiple times to be extra sure
	for i := 0; i < 5; i++ {
		StopAllPoolConnections()
		time.Sleep(200 * time.Millisecond)
	}

	// Clear the global app to signal all goroutines to stop
	setApp(nil)

	// Wait for all background goroutines to notice and exit
	time.Sleep(500 * time.Millisecond)

	// Stop pool connections again in case any were created in the meantime
	for i := 0; i < 3; i++ {
		StopAllPoolConnections()
		time.Sleep(50 * time.Millisecond)
	}

	// Force garbage collection to clean up any remaining resources
	// This helps ensure that any lingering references are cleaned up
	runtime.GC()
	runtime.GC() // Force twice to be sure
}

// SafeSetTestApp safely sets a test app after stopping all background goroutines
func SafeSetTestApp(testApp *App) *App {
	// Get original app to restore later
	originalApp := getApp()

	// Aggressively stop all background goroutines
	forceStopAllBackgroundGoroutines()

	// Now safely set the new app
	setApp(testApp)

	return originalApp
}

// SafeRestoreApp safely restores the original app
func SafeRestoreApp(originalApp *App) {
	// Stop the current app properly to clean up background goroutines
	currentApp := getApp()
	if currentApp != nil {
		currentApp.Stop()
	}

	// Aggressively stop all background goroutines
	forceStopAllBackgroundGoroutines()

	// Restore the original app
	setApp(originalApp)
}

// CleanTestEnvironment ensures a clean test environment
func CleanTestEnvironment() {
	forceStopAllBackgroundGoroutines()
}
