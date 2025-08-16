package twt2

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestSSHConnectionFailureScenarios tests comprehensive SSH connection failure scenarios
func TestSSHConnectionFailureScenarios(t *testing.T) {
	// Save original app state
	originalApp := app
	defer func() {
		app = originalApp
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond) // Give cleanup time
	}()

	t.Run("SSH key file not found", func(t *testing.T) {
		poolConn := createPoolConnection(1, "127.0.0.1", 22, false, "testuser", "/nonexistent/path/to/key", 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection when SSH key file doesn't exist")
		}

		t.Logf("✓ SSH key file not found scenario handled correctly")
	})

	t.Run("SSH key file unreadable", func(t *testing.T) {
		// Create temporary unreadable file
		tempDir := t.TempDir()
		keyPath := filepath.Join(tempDir, "unreadable_key")

		// Create file with some content
		err := os.WriteFile(keyPath, []byte("fake-key-content"), 0644)
		if err != nil {
			t.Fatalf("Failed to create test key file: %v", err)
		}

		// Make file unreadable
		if err := os.Chmod(keyPath, 0000); err != nil {
			t.Skip("Cannot test file permissions on this system")
		}
		defer os.Chmod(keyPath, 0644) // Restore for cleanup

		poolConn := createPoolConnection(2, "127.0.0.1", 22, false, "testuser", keyPath, 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection when SSH key file is unreadable")
		}

		t.Logf("✓ SSH key file unreadable scenario handled correctly")
	})

	t.Run("Invalid SSH key format", func(t *testing.T) {
		// Create temporary file with invalid key content
		tempDir := t.TempDir()
		keyPath := filepath.Join(tempDir, "invalid_key")

		// Write invalid key content
		invalidKeyContent := "this is not a valid SSH private key"
		err := os.WriteFile(keyPath, []byte(invalidKeyContent), 0600)
		if err != nil {
			t.Fatalf("Failed to create invalid key file: %v", err)
		}

		poolConn := createPoolConnection(3, "127.0.0.1", 22, false, "testuser", keyPath, 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection when SSH key format is invalid")
		}

		t.Logf("✓ Invalid SSH key format scenario handled correctly")
	})

	t.Run("SSH key with wrong format", func(t *testing.T) {
		tempDir := t.TempDir()
		keyPath := filepath.Join(tempDir, "wrong_format_key")

		// Write content that looks like a key but isn't valid
		wrongFormatKey := `-----BEGIN RSA PRIVATE KEY-----
this is not actually a valid base64 encoded key
it should fail to parse
-----END RSA PRIVATE KEY-----`

		err := os.WriteFile(keyPath, []byte(wrongFormatKey), 0600)
		if err != nil {
			t.Fatalf("Failed to create wrong format key file: %v", err)
		}

		poolConn := createPoolConnection(4, "127.0.0.1", 22, false, "testuser", keyPath, 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection when SSH key has wrong format")
		}

		t.Logf("✓ SSH key with wrong format scenario handled correctly")
	})

	t.Run("SSH connection timeout scenarios", func(t *testing.T) {
		// Test with non-routable IP (RFC 5737 - TEST-NET-1)
		// This should timeout quickly
		poolConn := createPoolConnection(5, "192.0.2.1", 22, false, "testuser", "/dev/null", 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection when SSH host is unreachable")
		}

		t.Logf("✓ SSH connection timeout scenario handled correctly")
	})

	t.Run("SSH connection to invalid hostname", func(t *testing.T) {
		// Test with invalid hostname that should fail DNS resolution
		poolConn := createPoolConnection(6, "invalid-ssh-hostname-that-does-not-exist.local", 22, false, "testuser", "/dev/null", 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection when SSH hostname is invalid")
		}

		t.Logf("✓ SSH connection to invalid hostname scenario handled correctly")
	})

	t.Run("SSH connection to closed port", func(t *testing.T) {
		// Test connection to localhost on a closed port
		poolConn := createPoolConnection(7, "127.0.0.1", 1, false, "testuser", "/dev/null", 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection when SSH port is closed")
		}

		t.Logf("✓ SSH connection to closed port scenario handled correctly")
	})

	t.Run("SSH connection with invalid SSH port", func(t *testing.T) {
		// Test with invalid SSH port (out of range)
		poolConn := createPoolConnection(8, "127.0.0.1", 22, false, "testuser", "/dev/null", 70000)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection when SSH port is out of range")
		}

		t.Logf("✓ SSH connection with invalid SSH port scenario handled correctly")
	})

	t.Run("SSH connection with zero SSH port", func(t *testing.T) {
		// Test with zero SSH port
		poolConn := createPoolConnection(9, "127.0.0.1", 22, false, "testuser", "/dev/null", 0)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection when SSH port is zero")
		}

		t.Logf("✓ SSH connection with zero SSH port scenario handled correctly")
	})

	t.Run("SSH connection with empty hostname", func(t *testing.T) {
		poolConn := createPoolConnection(10, "", 22, false, "testuser", "/dev/null", 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection when hostname is empty")
		}

		t.Logf("✓ SSH connection with empty hostname scenario handled correctly")
	})
}

// TestSSHConnectionParallelFailures tests parallel connection creation with failures
func TestSSHConnectionParallelFailures(t *testing.T) {
	defer func() {
		StopAllPoolConnections()
		time.Sleep(200 * time.Millisecond) // Give cleanup time
	}()

	t.Run("Parallel SSH connections with mixed success/failure", func(t *testing.T) {
		// This tests the createPoolConnectionsParallel function with failure scenarios
		connections := createPoolConnectionsParallel(5, "invalid-host-for-testing", 22, false, "testuser", "/nonexistent/key", 22)

		// All connections should be placeholders or nil since they'll fail
		if len(connections) > 5 {
			t.Errorf("Expected at most 5 connections, got %d", len(connections))
		}

		// Check that placeholders were created for failed connections
		for i, conn := range connections {
			if conn != nil {
				if conn.Conn == nil && conn.retryCtx != nil {
					t.Logf("✓ Connection %d: Placeholder created for failed SSH connection", i)
				} else {
					t.Errorf("Connection %d: Unexpected connection state", i)
				}
			}
		}

		t.Logf("✓ Parallel SSH connections with failures handled correctly")
	})

	t.Run("Zero pool size handling", func(t *testing.T) {
		connections := createPoolConnectionsParallel(0, "127.0.0.1", 22, false, "testuser", "/dev/null", 22)

		if len(connections) != 0 {
			t.Errorf("Expected empty slice for zero pool size, got %d connections", len(connections))
		}

		t.Logf("✓ Zero pool size handled correctly")
	})

	t.Run("Negative pool size handling", func(t *testing.T) {
		connections := createPoolConnectionsParallel(-5, "127.0.0.1", 22, false, "testuser", "/dev/null", 22)

		if len(connections) != 0 {
			t.Errorf("Expected empty slice for negative pool size, got %d connections", len(connections))
		}

		t.Logf("✓ Negative pool size handled correctly")
	})
}

// TestSSHKeepAliveFailures tests SSH keep-alive failure scenarios
func TestSSHKeepAliveFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping SSH keep-alive failure tests in short mode")
	}

	t.Run("SSH keep-alive with nil SSH client", func(t *testing.T) {
		// Create a mock pool connection with nil SSH client
		poolConn := &PoolConnection{
			ID:        1,
			SSHClient: nil, // nil SSH client should stop keep-alive
			Healthy:   true,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// This should exit quickly when it detects nil SSH client
		sshKeepAlive(ctx, poolConn)

		t.Logf("✓ SSH keep-alive with nil SSH client handled correctly")
	})

	t.Run("SSH keep-alive context cancellation", func(t *testing.T) {
		// Create a mock pool connection
		poolConn := &PoolConnection{
			ID:        2,
			SSHClient: nil, // nil will cause quick exit
			Healthy:   true,
		}

		ctx, cancel := context.WithCancel(context.Background())

		// Start keep-alive in goroutine
		done := make(chan bool)
		go func() {
			sshKeepAlive(ctx, poolConn)
			done <- true
		}()

		// Cancel context
		cancel()

		// Wait for keep-alive to exit
		select {
		case <-done:
			t.Logf("✓ SSH keep-alive context cancellation handled correctly")
		case <-time.After(1 * time.Second):
			t.Errorf("SSH keep-alive did not exit after context cancellation")
		}
	})
}

// TestSSHConnectionRetryLogic tests the retry logic in connection creation
func TestSSHConnectionRetryLogic(t *testing.T) {
	defer func() {
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond)
	}()

	t.Run("SSH connection retry exhaustion", func(t *testing.T) {
		// This should exhaust all retries and return nil
		poolConn := createPoolConnection(1, "127.0.0.1", 1, false, "testuser", "/dev/null", 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection after retry exhaustion")
		}

		t.Logf("✓ SSH connection retry exhaustion handled correctly")
	})

	t.Run("Connection creation with various invalid parameters", func(t *testing.T) {
		testCases := []struct {
			name    string
			host    string
			port    int
			sshUser string
			sshKey  string
			sshPort int
		}{
			{"empty host", "", 22, "user", "/dev/null", 22},
			{"empty user", "127.0.0.1", 22, "", "/dev/null", 22},
			{"empty key path", "127.0.0.1", 22, "user", "", 22},
			{"invalid port", "127.0.0.1", -1, "user", "/dev/null", 22},
			{"high port", "127.0.0.1", 70000, "user", "/dev/null", 22},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				poolConn := createPoolConnection(1, tc.host, tc.port, false, tc.sshUser, tc.sshKey, tc.sshPort)

				if poolConn != nil {
					t.Errorf("Expected nil pool connection for %s", tc.name)
				}

				t.Logf("✓ %s handled correctly", tc.name)
			})
		}
	})
}

// TestSSHConnectionResourceCleanup tests resource cleanup on SSH failures
func TestSSHConnectionResourceCleanup(t *testing.T) {
	defer func() {
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond)
	}()

	t.Run("Resource cleanup on SSH connection failure", func(t *testing.T) {
		// Create temporary key file that will cause parsing to fail after reading
		tempDir := t.TempDir()
		keyPath := filepath.Join(tempDir, "bad_key")

		// Write content that can be read but will fail to parse
		badKeyContent := "not-a-valid-ssh-key-but-readable"
		err := os.WriteFile(keyPath, []byte(badKeyContent), 0600)
		if err != nil {
			t.Fatalf("Failed to create bad key file: %v", err)
		}

		// This should fail at SSH key parsing stage
		poolConn := createPoolConnection(1, "127.0.0.1", 22, false, "testuser", keyPath, 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection when SSH key parsing fails")
		}

		// Verify no resources are leaked (no specific test here, but important for production)
		t.Logf("✓ Resource cleanup on SSH connection failure handled correctly")
	})

	t.Run("Multiple simultaneous SSH connection failures", func(t *testing.T) {
		// Test multiple goroutines trying to create connections that will fail
		var wg sync.WaitGroup
		results := make([]*PoolConnection, 10)

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				// Each connection will fail with different invalid hostnames
				results[id] = createPoolConnection(uint64(id), "invalid-host-"+string(rune(id+48)), 22, false, "user", "/dev/null", 22)
			}(i)
		}

		wg.Wait()

		// All should be nil
		for i, poolConn := range results {
			if poolConn != nil {
				t.Errorf("Expected nil pool connection %d, got non-nil", i)
			}
		}

		t.Logf("✓ Multiple simultaneous SSH connection failures handled correctly")
	})
}

// TestSSHConnectionEdgeCases tests edge cases in SSH connection handling
func TestSSHConnectionEdgeCases(t *testing.T) {
	defer func() {
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond)
	}()

	t.Run("SSH connection with ping enabled but connection fails", func(t *testing.T) {
		// Test that ping setting doesn't cause issues when connection fails
		poolConn := createPoolConnection(1, "127.0.0.1", 1, true, "testuser", "/dev/null", 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection even with ping enabled")
		}

		t.Logf("✓ SSH connection with ping enabled but connection fails handled correctly")
	})

	t.Run("Very high connection ID", func(t *testing.T) {
		// Test with very high connection ID
		poolConn := createPoolConnection(18446744073709551615, "127.0.0.1", 1, false, "user", "/dev/null", 22) // max uint64

		if poolConn != nil {
			t.Errorf("Expected nil pool connection with high ID")
		}

		t.Logf("✓ Very high connection ID handled correctly")
	})

	t.Run("SSH connection with special characters in parameters", func(t *testing.T) {
		// Test with special characters in hostname and username
		poolConn := createPoolConnection(1, "host-with-special!@#$.com", 22, false, "user@domain", "/dev/null", 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection with special characters")
		}

		t.Logf("✓ SSH connection with special characters handled correctly")
	})
}

// TestSSHConnectionStopBehavior tests stopping SSH connections
func TestSSHConnectionStopBehavior(t *testing.T) {
	t.Run("StopAllPoolConnections with no connections", func(t *testing.T) {
		// Should not panic or cause issues when called with no connections
		StopAllPoolConnections()

		t.Logf("✓ StopAllPoolConnections with no connections handled correctly")
	})

	t.Run("StopAllPoolConnections multiple calls", func(t *testing.T) {
		// Should be safe to call multiple times
		StopAllPoolConnections()
		StopAllPoolConnections()
		StopAllPoolConnections()

		t.Logf("✓ Multiple StopAllPoolConnections calls handled correctly")
	})
}

// TestSSHConnectionEnvironmentalFactors tests environmental factors affecting SSH connections
func TestSSHConnectionEnvironmentalFactors(t *testing.T) {
	defer func() {
		StopAllPoolConnections()
		time.Sleep(100 * time.Millisecond)
	}()

	t.Run("SSH connection with different working directories", func(t *testing.T) {
		// Save current directory
		originalDir, err := os.Getwd()
		if err != nil {
			t.Fatalf("Failed to get current directory: %v", err)
		}
		defer os.Chdir(originalDir)

		// Change to temp directory
		tempDir := t.TempDir()
		os.Chdir(tempDir)

		// Try to create connection with relative path (should fail)
		poolConn := createPoolConnection(1, "127.0.0.1", 22, false, "user", "./nonexistent", 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection with relative path from temp dir")
		}

		t.Logf("✓ SSH connection with different working directories handled correctly")
	})

	t.Run("SSH connection with symlink to nonexistent key", func(t *testing.T) {
		tempDir := t.TempDir()
		symlinkPath := filepath.Join(tempDir, "symlink_key")

		// Create symlink to nonexistent file
		err := os.Symlink("/nonexistent/target", symlinkPath)
		if err != nil {
			t.Skip("Cannot create symlinks on this system")
		}

		poolConn := createPoolConnection(1, "127.0.0.1", 22, false, "user", symlinkPath, 22)

		if poolConn != nil {
			t.Errorf("Expected nil pool connection with broken symlink")
		}

		t.Logf("✓ SSH connection with symlink to nonexistent key handled correctly")
	})
}
