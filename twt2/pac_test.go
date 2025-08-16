package twt2

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestPACFileHandling tests comprehensive PAC file functionality
func TestPACFileHandling(t *testing.T) {
	// Save original app state
	originalApp := app
	defer func() { app = originalApp }()

	t.Run("PAC file with custom file path", func(t *testing.T) {
		// Create temporary PAC file
		tempDir := t.TempDir()
		pacFilePath := filepath.Join(tempDir, "test.pac")

		pacContent := `function FindProxyForURL(url, host) {
    host = host.toLowerCase();
    if (dnsDomainIs(host, ".test.com") || host == "test.com") {
        return "PROXY localhost:3128";
    }
    return "DIRECT";
}`

		err := os.WriteFile(pacFilePath, []byte(pacContent), 0644)
		if err != nil {
			t.Fatalf("Failed to create test PAC file: %v", err)
		}

		// Set up app with PAC file path
		setApp(&App{
			PACFilePath: pacFilePath,
		})

		// Test PAC file serving
		req := httptest.NewRequest("GET", "/proxy.pac", nil)
		req.RemoteAddr = "127.0.0.1:12345"
		w := httptest.NewRecorder()

		servePACFile(w, req)

		// Check response
		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
		}

		// Check headers
		expectedContentType := "application/x-ns-proxy-autoconfig"
		if ct := w.Header().Get("Content-Type"); ct != expectedContentType {
			t.Errorf("Expected Content-Type %q, got %q", expectedContentType, ct)
		}

		expectedCacheControl := "no-cache, no-store, must-revalidate"
		if cc := w.Header().Get("Cache-Control"); cc != expectedCacheControl {
			t.Errorf("Expected Cache-Control %q, got %q", expectedCacheControl, cc)
		}

		// Check content
		body := w.Body.String()
		if !strings.Contains(body, "FindProxyForURL") {
			t.Errorf("PAC file should contain FindProxyForURL function")
		}
		if !strings.Contains(body, "test.com") {
			t.Errorf("PAC file should contain test domain")
		}
	})

	t.Run("PAC file with nonexistent file", func(t *testing.T) {
		// Set up app with nonexistent PAC file
		setApp(&App{
			PACFilePath: "/nonexistent/path/test.pac",
		})

		req := httptest.NewRequest("GET", "/proxy.pac", nil)
		req.RemoteAddr = "127.0.0.1:12345"
		w := httptest.NewRecorder()

		servePACFile(w, req)

		// Should return 404 for nonexistent file
		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status %d for nonexistent file, got %d", http.StatusNotFound, w.Code)
		}
	})

	t.Run("PAC file without file path configured", func(t *testing.T) {
		// Set up app without PAC file path
		setApp(&App{
			PACFilePath: "",
		})

		req := httptest.NewRequest("GET", "/proxy.pac", nil)
		req.RemoteAddr = "127.0.0.1:12345"
		w := httptest.NewRecorder()

		servePACFile(w, req)

		// Should still return 200 with empty content
		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d for no PAC file configured, got %d", http.StatusOK, w.Code)
		}

		// Should have proper headers even with empty content
		expectedContentType := "application/x-ns-proxy-autoconfig"
		if ct := w.Header().Get("Content-Type"); ct != expectedContentType {
			t.Errorf("Expected Content-Type %q, got %q", expectedContentType, ct)
		}
	})

	t.Run("WPAD endpoint compatibility", func(t *testing.T) {
		tempDir := t.TempDir()
		pacFilePath := filepath.Join(tempDir, "test.pac")

		pacContent := `function FindProxyForURL(url, host) { return "PROXY localhost:3128"; }`
		err := os.WriteFile(pacFilePath, []byte(pacContent), 0644)
		if err != nil {
			t.Fatalf("Failed to create test PAC file: %v", err)
		}

		setApp(&App{
			PACFilePath: pacFilePath,
		})

		// Test WPAD endpoint
		req := httptest.NewRequest("GET", "/wpad.dat", nil)
		req.RemoteAddr = "127.0.0.1:12345"
		w := httptest.NewRecorder()

		handleHTTPRequest(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d for WPAD endpoint, got %d", http.StatusOK, w.Code)
		}

		body := w.Body.String()
		if !strings.Contains(body, "FindProxyForURL") {
			t.Errorf("WPAD endpoint should serve PAC content")
		}
	})

	t.Run("PAC file caching headers", func(t *testing.T) {
		tempDir := t.TempDir()
		pacFilePath := filepath.Join(tempDir, "test.pac")

		pacContent := `function FindProxyForURL(url, host) { return "DIRECT"; }`
		err := os.WriteFile(pacFilePath, []byte(pacContent), 0644)
		if err != nil {
			t.Fatalf("Failed to create test PAC file: %v", err)
		}

		setApp(&App{
			PACFilePath: pacFilePath,
		})

		req := httptest.NewRequest("GET", "/proxy.pac", nil)
		w := httptest.NewRecorder()

		servePACFile(w, req)

		// Check all cache-related headers
		headers := map[string]string{
			"Cache-Control": "no-cache, no-store, must-revalidate",
			"Pragma":        "no-cache",
			"Expires":       "0",
		}

		for header, expected := range headers {
			if actual := w.Header().Get(header); actual != expected {
				t.Errorf("Expected %s header %q, got %q", header, expected, actual)
			}
		}

		// Check Content-Length header is set
		if cl := w.Header().Get("Content-Length"); cl == "" {
			t.Errorf("Content-Length header should be set")
		}
	})

	t.Run("PAC file with app not initialized", func(t *testing.T) {
		// Set app to nil to test error handling
		setApp(nil)

		req := httptest.NewRequest("GET", "/proxy.pac", nil)
		req.RemoteAddr = "127.0.0.1:12345"
		w := httptest.NewRecorder()

		// This should handle gracefully when app is nil
		servePACFile(w, req)

		// Should return 200 with empty content when app is nil
		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d when app is nil, got %d", http.StatusOK, w.Code)
		}
	})

	t.Run("PAC file reading permissions", func(t *testing.T) {
		tempDir := t.TempDir()
		pacFilePath := filepath.Join(tempDir, "readonly.pac")

		pacContent := `function FindProxyForURL(url, host) { return "DIRECT"; }`
		err := os.WriteFile(pacFilePath, []byte(pacContent), 0644)
		if err != nil {
			t.Fatalf("Failed to create test PAC file: %v", err)
		}

		// Make file unreadable (on Unix systems)
		if err := os.Chmod(pacFilePath, 0000); err != nil {
			t.Skip("Cannot test file permissions on this system")
		}
		defer os.Chmod(pacFilePath, 0644) // Restore permissions for cleanup

		setApp(&App{
			PACFilePath: pacFilePath,
		})

		req := httptest.NewRequest("GET", "/proxy.pac", nil)
		w := httptest.NewRecorder()

		servePACFile(w, req)

		// Should return 404 for unreadable file
		if w.Code != http.StatusNotFound {
			t.Errorf("Expected status %d for unreadable file, got %d", http.StatusNotFound, w.Code)
		}
	})

	t.Run("PAC file content validation", func(t *testing.T) {
		tempDir := t.TempDir()
		pacFilePath := filepath.Join(tempDir, "complex.pac")

		// Create a more complex PAC file to test content handling
		pacContent := `function FindProxyForURL(url, host) {
    // Complex PAC logic with comments and multiple conditions
    host = host.toLowerCase();
    url = url.toLowerCase();
    
    // Local network detection
    if (isInNet(host, "192.168.0.0", "255.255.0.0") ||
        isInNet(host, "10.0.0.0", "255.0.0.0") ||
        isInNet(host, "172.16.0.0", "255.240.0.0")) {
        return "DIRECT";
    }
    
    // Specific domains through proxy
    var proxyDomains = [".palecci.cz", ".example.com", ".test.org"];
    for (var i = 0; i < proxyDomains.length; i++) {
        if (dnsDomainIs(host, proxyDomains[i])) {
            return "PROXY localhost:3128";
        }
    }
    
    // HTTPS URLs through proxy for security
    if (url.indexOf("https://") == 0 && host.indexOf("secure") != -1) {
        return "PROXY localhost:3128";
    }
    
    return "DIRECT";
}`

		err := os.WriteFile(pacFilePath, []byte(pacContent), 0644)
		if err != nil {
			t.Fatalf("Failed to create complex PAC file: %v", err)
		}

		setApp(&App{
			PACFilePath: pacFilePath,
		})

		req := httptest.NewRequest("GET", "/proxy.pac", nil)
		w := httptest.NewRecorder()

		servePACFile(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status %d for complex PAC file, got %d", http.StatusOK, w.Code)
		}

		body := w.Body.String()
		expectedContent := []string{
			"FindProxyForURL",
			"isInNet",
			"dnsDomainIs",
			"palecci.cz",
			"PROXY localhost:3128",
			"DIRECT",
		}

		for _, expected := range expectedContent {
			if !strings.Contains(body, expected) {
				t.Errorf("PAC file should contain %q", expected)
			}
		}
	})
}

// TestPACFileIntegration tests PAC file serving through HTTP requests
func TestPACFileIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PAC integration test in short mode")
	}

	// Create test PAC file
	tempDir := t.TempDir()
	pacFilePath := filepath.Join(tempDir, "integration.pac")

	pacContent := `function FindProxyForURL(url, host) {
    if (host == "integration.test") {
        return "PROXY localhost:3128";
    }
    return "DIRECT";
}`

	err := os.WriteFile(pacFilePath, []byte(pacContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create integration PAC file: %v", err)
	}

	// Create test server
	testApp := NewApp(
		func(w http.ResponseWriter, r *http.Request) {
			handleHTTPRequest(w, r)
		},
		0, // Let system assign port
		"localhost",
		9999,
		0, // No pool connections to avoid background goroutines
		0,
		false,
		false,
		"",
		"",
		22,
		false,
		"",
		"",
		pacFilePath, // Set PAC file path
	)

	// Find free port and start server
	server := httptest.NewServer(testApp)
	defer func() {
		server.Close()
		testApp.Stop() // Stop the app to cleanup background goroutines
	}()

	// Test PAC file endpoint
	resp, err := http.Get(server.URL + "/proxy.pac")
	if err != nil {
		t.Fatalf("Failed to request PAC file: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	// Check headers
	expectedContentType := "application/x-ns-proxy-autoconfig"
	if ct := resp.Header.Get("Content-Type"); ct != expectedContentType {
		t.Errorf("Expected Content-Type %q, got %q", expectedContentType, ct)
	}

	// Check content
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, "integration.test") {
		t.Errorf("PAC file should contain integration test domain")
	}

	// Test WPAD endpoint
	resp, err = http.Get(server.URL + "/wpad.dat")
	if err != nil {
		t.Fatalf("Failed to request WPAD file: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected WPAD status %d, got %d", http.StatusOK, resp.StatusCode)
	}

	t.Logf("✓ PAC file integration test passed")
	t.Logf("  Server URL: %s", server.URL)
	t.Logf("  PAC file served correctly at /proxy.pac and /wpad.dat")
}

// TestPACFilePerformance tests PAC file serving performance
func TestPACFilePerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping PAC performance test in short mode")
	}

	// Create large PAC file to test performance
	tempDir := t.TempDir()
	pacFilePath := filepath.Join(tempDir, "large.pac")

	var pacBuilder strings.Builder
	pacBuilder.WriteString(`function FindProxyForURL(url, host) {
    host = host.toLowerCase();
    
    // Large number of domain checks for performance testing
`)

	// Add many domain checks
	for i := 0; i < 100; i++ {
		pacBuilder.WriteString(fmt.Sprintf(`    if (dnsDomainIs(host, ".domain%d.com")) {
        return "PROXY localhost:3128";
    }
`, i))
	}

	pacBuilder.WriteString(`    
    return "DIRECT";
}`)

	err := os.WriteFile(pacFilePath, []byte(pacBuilder.String()), 0644)
	if err != nil {
		t.Fatalf("Failed to create large PAC file: %v", err)
	}

	// Save original app state
	originalApp := app
	defer func() { app = originalApp }()

	setApp(&App{
		PACFilePath: pacFilePath,
	})

	// Performance test - measure response time
	start := time.Now()

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/proxy.pac", nil)
		w := httptest.NewRecorder()
		servePACFile(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Request %d failed with status %d", i, w.Code)
		}
	}

	duration := time.Since(start)
	avgDuration := duration / 10

	// Should serve large PAC file reasonably quickly (under 10ms per request)
	if avgDuration > 10*time.Millisecond {
		t.Logf("Warning: PAC file serving took %v on average (expected < 10ms)", avgDuration)
	}

	t.Logf("✓ PAC file performance test completed")
	t.Logf("  Average response time: %v", avgDuration)
	t.Logf("  Total requests: 10")
}
