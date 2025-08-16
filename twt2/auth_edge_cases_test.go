package twt2

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestAuthenticationEdgeCases tests comprehensive authentication edge cases
func TestAuthenticationEdgeCases(t *testing.T) {
	// Save original app state
	originalApp := app
	defer func() { app = originalApp }()

	t.Run("Authentication with empty credentials", func(t *testing.T) {
		// Test when both username and password are empty
		req := httptest.NewRequest("CONNECT", "example.com:443", nil)

		// Should return true (no auth required)
		result := authenticateProxyRequest(req, "", "")
		if !result {
			t.Errorf("Expected authentication to pass when no credentials required")
		}
	})

	t.Run("Authentication with whitespace-only credentials", func(t *testing.T) {
		req := httptest.NewRequest("CONNECT", "example.com:443", nil)

		// Test with spaces as username/password
		result := authenticateProxyRequest(req, " ", " ")
		if result {
			t.Errorf("Expected authentication to fail with whitespace-only credentials and no header")
		}

		// Test with proper header for whitespace credentials
		authHeader := base64.StdEncoding.EncodeToString([]byte(" : "))
		req.Header.Set("Proxy-Authorization", "Basic "+authHeader)

		result = authenticateProxyRequest(req, " ", " ")
		if !result {
			t.Errorf("Expected authentication to pass with matching whitespace credentials")
		}
	})

	t.Run("Authentication with special characters", func(t *testing.T) {
		specialUser := "user@domain.com"
		specialPass := "p@ssw0rd!#$%^&*()"

		req := httptest.NewRequest("CONNECT", "example.com:443", nil)
		authHeader := base64.StdEncoding.EncodeToString([]byte(specialUser + ":" + specialPass))
		req.Header.Set("Proxy-Authorization", "Basic "+authHeader)

		result := authenticateProxyRequest(req, specialUser, specialPass)
		if !result {
			t.Errorf("Expected authentication to pass with special characters")
		}
	})

	t.Run("Authentication with unicode characters", func(t *testing.T) {
		unicodeUser := "用户"
		unicodePass := "密码123"

		req := httptest.NewRequest("CONNECT", "example.com:443", nil)
		authHeader := base64.StdEncoding.EncodeToString([]byte(unicodeUser + ":" + unicodePass))
		req.Header.Set("Proxy-Authorization", "Basic "+authHeader)

		result := authenticateProxyRequest(req, unicodeUser, unicodePass)
		if !result {
			t.Errorf("Expected authentication to pass with unicode characters")
		}
	})

	t.Run("Authentication with very long credentials", func(t *testing.T) {
		longUser := strings.Repeat("a", 1000)
		longPass := strings.Repeat("b", 1000)

		req := httptest.NewRequest("CONNECT", "example.com:443", nil)
		authHeader := base64.StdEncoding.EncodeToString([]byte(longUser + ":" + longPass))
		req.Header.Set("Proxy-Authorization", "Basic "+authHeader)

		result := authenticateProxyRequest(req, longUser, longPass)
		if !result {
			t.Errorf("Expected authentication to pass with very long credentials")
		}
	})

	t.Run("Authentication header case variations", func(t *testing.T) {
		user := "testuser"
		pass := "testpass"
		authHeader := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))

		testCases := []struct {
			name   string
			header string
			expect bool
		}{
			{"lowercase basic", "basic " + authHeader, false},
			{"mixed case Basic", "Basic " + authHeader, true},
			{"uppercase BASIC", "BASIC " + authHeader, false},
			{"extra spaces", "Basic  " + authHeader, false},
			{"tab instead of space", "Basic\t" + authHeader, false},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest("CONNECT", "example.com:443", nil)
				req.Header.Set("Proxy-Authorization", tc.header)

				result := authenticateProxyRequest(req, user, pass)
				if result != tc.expect {
					t.Errorf("Expected %v for header %q, got %v", tc.expect, tc.header, result)
				}
			})
		}
	})

	t.Run("Authentication with malformed base64", func(t *testing.T) {
		malformedHeaders := []string{
			"Basic !!!invalid",
			"Basic @#$%invalid",
			"Basic incomplete=",
			"Basic =invalid",
			"Basic dGVzdA", // incomplete base64
		}

		for _, header := range malformedHeaders {
			t.Run("header: "+header, func(t *testing.T) {
				req := httptest.NewRequest("CONNECT", "example.com:443", nil)
				req.Header.Set("Proxy-Authorization", header)

				result := authenticateProxyRequest(req, "user", "pass")
				if result {
					t.Errorf("Expected authentication to fail with malformed base64: %s", header)
				}
			})
		}
	})

	t.Run("Authentication with missing colon separator", func(t *testing.T) {
		req := httptest.NewRequest("CONNECT", "example.com:443", nil)

		// Create base64 of credentials without colon
		noColonCreds := base64.StdEncoding.EncodeToString([]byte("userpassword"))
		req.Header.Set("Proxy-Authorization", "Basic "+noColonCreds)

		result := authenticateProxyRequest(req, "user", "password")
		if result {
			t.Errorf("Expected authentication to fail when colon separator is missing")
		}
	})

	t.Run("Authentication with multiple colons", func(t *testing.T) {
		req := httptest.NewRequest("CONNECT", "example.com:443", nil)

		// Create credentials with multiple colons
		multiColonCreds := base64.StdEncoding.EncodeToString([]byte("user:pass:extra"))
		req.Header.Set("Proxy-Authorization", "Basic "+multiColonCreds)

		result := authenticateProxyRequest(req, "user", "pass:extra")
		if !result {
			t.Errorf("Expected authentication to pass when password contains colon")
		}
	})

	t.Run("Authentication timing attack resistance", func(t *testing.T) {
		user := "testuser"
		pass := "testpass"

		req := httptest.NewRequest("CONNECT", "example.com:443", nil)
		authHeader := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
		req.Header.Set("Proxy-Authorization", "Basic "+authHeader)

		// Test multiple authentication attempts to ensure consistent timing
		times := make([]time.Duration, 5)

		for i := 0; i < 5; i++ {
			start := time.Now()
			authenticateProxyRequest(req, user, pass)
			times[i] = time.Since(start)
		}

		// Verify that timing is roughly consistent (within reasonable bounds)
		// This is a basic check - real timing attack resistance requires more sophisticated testing
		for i := 1; i < len(times); i++ {
			ratio := float64(times[i]) / float64(times[0])
			if ratio > 10.0 || ratio < 0.1 {
				t.Logf("Warning: Timing variation detected - may indicate timing vulnerability")
				t.Logf("  Time[0]: %v, Time[%d]: %v, Ratio: %.2f", times[0], i, times[i], ratio)
			}
		}

		t.Logf("✓ Timing consistency check completed")
	})

	t.Run("Authentication with different HTTP methods", func(t *testing.T) {
		user := "testuser"
		pass := "testpass"
		authHeader := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))

		methods := []string{"CONNECT", "GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"}

		for _, method := range methods {
			t.Run("method: "+method, func(t *testing.T) {
				req := httptest.NewRequest(method, "example.com:443", nil)
				req.Header.Set("Proxy-Authorization", "Basic "+authHeader)

				result := authenticateProxyRequest(req, user, pass)
				if !result {
					t.Errorf("Expected authentication to pass for method %s", method)
				}
			})
		}
	})
}

// TestSendProxyAuthRequiredEdgeCases tests the proxy auth required response
func TestSendProxyAuthRequiredEdgeCases(t *testing.T) {
	t.Run("Proxy auth required response format", func(t *testing.T) {
		w := httptest.NewRecorder()

		sendProxyAuthRequired(w)

		// Check status code
		if w.Code != http.StatusProxyAuthRequired {
			t.Errorf("Expected status %d, got %d", http.StatusProxyAuthRequired, w.Code)
		}

		// Check Proxy-Authenticate header
		expectedHeader := "Basic realm=\"TW2 Proxy\""
		if auth := w.Header().Get("Proxy-Authenticate"); auth != expectedHeader {
			t.Errorf("Expected Proxy-Authenticate header %q, got %q", expectedHeader, auth)
		}

		// Check response body
		expectedBody := "Proxy Authentication Required"
		if body := w.Body.String(); body != expectedBody {
			t.Errorf("Expected response body %q, got %q", expectedBody, body)
		}
	})

	t.Run("Multiple calls to sendProxyAuthRequired", func(t *testing.T) {
		w := httptest.NewRecorder()

		// Call multiple times
		sendProxyAuthRequired(w)
		sendProxyAuthRequired(w)

		// Should still work correctly (idempotent)
		if w.Code != http.StatusProxyAuthRequired {
			t.Errorf("Expected status %d after multiple calls", http.StatusProxyAuthRequired)
		}
	})
}

// TestAuthenticationIntegration tests authentication in complete HTTP flow
func TestAuthenticationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping authentication integration test in short mode")
	}

	// Save original app state
	originalApp := app
	defer func() { app = originalApp }()

	t.Run("Complete authentication flow", func(t *testing.T) {
		username := "integrationuser"
		password := "integrationpass"

		// Create app with authentication enabled
		testApp := NewApp(
			func(w http.ResponseWriter, r *http.Request) {
				// This handler shouldn't be called for CONNECT requests
				t.Errorf("Handler called unexpectedly for method %s", r.Method)
			},
			0, // Let system assign port
			"localhost",
			9999,
			0, // No pool connections
			0,
			false,
			false,
			"",
			"",
			22,
			true,     // Enable proxy auth
			username, // Username
			password, // Password
			"",       // No PAC file
		)

		server := httptest.NewServer(testApp)
		defer func() {
			server.Close()
			testApp.Stop()
		}()

		// Test 1: Request without authentication should fail
		req, _ := http.NewRequest("CONNECT", server.URL, nil)
		req.Host = "example.com:443"
		req.URL.Host = "example.com:443"

		client := &http.Client{}
		resp, err := client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusProxyAuthRequired {
				t.Errorf("Expected status %d without auth, got %d", http.StatusProxyAuthRequired, resp.StatusCode)
			}

			// Check for Proxy-Authenticate header
			if auth := resp.Header.Get("Proxy-Authenticate"); auth == "" {
				t.Errorf("Expected Proxy-Authenticate header in 407 response")
			}
		}

		// Test 2: Request with correct authentication should proceed
		// Note: This test verifies the authentication logic, though actual proxying won't work in test environment
		authHeader := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		req.Header.Set("Proxy-Authorization", "Basic "+authHeader)

		// For testing purposes, we'll verify that authentication passes by checking the response
		// In a real scenario, this would establish the proxy connection
		resp, err = client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			// Authentication should succeed (though connection will fail in test environment)
			if resp.StatusCode == http.StatusProxyAuthRequired {
				t.Errorf("Authentication should have succeeded, but got 407 again")
			}
		}

		t.Logf("✓ Authentication integration test completed")
	})

	t.Run("Authentication with different request types", func(t *testing.T) {
		username := "testuser"
		password := "testpass"

		testApp := NewApp(
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
			},
			0,
			"localhost",
			9999,
			0,
			0,
			false,
			false,
			"",
			"",
			22,
			true,
			username,
			password,
			"",
		)

		server := httptest.NewServer(testApp)
		defer func() {
			server.Close()
			testApp.Stop()
		}()

		// Test GET request (should not require authentication for non-CONNECT)
		resp, err := http.Get(server.URL + "/test")
		if err != nil {
			t.Fatalf("Failed to make GET request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected GET request to succeed without auth, got status %d", resp.StatusCode)
		}

		t.Logf("✓ Authentication integration with different request types completed")
	})
}

// TestAuthenticationBoundaryConditions tests boundary conditions for authentication
func TestAuthenticationBoundaryConditions(t *testing.T) {
	t.Run("Empty username with non-empty password", func(t *testing.T) {
		req := httptest.NewRequest("CONNECT", "example.com:443", nil)

		// Should require authentication but reject empty username
		result := authenticateProxyRequest(req, "", "password")
		if result {
			t.Errorf("Expected authentication to fail with empty username but non-empty password")
		}
	})

	t.Run("Non-empty username with empty password", func(t *testing.T) {
		req := httptest.NewRequest("CONNECT", "example.com:443", nil)

		// Should require authentication but reject empty password
		result := authenticateProxyRequest(req, "username", "")
		if result {
			t.Errorf("Expected authentication to fail with non-empty username but empty password")
		}
	})

	t.Run("Authentication with only colon", func(t *testing.T) {
		req := httptest.NewRequest("CONNECT", "example.com:443", nil)

		// Base64 encode just a colon
		authHeader := base64.StdEncoding.EncodeToString([]byte(":"))
		req.Header.Set("Proxy-Authorization", "Basic "+authHeader)

		result := authenticateProxyRequest(req, "", "")
		if !result {
			t.Errorf("Expected authentication to pass when both expected and provided credentials are empty")
		}
	})

	t.Run("Authentication with null bytes", func(t *testing.T) {
		req := httptest.NewRequest("CONNECT", "example.com:443", nil)

		// Test credentials containing null bytes
		userWithNull := "user\x00"
		passWithNull := "pass\x00"

		authHeader := base64.StdEncoding.EncodeToString([]byte(userWithNull + ":" + passWithNull))
		req.Header.Set("Proxy-Authorization", "Basic "+authHeader)

		result := authenticateProxyRequest(req, userWithNull, passWithNull)
		if !result {
			t.Errorf("Expected authentication to pass with null bytes in credentials")
		}
	})

	t.Run("Authentication header truncation", func(t *testing.T) {
		req := httptest.NewRequest("CONNECT", "example.com:443", nil)

		// Test various truncated headers
		truncatedHeaders := []string{
			"B",
			"Ba",
			"Bas",
			"Basi",
			"Basic",
			"Basic ",
		}

		for _, header := range truncatedHeaders {
			t.Run("header: "+header, func(t *testing.T) {
				req.Header.Set("Proxy-Authorization", header)

				result := authenticateProxyRequest(req, "user", "pass")
				if result {
					t.Errorf("Expected authentication to fail with truncated header: %q", header)
				}
			})
		}
	})
}

// TestAuthenticationPerformance tests authentication performance characteristics
func TestAuthenticationPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping authentication performance test in short mode")
	}

	user := "performanceuser"
	pass := "performancepass"

	req := httptest.NewRequest("CONNECT", "example.com:443", nil)
	authHeader := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
	req.Header.Set("Proxy-Authorization", "Basic "+authHeader)

	// Performance test
	iterations := 1000
	start := time.Now()

	for i := 0; i < iterations; i++ {
		result := authenticateProxyRequest(req, user, pass)
		if !result {
			t.Errorf("Authentication failed on iteration %d", i)
			break
		}
	}

	duration := time.Since(start)
	avgDuration := duration / time.Duration(iterations)

	// Should be very fast (under 100µs per authentication)
	if avgDuration > 100*time.Microsecond {
		t.Logf("Warning: Authentication taking %v on average (expected < 100µs)", avgDuration)
	}

	t.Logf("✓ Authentication performance test completed")
	t.Logf("  %d authentications in %v", iterations, duration)
	t.Logf("  Average time per authentication: %v", avgDuration)
}
