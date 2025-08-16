package twt2

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
)

// TestMain sets up test environment with quiet logging
func TestMain(m *testing.M) {
	// Set log level to PANIC to suppress all log noise in tests (ERROR, WARN, INFO, DEBUG)
	logrus.SetLevel(logrus.PanicLevel)

	// Run tests
	result := m.Run()

	// Restore log level
	logrus.SetLevel(logrus.InfoLevel)

	// Exit with test result
	os.Exit(result)
}

// TestQuietLogging verifies that our test setup suppresses log output
func TestQuietLogging(t *testing.T) {
	// Verify that log level is PANIC (most restrictive)
	if logrus.GetLevel() != logrus.PanicLevel {
		t.Errorf("Expected log level PANIC in tests, got %v", logrus.GetLevel())
	}

	// These should not appear in output due to log level
	logrus.Debug("This debug message should not appear")
	logrus.Info("This info message should not appear")
	logrus.Warn("This warning should not appear")
	logrus.Error("This error message should not appear")

	t.Logf("✓ Log level properly set to PANIC for completely quiet tests")
}
