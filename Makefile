# Makefile for TW2 (Two-Way Tunnel)

# Variables
BINARY_NAME := tw2
MAIN_DIR := main
TWLIB_DIR := twt2
PROTO_DIR := twtproto
OUTPUT_DIR := $(MAIN_DIR)

# Go build flags
GO_BUILD_FLAGS := -v
GO_TEST_FLAGS := -v -race -cover

# Git information for version embedding
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GIT_DIRTY := $(shell git diff-index --quiet HEAD -- 2>/dev/null || echo "-dirty")
BUILD_TIME := $(shell date -u +"%Y-%m-%d %H:%M:%S UTC")
VERSION := $(GIT_COMMIT)$(GIT_DIRTY)

# LDFLAGS for embedding version info
LDFLAGS := -X 'main.commitHash=$(VERSION)' -X 'main.buildTime=$(BUILD_TIME)'

# Build environment
CGO_ENABLED := 1

.PHONY: all build static test clean help

# Default target
all: build static

# Build dynamic binary
build:
	@echo "Building $(BINARY_NAME) (dynamic)..."
	@echo "  Version: $(VERSION)"
	@echo "  Build Time: $(BUILD_TIME)"
	@cd $(MAIN_DIR) && \
		CGO_ENABLED=$(CGO_ENABLED) go build $(GO_BUILD_FLAGS) \
		-ldflags "$(LDFLAGS)" \
		-o $(BINARY_NAME)
	@echo "Build completed: $(MAIN_DIR)/$(BINARY_NAME)"
	@ls -lh $(MAIN_DIR)/$(BINARY_NAME)

# Build static binary
static:
	@echo "Building $(BINARY_NAME) (static)..."
	@echo "  Version: $(VERSION)"
	@echo "  Build Time: $(BUILD_TIME)"
	@cd $(MAIN_DIR) && \
		CGO_ENABLED=0 go build $(GO_BUILD_FLAGS) \
		-ldflags "$(LDFLAGS)" \
		-o $(BINARY_NAME)-static
	@echo "Static build completed: $(MAIN_DIR)/$(BINARY_NAME)-static"
	@ls -lh $(MAIN_DIR)/$(BINARY_NAME)-static

# Run tests
test:
	@echo "Running tests for twt2 library..."
	@cd $(TWLIB_DIR) && go test $(GO_TEST_FLAGS) ./...
	@echo ""
	@echo "Running tests for twtproto..."
	@cd $(PROTO_DIR) && go test $(GO_TEST_FLAGS) ./...
	@echo ""
	@echo "Running tests for main..."
	@cd $(MAIN_DIR) && go test $(GO_TEST_FLAGS) ./...
	@echo ""
	@echo "All tests completed successfully!"

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage report..."
	@cd $(TWLIB_DIR) && go test $(GO_TEST_FLAGS) -coverprofile=coverage.out ./...
	@cd $(TWLIB_DIR) && go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: $(TWLIB_DIR)/coverage.html"

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	@cd $(TWLIB_DIR) && go test -bench=. -benchmem ./...

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -f $(MAIN_DIR)/$(BINARY_NAME)
	@rm -f $(MAIN_DIR)/$(BINARY_NAME)-static
	@rm -f $(MAIN_DIR)/$(BINARY_NAME)-test
	@rm -f $(MAIN_DIR)/twt2main
	@rm -f $(TWLIB_DIR)/coverage.out
	@rm -f $(TWLIB_DIR)/coverage.html
	@echo "Clean completed!"

# Install dependencies
deps:
	@echo "Installing dependencies..."
	@cd $(MAIN_DIR) && go mod download
	@cd $(TWLIB_DIR) && go mod download
	@cd $(PROTO_DIR) && go mod download
	@echo "Dependencies installed!"

# Tidy go modules
tidy:
	@echo "Tidying go modules..."
	@cd $(MAIN_DIR) && go mod tidy
	@cd $(TWLIB_DIR) && go mod tidy
	@cd $(PROTO_DIR) && go mod tidy
	@echo "Go modules tidied!"

# Lint code
lint:
	@echo "Running go vet..."
	@cd $(MAIN_DIR) && go vet ./...
	@cd $(TWLIB_DIR) && go vet ./...
	@cd $(PROTO_DIR) && go vet ./...
	@echo "Linting completed!"

# Format code
fmt:
	@echo "Formatting code..."
	@cd $(MAIN_DIR) && go fmt ./...
	@cd $(TWLIB_DIR) && go fmt ./...
	@cd $(PROTO_DIR) && go fmt ./...
	@echo "Code formatted!"

# Check if binaries exist and show version
version:
	@if [ -f $(MAIN_DIR)/$(BINARY_NAME) ]; then \
		echo "Dynamic binary version:"; \
		$(MAIN_DIR)/$(BINARY_NAME) -version 2>/dev/null || echo "Version flag not supported"; \
	else \
		echo "Dynamic binary not found. Run 'make build' first."; \
	fi
	@if [ -f $(MAIN_DIR)/$(BINARY_NAME)-static ]; then \
		echo "Static binary version:"; \
		$(MAIN_DIR)/$(BINARY_NAME)-static -version 2>/dev/null || echo "Version flag not supported"; \
	else \
		echo "Static binary not found. Run 'make static' first."; \
	fi

# Development build (with race detector)
dev:
	@echo "Building $(BINARY_NAME) for development (with race detector)..."
	@cd $(MAIN_DIR) && \
		CGO_ENABLED=1 go build $(GO_BUILD_FLAGS) \
		-race \
		-ldflags "$(LDFLAGS)" \
		-o $(BINARY_NAME)-dev
	@echo "Development build completed: $(MAIN_DIR)/$(BINARY_NAME)-dev"

# Show help
help:
	@echo "TW2 Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  build         Build dynamic binary (default)"
	@echo "  static        Build static binary (CGO_ENABLED=0)"
	@echo "  test          Run all tests"
	@echo "  test-coverage Run tests with coverage report"
	@echo "  bench         Run benchmarks"
	@echo "  clean         Remove build artifacts"
	@echo "  deps          Install dependencies"
	@echo "  tidy          Tidy go modules"
	@echo "  lint          Run go vet"
	@echo "  fmt           Format code"
	@echo "  version       Show version of built binaries"
	@echo "  dev           Build development binary with race detector"
	@echo "  help          Show this help message"
	@echo ""
	@echo "Build information:"
	@echo "  Version: $(VERSION)"
	@echo "  Build Time: $(BUILD_TIME)"
