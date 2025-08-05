#!/bin/bash

# Advanced build script for TW2 with embedded version information

set -e

# Default values
STATIC=false
OUTPUT_NAME="tw2"
VERBOSE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --static)
            STATIC=true
            shift
            ;;
        --output|-o)
            OUTPUT_NAME="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --static         Build static binary (CGO_ENABLED=0)"
            echo "  --output, -o     Output binary name (default: tw2)"
            echo "  --verbose, -v    Verbose output"
            echo "  --help, -h       Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Get git information
if git rev-parse --git-dir > /dev/null 2>&1; then
    COMMIT_HASH=$(git rev-parse --short HEAD)
    COMMIT_FULL=$(git rev-parse HEAD)
    GIT_TAG=$(git describe --tags --exact-match 2>/dev/null || echo "")
    GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
    GIT_DIRTY=""
    if ! git diff-index --quiet HEAD --; then
        GIT_DIRTY="-dirty"
    fi
else
    COMMIT_HASH="unknown"
    COMMIT_FULL="unknown"
    GIT_TAG=""
    GIT_BRANCH="unknown"
    GIT_DIRTY=""
fi

# Get build timestamp
BUILD_TIME=$(date -u +"%Y-%m-%d %H:%M:%S UTC")
BUILD_USER=$(whoami)
BUILD_HOST=$(hostname)

# Create version string
if [ -n "$GIT_TAG" ]; then
    VERSION="${GIT_TAG}"
else
    VERSION="${COMMIT_HASH}${GIT_DIRTY}"
fi

# Create ldflags for embedding version info
LDFLAGS="-X 'main.commitHash=${COMMIT_HASH}${GIT_DIRTY}' -X 'main.buildTime=${BUILD_TIME}'"

# Add more build info if verbose
if [ "$VERBOSE" = true ]; then
    LDFLAGS="${LDFLAGS} -X 'main.gitBranch=${GIT_BRANCH}' -X 'main.buildUser=${BUILD_USER}' -X 'main.buildHost=${BUILD_HOST}'"
fi

echo "Building TW2 with:"
echo "  Version:     ${VERSION}"
echo "  Commit Hash: ${COMMIT_HASH}${GIT_DIRTY}"
if [ "$VERBOSE" = true ]; then
    echo "  Full Hash:   ${COMMIT_FULL}"
    echo "  Branch:      ${GIT_BRANCH}"
    echo "  Tag:         ${GIT_TAG:-"(none)"}"
fi
echo "  Build Time:  ${BUILD_TIME}"
if [ "$VERBOSE" = true ]; then
    echo "  Build User:  ${BUILD_USER}"
    echo "  Build Host:  ${BUILD_HOST}"
fi
echo "  Static:      ${STATIC}"
echo "  Output:      ${OUTPUT_NAME}"
echo ""

# Change to main directory
cd main

# Set up build environment
if [ "$STATIC" = true ]; then
    export CGO_ENABLED=0
    echo "Building static binary (CGO_ENABLED=0)..."
else
    echo "Building dynamic binary..."
fi

# Build the binary
echo "Building..."
if [ "$VERBOSE" = true ]; then
    echo "Command: go build -ldflags \"${LDFLAGS}\" -o ${OUTPUT_NAME}"
fi

go build -ldflags "${LDFLAGS}" -o "${OUTPUT_NAME}"

echo "Build completed successfully!"
echo ""
echo "Binary info:"
ls -lh "${OUTPUT_NAME}"
echo ""
echo "To see version information:"
echo "  ./${OUTPUT_NAME} -version"
