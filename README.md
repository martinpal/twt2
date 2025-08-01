# TW2 - Trans-Warp Tunnel Proxy

TW2 (Trans-Warp 2) is a high-performance HTTP proxy tunnel system written in Go that allows secure tunneling of HTTP traffic between two endpoints using Protocol Buffers for efficient communication.

TW2 implements HTTP CONNECT method tunneling, which is the standard method for proxying any TCP-based traffic through HTTP proxies. This makes it compatible with HTTPS, SSH, and any other protocol that needs to be tunneled through an HTTP proxy.

## Overview

TW2 consists of two main components that work together to create a secure tunnel:

1. **HTTP Proxy Server** - Accepts HTTP requests and forwards them through the tunnel
2. **Protocol Buffer Server** - Handles the actual tunneling communication between endpoints

The system uses connection pooling, sequence numbering, and message queuing to ensure reliable and efficient data transmission.

## Architecture

```
Client → HTTP Proxy → ProtoBuf Tunnel → Remote ProtoBuf Server → Target Server
```

### Key Features

- **HTTP CONNECT Method**: Implements standard HTTP CONNECT tunneling for universal protocol support
- **Connection Pooling**: Maintains a pool of persistent connections for efficient data transfer
- **Sequence Numbering**: Ensures ordered delivery of data packets
- **Message Queuing**: Handles out-of-order messages and connection reliability
- **Concurrent Processing**: Uses goroutines for handling multiple connections simultaneously
- **Configurable Logging**: Multiple log levels for debugging and monitoring
- **Connection Management**: Automatic cleanup and resource management

### Protocol Messages

The system uses Protocol Buffers with the following message types:
- `DATA_DOWN`: Downlink data transmission
- `DATA_UP`: Uplink data transmission  
- `OPEN_CONN`: Open new remote connection
- `CLOSE_CONN_S`: Close remote server connection
- `CLOSE_CONN_C`: Close local client connection
- `PING`: Keep-alive messages

## Project Structure

```
├── main/           # Main executable package
│   ├── main.go     # Application entry point
│   ├── go.mod      # Module dependencies
│   └── go.sum      # Dependency checksums
├── twt2/           # Core proxy library
│   ├── twt2.go     # Main proxy implementation
│   ├── twt2_test.go # Unit tests
│   ├── go.mod      # Module dependencies
│   └── go.sum      # Dependency checksums
└── twtproto/       # Protocol Buffer definitions
    ├── twt.proto   # Protocol Buffer schema
    ├── twt.pb.go   # Generated Go code
    ├── go.mod      # Module dependencies
    └── go.sum      # Dependency checksums
```

## Dependencies

- **Go 1.13+** - Programming language
- **Protocol Buffers** - Message serialization
- **github.com/golang/protobuf** - Go protobuf support
- **github.com/silenceper/pool** - Connection pooling
- **github.com/sirupsen/logrus** - Structured logging

## Building

### Prerequisites

1. Install Go 1.13 or later
2. Install Protocol Buffer compiler (protoc) if modifying `.proto` files

### Build Instructions

```bash
# Clone the repository
git clone <repository-url>
cd twt2

# Build the main executable
cd main
go build -o tw2
```

### Building All Modules

```bash
# Build twt2 library
cd twt2
go build

# Build twtproto library  
cd ../twtproto
go build

# Build main executable
cd ../main
go build -o tw2
```

## Usage

### Command Line Options

```bash
./tw2 [options]

Options:
  -L int     Log level: (1) Error, (2) Warn, (3) Info, (4) Debug, (5) Trace (default 2)
  -l int     HTTP proxy port to listen on (default 3128)
  -h string  Address of the peer host (default "127.0.0.1")
  -p int     Port of the peer on peer host (default 33333)
  -b int     ProtoBuf port to listen on (default 33333)
  -i int     Initial size of connection pool (default 100)
  -c int     Maximum size of connection pool (default 500)
  -ping      Enable ping on pool connections (default false)
```

### Basic Usage Examples

```bash
# On the tunnel server (remote end)
./tw2 -L 2 -b 33333 -l 3128

# On the client end  
./tw2 -L 2 -h remote-server.com -p 33333 -l 3128 -b 33334
```

## Monitoring

The application includes built-in monitoring and statistics:

- **HTTP Profiling**: Available at `localhost:6060` for performance analysis
- **Connection Statistics**: Logged every 5 seconds showing pool and connection counts
- **Detailed Logging**: Multiple log levels for troubleshooting

### Log Levels

1. **Error**: Only critical errors
2. **Warn**: Warnings and errors (default)
3. **Info**: General information, warnings, and errors
4. **Debug**: Detailed debugging information
5. **Trace**: Very verbose tracing (including hex dumps)

## Development

### Running Tests

```bash
cd twt2
go test -v
```

### Regenerating Protocol Buffers

If you modify `twt.proto`:

```bash
cd twtproto
protoc --go_out=. --go_opt=paths=source_relative twt.proto
```

### Code Structure

- **App**: Main application structure managing connections and pools
- **Connection**: Represents individual tunneled connections with sequence tracking
- **Handler**: HTTP request handler interface
- **Message Processing**: Protocol buffer message handling and queuing

## Security Considerations

- The tunnel does not provide encryption by default.
- All data is forwarded without any additional encryption. The clients are responsible for securing their own connections (TLS/SSL, SSH).
- There is no built-in authentication.

## Performance Tuning

- Adjust pool sizes (`-i`, `-c`) based on expected concurrent connections
- Enable ping (`-ping`) for long-lived connections in unstable networks
- Use appropriate log levels in production (level 2 or lower)
- Monitor connection statistics for optimal pool sizing

## Troubleshooting

### Common Issues

1. **Connection Pool Exhaustion**: Increase `-c` parameter
2. **High Memory Usage**: Reduce pool sizes or enable ping
3. **Connection Drops**: Enable ping and check network stability
4. **Performance Issues**: Increase initial pool size with `-i`

### Debug Information

Use log level 4 or 5 to see detailed connection and message information:

```bash
./tw2 -L 4  # Debug level
./tw2 -L 5  # Trace level (very verbose)
```

## License

This project is licensed under the GPL-3.0 License. See the [LICENSE](LICENSE) file for details.

## Authors

Martin Palecek <martin.palecek@ketry.net>
