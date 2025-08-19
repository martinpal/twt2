# TW2 - Trans-Warp Tunnel Proxy

**High-Performance Connection Multiplexing Tunnel with SSH Transport**

TW2 (Trans-Warp 2) is a sophisticated HTTP proxy tunnel system that **multiplexes and demultiplexes multiple TCP connections through a pool of SSH tunnels**. This connection pooling architecture dramatically improves network utilization by aggregating bandwidth from multiple SSH connections, resulting in **significantly higher throughput and surprisingly lower latency** - even for interactive applications like SSH, RDP, and real-time protocols.

## Key Innovation: Connection Pool Multiplexing

🚀 **Primary Feature**: TW2's core advantage is its ability to **demux/mux TCP connections through a connection pool**, allowing multiple logical connections to share bandwidth across several SSH tunnels simultaneously.

### Performance Benefits

- **Higher Bandwidth Utilization**: Grabs a bigger percentage of available network bandwidth by pooling multiple SSH connections
- **Reduced Latency**: Even interactive applications (SSH, RDP, VNC) experience lower latency due to intelligent connection distribution
- **Improved Reliability**: Connection failures are transparent - traffic automatically reroutes through healthy pool connections
- **Optimal Resource Usage**: Automatically balances load across available SSH tunnels

### How It Works

```
Multiple Clients         CLIENT SIDE (DEMUX)   SSH POOL   SERVER SIDE (MUX)     Target Services
                         ┌─────────────────┐ ══════════► ┌─────────────────┐
Client A (SSH) ────────► │                 │ ══════════► │                 │ ───► SSH Server
Client B (RDP) ────────► │   TW2 Client    │ ══════════► │   TW2 Server    │ ───► RDP Server
Client C (HTTP) ───────► │ (Demultiplexer) │ ══════════► │  (Multiplexer)  │ ───► Web Server
Client D (VNC) ────────► │                 │ ══════════► │                 │ ───► VNC Server
                         └─────────────────┘ ══════════► └─────────────────┘
                              Multiple                         Multiple
                            connections                      connections
                              demuxed                           muxed
                                 ▼                                ▼
                        SSH Tunnel Pool             Individual Target Connections
```

All data is chunked and distributed across multiple SSH tunnels in the pool, with each logical connection's traffic load-balanced over the available tunnels for maximum bandwidth utilization and seamless failover.

## Overview

TW2 consists of two main components that work together to create a secure, multiplexed tunnel:

1. **HTTP Proxy Server** - Accepts HTTP CONNECT requests and multiplexes them through the SSH tunnel pool
2. **Protocol Buffer Server** - Handles demultiplexing and reliable delivery at the remote endpoint

The system uses connection pooling, sequence numbering, and message queuing to ensure reliable and efficient data transmission across multiple SSH channels.

## Architecture

```
Clients → HTTP Proxy → SSH Pool (Mux) → Remote SSH → ProtoBuf Server (Demux) → Target Servers
```

### Key Features

- **🎯 Connection Pool Multiplexing**: Multiple TCP connections share bandwidth across several SSH tunnels for maximum throughput
- **📈 Bandwidth Optimization**: Achieves higher network utilization than single-connection tunnels
- **⚡ Low Latency**: Intelligent load distribution reduces latency even for interactive protocols
- **🔒 SSH Tunneling**: All tunnel connections use independent SSH sessions for strong encryption and authentication
- **🔑 SSH Key Authentication**: Uses SSH public key authentication (no passwords)
- **🌐 HTTP CONNECT Method**: Implements standard HTTP CONNECT tunneling for universal protocol support
- **🔄 Automatic Failover**: Seamless connection rerouting when individual SSH tunnels fail
- **📊 Sequence Numbering**: Ensures ordered delivery of data packets across the connection pool
- **📨 Message Queuing**: Handles out-of-order messages and connection reliability
- **⚙️ Concurrent Processing**: Uses goroutines for handling multiple connections simultaneously
- **📝 Configurable Logging**: Multiple log levels for debugging and monitoring
- **🧹 Connection Management**: Automatic cleanup and resource management

### Protocol Messages

The system uses Protocol Buffers with the following message types:
- `DATA_DOWN`: Downlink data transmission (server to client)
- `DATA_UP`: Uplink data transmission (client to server)
- `ACK_DOWN`: Acknowledge reception of downlink data
- `ACK_UP`: Acknowledge reception of uplink data
- `OPEN_CONN`: Open new remote connection to target server
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

- **Go 1.23+** - Programming language
- **Protocol Buffers** - Message serialization
- **google.golang.org/protobuf** - Modern Go protobuf support
- **github.com/sirupsen/logrus** - Structured logging
- **golang.org/x/crypto** - SSH client implementation
- **github.com/stretchr/testify** - Testing framework (development only)

## Building

### Prerequisites

1. Install Go 1.23 or later
2. Install Protocol Buffer compiler (protoc) if modifying `.proto` files
3. **SSH Setup (Required)**:
   - SSH server running on the target host
   - SSH public key authentication configured
   - SSH client installed on the client machine
   - Valid SSH private key file accessible to TW2

### Build Instructions

#### Build

```bash
# Clone the repository
git clone <repository-url>
cd twt2

# Build both dynamic and static binaries (recommended)
make all

# Or build specific variants:
make build     # Dynamic binary with version info
make static    # Static binary (no CGO dependencies)
make dev       # Development build with race detector
```

#### Make Targets Available

```bash
# Build targets
make build         # Build dynamic binary
make static        # Build static binary
make dev           # Development build with race detector
make all           # Build both dynamic and static (default)

# Testing and quality
make test          # Run all tests
make test-coverage # Run tests with coverage report
make bench         # Run benchmarks
make lint          # Run go vet
make fmt           # Format code

# Maintenance
make clean         # Remove build artifacts
make deps          # Install dependencies
make tidy          # Tidy go modules
make version       # Show version of built binaries
make help          # Show all available targets
```

### Testing and Development

```bash
# Run all tests
make test

# Run tests with coverage report
make test-coverage

# Run benchmarks
make bench

# Development build with race detector
make dev

# Format and lint code
make fmt lint

# Clean build artifacts
make clean
```

## SSH Setup

### Server Side Setup

1. **Install SSH server** (if not already installed):
   ```bash
   # Ubuntu/Debian
   sudo apt-get install openssh-server

   # CentOS/RHEL
   sudo yum install openssh-server
   ```

2. **Configure SSH server** (`/etc/ssh/sshd_config`):
   ```
   PubkeyAuthentication yes
   AuthorizedKeysFile .ssh/authorized_keys
   PasswordAuthentication no  # Recommended for security
   ```

3. **Restart SSH service**:
   ```bash
   sudo systemctl restart sshd
   ```

### Client Side Setup

1. **Generate SSH key pair** (if you don't have one):
   ```bash
   ssh-keygen -t rsa -b 4096 -f ~/.ssh/tw2_key
   ```

2. **Copy public key to server**:
   ```bash
   ssh-copy-id -i ~/.ssh/tw2_key.pub user@remote-server.com
   ```

3. **Test SSH connection**:
   ```bash
   ssh -i ~/.ssh/tw2_key user@remote-server.com
   ```

### Security Notes

- TW2 disables SSH ControlMaster to ensure independent connections
- Each tunnel connection uses its own SSH process for isolation
- SSH StrictHostKeyChecking is disabled for automated connections
- Consider using dedicated SSH keys for TW2 tunnel connections

## Usage

### Command Line Options

```bash
./tw2 [options]

Options:
  -L int        Log level: (1) Error, (2) Warn, (3) Info, (4) Debug, (5) Trace (default 2)
  -l int        HTTP proxy port to listen on (client mode only) (default 3128)
  -h string     Address of the peer host (client mode only) (default "127.0.0.1")
  -p int        Port of the peer on peer host (client mode only) (default 33333)
  -b int        ProtoBuf port to listen on (default 33333)
  -i int        Initial size of connection pool (client mode only) (default 100)
  -c int        Maximum size of connection pool (client mode only) (default 500)
  -ping         Enable ping on pool connections (client mode only) (default false)
  -server       Run in server mode (only ProtoBuf server, no HTTP proxy) (default false)
  -ssh-user string   SSH username for tunnel connections (required in client mode)
  -ssh-key string    Path to SSH private key file (required in client mode)
  -proxy-user string Username for proxy authentication (client mode only)
  -proxy-pass string Password for proxy authentication (client mode only)
  -pac-file string   Path to PAC (Proxy Auto-Configuration) file (client mode only, optional)
  -version      Show version information and exit
```

### Version Information

```bash
# Show version, commit hash, and build information
./tw2 -version

# Version info is also displayed at startup
# Example output:
# TW2 (Trans-Warp Tunnel Proxy) - Commit: a1b2c3d, Built: 2025-08-06 10:30:45 UTC
```

### Basic Usage Examples

#### Server Mode (Remote End)
```bash
# Start the tunnel server (listens on loopback only for SSH)
./tw2 -server -L 2 -b 33333
```

#### Client Mode (Local End)
```bash
# Start the client with HTTP proxy and SSH tunnel
./tw2 -L 2 -h remote-server.com -p 33333 -l 3128 -b 33334 \
     -ssh-user tunneluser -ssh-key ~/.ssh/id_rsa
```

#### Single Host Testing
```bash
# Terminal 1: Start server
./tw2 -server -b 33333

# Terminal 2: Start client (requires SSH server running locally)
./tw2 -h 127.0.0.1 -p 33333 -l 3128 -b 33334 \
     -ssh-user $USER -ssh-key ~/.ssh/id_rsa
```

## TW2 PAC (Proxy Auto-Configuration) Support

TW2 now supports PAC files to automatically configure which websites should use the proxy and which should connect directly.

### Features

- **Automatic PAC file serving**: Serves PAC files via HTTP GET requests
- **Custom PAC files**: Load PAC configuration from disk via command-line option
- **Default PAC generation**: Automatically generates PAC content if no file is specified
- **Standard endpoints**: Supports both `/proxy.pac` and `/wpad.dat` endpoints
- **Proper HTTP headers**: Sets correct content-type and cache control headers

### Usage

#### Command Line Option

```bash
./tw2 -pac-file /path/to/proxy.pac [other options...]
```

#### PAC File Endpoints

Once TW2 is running, browsers can access the PAC file at:

- `http://localhost:3128/proxy.pac` (standard)
- `http://localhost:3128/wpad.dat` (Web Proxy Auto-Discovery)

#### Browser Configuration

##### Automatic Configuration
1. Open browser proxy settings
2. Select "Automatic proxy configuration"
3. Enter PAC URL: `http://localhost:3128/proxy.pac`
4. Save settings

##### Manual Browser Setup Examples

**Chrome/Chromium:**
```bash
google-chrome --proxy-pac-url=http://localhost:3128/proxy.pac
```

**Firefox:**
1. Go to Settings → Network Settings
2. Select "Automatic proxy configuration URL"
3. Enter: `http://localhost:3128/proxy.pac`

**curl with PAC:**
```bash
curl --proxy-header "Proxy-Connection: Keep-Alive" \
     --proxy-pac-url http://localhost:3128/proxy.pac \
     https://server.palecci.cz/
```

### Security Considerations

1. **PAC File Access**: PAC files are served without authentication to allow browser access
2. **Content Validation**: Validate PAC file syntax before deployment
3. **Network Exposure**: Consider firewall rules for PAC file access
4. **Cache Control**: PAC files are served with no-cache headers for immediate updates

### Troubleshooting

#### PAC File Not Loading
- Verify file path is correct and readable
- Check TW2 logs for file reading errors
- Test PAC endpoint directly with curl

#### Browser Not Using PAC
- Clear browser cache and restart
- Check browser proxy settings
- Verify PAC file syntax
- Test PAC logic in browser developer tools

#### Authentication Issues
- PAC file serving bypasses proxy authentication
- Actual proxy connections still require authentication
- Configure browser with proxy credentials separately

### Performance Considerations

- PAC files are read from disk on each request for dynamic updates
- Consider file system caching for high-traffic scenarios
- Complex PAC logic can impact browser performance
- Keep PAC rules simple and efficient

## TW2 Proxy Authentication

TW2 supports HTTP Basic Authentication for proxy access control.

### Features

- **HTTP Basic Authentication**: Industry-standard authentication mechanism
- **Secure Credential Comparison**: Uses constant-time comparison to prevent timing attacks
- **Client-side Only**: Authentication is only active in client mode (HTTP proxy)
- **Standard Compliance**: Follows RFC 7617 for HTTP Basic Authentication

### Usage

#### Enabling Authentication

To enable proxy authentication, provide both username and password when starting TW2 in client mode:

```bash
./tw2 -proxy-user myuser -proxy-pass mypassword [other options...]
```

#### Client Configuration

Clients connecting to your authenticated proxy need to provide credentials:

##### curl Example
```bash
curl --proxy-user myuser:mypassword --proxy http://localhost:3128 https://example.com
```

##### Browser Configuration
Most browsers allow you to configure authenticated proxies:
- URL: `http://localhost:3128`
- Username: `myuser`
- Password: `mypassword`

##### Programming Example (Python)
```python
import requests

proxies = {
    'http': 'http://myuser:mypassword@localhost:3128',
    'https': 'http://myuser:mypassword@localhost:3128'
}

response = requests.get('https://example.com', proxies=proxies)
```

### Command Line Options

- `-proxy-user <username>`: Username for proxy authentication (client mode only)
- `-proxy-pass <password>`: Password for proxy authentication (client mode only)

**Note**: Both options must be provided together. If only one is specified, TW2 will exit with an error.

### Security Considerations

1. **Credential Storage**: Avoid hardcoding credentials. Consider using environment variables or configuration files.

2. **Command Line Visibility**: Be aware that command-line arguments may be visible to other users on the system via `ps` command.

3. **Transport Security**: While proxy authentication is encrypted when used with HTTPS, consider additional security measures for sensitive environments.

4. **Strong Passwords**: Use strong, unique passwords for proxy authentication.

### Authentication Flow

1. Client sends HTTP CONNECT request to proxy
2. TW2 checks for `Proxy-Authorization: Basic <base64-credentials>` header
3. If authentication is enabled and credentials are missing/invalid:
   - Returns `407 Proxy Authentication Required`
   - Includes `Proxy-Authenticate: Basic realm="TW2 Proxy"` header
4. If credentials are valid, proxy connection proceeds normally

### Example Complete Setup

#### Server Side (Remote)
```bash
./tw2 -server -L 2 -b 33333
```

#### Client Side (Local) with Authentication
```bash
./tw2 -L 2 -h remote-server.com -p 33333 -l 3128 -b 33334 \
     -ssh-user tunneluser -ssh-key ~/.ssh/id_rsa \
     -proxy-user proxyuser -proxy-pass securepassword123
```

#### Client Application
```bash
curl --proxy-user proxyuser:securepassword123 \
     --proxy http://localhost:3128 \
     https://httpbin.org/ip
```

### Troubleshooting

#### Authentication Failed
- Check username and password are correct
- Ensure both `-proxy-user` and `-proxy-pass` are provided
- Verify client is sending proper `Proxy-Authorization` header

#### No Authentication Required
- If no credentials are configured, TW2 operates without authentication
- Authentication is ignored in server mode

#### Log Messages
- Successful authentication: `Proxy authentication successful for <host> from <ip>`
- Failed authentication: `Proxy authentication failed for <host> from <ip>`

## PAC (Proxy Auto-Configuration) Support

TW2 supports PAC files to automatically configure which websites should use the proxy and which should connect directly.

### Features
- **Automatic PAC serving**: Serves PAC files via HTTP GET requests at `/proxy.pac` and `/wpad.dat`
- **Custom PAC files**: Load configuration from disk via `-pac-file` option
- **Default PAC generation**: Automatically generates PAC content for `server.palecci.cz` if no file specified
- **Browser integration**: Standard PAC file format compatible with all major browsers

### Usage

#### Basic PAC Configuration
```bash
# With custom PAC file
./tw2 -pac-file /path/to/proxy.pac [other options...]

# PAC file URL for browsers: http://localhost:3128/proxy.pac
```

#### Default PAC Behavior
If no PAC file is specified, TW2 generates default configuration that:
- Routes `server.palecci.cz` and `*.palecci.cz` through the proxy
- Uses direct connection for all other addresses

#### Browser Configuration
1. Open browser proxy settings
2. Select "Automatic proxy configuration"
3. Enter PAC URL: `http://localhost:3128/proxy.pac`

## Monitoring

The application includes built-in monitoring and statistics:

- **HTTP Profiling**: Available at `localhost:6060` (client mode) or `localhost:6061` (server mode) for performance analysis
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

- **SSH Encryption**: All tunnel traffic is encrypted using SSH's strong encryption algorithms
- **SSH Key Authentication**: Only SSH public key authentication is supported (no passwords)
- **Independent SSH Sessions**: Each tunnel connection uses a separate SSH process to prevent session hijacking
- **Loopback Only**: Server side only listens on loopback interface (127.0.0.1)
- **No ControlMaster**: SSH ControlMaster is explicitly disabled to prevent connection sharing vulnerabilities
- **SSH Key Management**: Protect SSH private keys with appropriate file permissions (600)
- **Firewall**: Only SSH port (22) needs to be open on the server side

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
