# TW2 Development Roadmap & TODO

## 🎯 Current State Assessment

### ✅ What's Working Well
- **Core tunneling functionality** - SSH tunneling with HTTP proxy
- **Robust connection management** - ACK-based reliable message delivery  
- **Security** - SSH key auth, independent sessions, proxy authentication
- **Monitoring** - Comprehensive Prometheus metrics (79% test coverage!)
- **Connection pooling** - Advanced pool management with health monitoring
- **PAC file support** - Automatic proxy configuration

### 📊 Recent Achievements
- Memory management fixes (explicit buffer cleanup)
- Authentication system improvements 
- Reliable ACK protocol implementation
- Connection health monitoring
- Comprehensive test suite

---

## 🚀 Development Phases

### Phase 1: Quality & Testing (2-3 weeks) 🧪

#### A. Test Infrastructure Improvements (High Impact)
- [ ] **Fix test noise** - Mock SSH connections in tests to eliminate connection retry logs
- [ ] **Clean test output** - Add log level control specifically for tests
- [ ] **Integration test suite** - End-to-end testing with real proxy scenarios
- [ ] **Load testing** - Performance testing under various connection loads
- [ ] **Benchmark tests** - Connection pool performance, message throughput

#### B. Test Coverage Gaps
- [ ] PAC file functionality testing
- [ ] Authentication edge cases
- [ ] SSH connection failure scenarios  
- [ ] Protocol buffer edge cases
- [ ] Error handling paths

#### C. Testing Goals
```bash
# Current: 79% coverage with noisy output
# Target: 85%+ coverage with clean, fast tests
```

### Phase 2: Refactoring (1-2 weeks) 🔧

#### A. Code Organization
- [ ] **Extract interfaces** - Create abstractions for SSH, connection management
- [ ] **Separate concerns** - Split `twt2.go` (2366 lines!) into focused modules
- [ ] **Configuration management** - Centralized config structure vs scattered flags
- [ ] **Error handling** - Consistent error types and handling patterns

#### B. Suggested Module Structure
```
twt2/
├── connection/     # Connection pool management
├── ssh/           # SSH tunnel handling  
├── proxy/         # HTTP proxy logic
├── auth/          # Authentication handling
├── protocol/      # Protocol buffer message handling
├── metrics/       # Prometheus metrics
├── config/        # Configuration management
└── core/          # Main application logic
```

#### C. Refactoring Priorities
- [ ] Extract connection pool logic from main file
- [ ] Create SSH tunnel abstraction interface
- [ ] Centralize configuration validation
- [ ] Standardize error handling patterns

### Phase 3: Features (3-4 weeks) 🚀

#### A. Immediate Wins (High Value)
- [ ] **Configuration file support** - YAML/JSON config files vs command-line only
- [ ] **Graceful shutdown** - Proper cleanup on SIGTERM/SIGINT
- [ ] **Health check endpoint** - HTTP endpoint for load balancer health checks
- [ ] **Connection statistics API** - REST API for real-time connection stats

#### B. Advanced Features
- [ ] **Load balancing** - Multiple remote servers support
- [ ] **Failover mechanism** - Automatic failover between remote servers  
- [ ] **Bandwidth limiting** - Rate limiting per connection or globally
- [ ] **Connection prioritization** - QoS for different connection types

#### C. Feature Details

##### Configuration File Support
```yaml
# tw2.yaml example
server:
  mode: client
  listen_port: 33333
  proxy_port: 3128

ssh:
  host: remote-server.com
  user: tunneluser
  key_path: ~/.ssh/id_rsa
  port: 22

pool:
  initial_size: 100
  max_size: 500
  ping_enabled: true

auth:
  enabled: true
  username: proxyuser
  password: securepass

logging:
  level: info
  format: json
```

##### Health Check Endpoint
```http
GET /health
{
  "status": "healthy",
  "connections": {
    "pool_total": 100,
    "pool_healthy": 98,
    "pool_in_use": 15,
    "local_connections": 5,
    "remote_connections": 3
  },
  "uptime": "2h15m30s"
}
```

### Phase 4: Operational Improvements (4+ weeks) 🛠️

#### A. Observability
- [ ] **Structured logging** - JSON logging for better parsing
- [ ] **Tracing support** - OpenTelemetry/Jaeger integration
- [ ] **Dashboard** - Grafana dashboard for metrics visualization
- [ ] **Alerting** - Prometheus alerting rules for connection issues

#### B. Deployment & Operations
- [ ] **Containerization** - Docker images for easy deployment
- [ ] **systemd service** - Service files for production deployment
- [ ] **Configuration validation** - Startup validation of all configuration
- [ ] **Operations documentation** - Deployment and monitoring guide

#### C. Security Enhancements
- [ ] **Certificate validation** - Strict SSH host key checking options
- [ ] **Audit logging** - Security event logging
- [ ] **Rate limiting** - Protection against abuse
- [ ] **Connection limits** - Per-client connection limiting

---

## 💡 Quick Wins (Can Start Today)

### 1. Fix Test Logging Noise
```go
// Add to test setup
func init() {
    log.SetLevel(log.ErrorLevel) // Quiet tests
}
```

### 2. Add Health Endpoint
```go
// Add to HTTP handler
func handleHealthCheck(w http.ResponseWriter, r *http.Request) {
    if r.URL.Path == "/health" {
        // Return JSON health status
    }
}
```

### 3. Configuration Validation
```go
// Add to main()
func validateConfig() error {
    // Check SSH key exists
    // Validate ports are available
    // Verify SSH connectivity
}
```

### 4. Graceful Shutdown
```go
// Add signal handling
c := make(chan os.Signal, 1)
signal.Notify(c, os.Interrupt, syscall.SIGTERM)
go func() {
    <-c
    // Clean shutdown logic
}()
```

---

## 🎖️ Recommendations

### **Start Here: Phase 1 (Testing)**
You have solid functionality but need confidence in stability. Clean, fast tests will enable rapid development of new features without fear of regressions.

### **Current Priorities**
1. **Fix test noise** - Mock SSH connections to eliminate retry logs
2. **Integration tests** - End-to-end proxy scenarios
3. **Load testing** - Performance validation
4. **Health endpoints** - Operational visibility

### **Success Metrics**
- Test suite runs in <30 seconds with clean output
- 85%+ test coverage maintained
- Zero flaky tests
- Clear performance baselines established

---

## 📋 Issue Tracking

### Critical Issues
- [ ] Test output noise makes CI/CD difficult
- [ ] Large single file (twt2.go - 2366 lines) hard to maintain
- [ ] No graceful shutdown handling
- [ ] Configuration scattered across command line flags

### Enhancement Requests  
- [ ] Configuration file support
- [ ] Multiple server load balancing
- [ ] REST API for statistics
- [ ] Docker containerization
- [ ] Grafana dashboard templates

### Technical Debt
- [ ] Extract connection pool into separate package
- [ ] Create SSH tunnel interface abstraction  
- [ ] Standardize error handling patterns
- [ ] Add structured logging with context

---

## 🔄 Review Schedule

- **Weekly**: Progress review and priority adjustment
- **Bi-weekly**: Architecture and design reviews  
- **Monthly**: Performance and security assessment
- **Quarterly**: Roadmap and feature planning

---

*Last Updated: August 16, 2025*
*Current Version: Based on commit a2e7c38*
