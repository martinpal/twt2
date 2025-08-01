package main

import (
	"flag"
	"fmt"
	"net/http"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	twt2 "palecci.cz/twt2"
)

func goid() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		panic(fmt.Sprintf("cannot get goroutine id: %v", err))
	}
	return id
}

func setLogLevel(logLevel *int) {
	switch *logLevel {
	case 1:
		log.SetLevel(log.ErrorLevel)
	case 2:
		log.SetLevel(log.WarnLevel)
	case 3:
		log.SetLevel(log.InfoLevel)
	case 4:
		log.SetLevel(log.DebugLevel)
	case 5:
		log.SetLevel(log.TraceLevel)
	default:
		log.Fatalf("Invalid log level %d", *logLevel)
	}
}

func stats() {
	time.AfterFunc(5*time.Second, stats)
	if twt2.GetApp() != nil {
		log.Infof("Pool connections: %d", len(twt2.GetApp().PoolConnections))
		twt2.GetApp().LocalConnectionMutex.Lock()
		twt2.GetApp().RemoteConnectionMutex.Lock()
		log.Infof("Local connection: %4d, Remote connections: %4d", len(twt2.GetApp().LocalConnections), len(twt2.GetApp().RemoteConnections))
		twt2.GetApp().RemoteConnectionMutex.Unlock()
		twt2.GetApp().LocalConnectionMutex.Unlock()
	}
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	// log.SetFormatter(&log.JSONFormatter{})
	log.SetReportCaller(true)
	log.SetFormatter(&log.TextFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			filename := path.Base(f.File)
			return fmt.Sprintf("%s()", f.Function), fmt.Sprintf("%d %s:%4d", goid(), filename, f.Line)
		},
	})

	logLevel := flag.Int("L", 2, "Log level. (1) Error, (2) Warn, (3) Info, (4) Debug, (5) Trace")
	proxyport := flag.Int("l", 3128, "Our http proxy port to listen on")
	listenPort := flag.Int("b", 33333, "Our protobuf port to listen on")
	poolInit := flag.Int("i", 100, "Initial size of the connection pool between the ends of tunnel")
	poolCap := flag.Int("c", 500, "Cap of the connection pool size")
	pingPool := flag.Bool("ping", false, "To ping or not to ping on the connection pool connections")
	serverMode := flag.Bool("server", false, "Run in server mode (only ProtoBuf server, no HTTP proxy)")
	sshServer := flag.String("ssh", "", "SSH server specification in format user@host[:sshport] (client mode only, defaults to port 22)")
	sshKeyPath := flag.String("ssh-key", "", "Path to SSH private key file (client mode only)")
	flag.Parse()
	setLogLevel(logLevel)

	// Determine if this is client or server mode
	isClient := !*serverMode

	// Parse SSH server specification for client mode
	var sshUser, sshHost string
	var sshPortInt int
	if isClient {
		if *sshServer == "" || *sshKeyPath == "" {
			log.Fatal("Client mode requires both -ssh and -ssh-key parameters")
		}

		// Parse user@host:port format
		parts := strings.Split(*sshServer, "@")
		if len(parts) != 2 {
			log.Fatal("Invalid SSH server format. Expected: user@host[:port]")
		}
		sshUser = parts[0]

		hostPort := strings.Split(parts[1], ":")
		if len(hostPort) == 1 {
			// No port specified, use default SSH port 22
			sshHost = hostPort[0]
			sshPortInt = 22
		} else if len(hostPort) == 2 {
			// Port specified, parse it
			sshHost = hostPort[0]
			sshPortStr := hostPort[1]

			// Validate SSH port is numeric
			var err error
			sshPortInt, err = strconv.Atoi(sshPortStr)
			if err != nil {
				log.Fatal("SSH port must be numeric")
			}
		} else {
			log.Fatal("Invalid SSH server format. Expected: user@host[:port]")
		}
	}

	if isClient {
		log.Info("Running in CLIENT mode")
		log.Infof("HTTP Proxy port %d\n", *proxyport)
		log.Infof("SSH tunnel to %s@%s:%d\n", sshUser, sshHost, sshPortInt)
		log.Infof("SSH key: %s\n", *sshKeyPath)
	} else {
		log.Info("Running in SERVER mode")
		log.Info("HTTP proxy disabled in server mode")
		log.Info("Server listening on loopback only (for SSH tunneling)")
	}

	log.Infof("ProtoBuf listening port %d\n", *listenPort)
	if isClient {
		log.Infof("Initial pool size %d\n", *poolInit)
		log.Infof("Maximum pool size %d\n", *poolCap)
		if *pingPool {
			log.Info("Will ping connections in pool")
		} else {
			log.Info("Will not ping connections in pool")
		}
	}

	// Create app instance
	twt2.NewApp(twt2.Hijack, *listenPort, sshHost, sshPortInt, *poolInit, *poolCap, *pingPool, isClient, sshUser, *sshKeyPath, sshPortInt)
	defer func() {
		// Cleanup pool connections and SSH tunnels (only relevant for client mode)
		if twt2.GetApp() != nil {
			for _, poolConn := range twt2.GetApp().PoolConnections {
				close(poolConn.SendChan)
				poolConn.Conn.Close()
				if poolConn.SSHClient != nil {
					log.Infof("Closing SSH client for connection %d", poolConn.ID)
					poolConn.SSHClient.Close()
				}
			}
		}
	}()

	log.Warn("Ready to serve")
	go stats()

	if isClient {
		// Only start HTTP proxy in client mode
		log.Infof("Starting HTTP proxy on port %d", *proxyport)
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *proxyport), twt2.GetApp()))
	} else {
		// Server mode - start ProtoBuf server and keep running
		log.Infof("Starting ProtoBuf server on port %d", *listenPort)
		twt2.ProtobufServer(*listenPort) // This will block forever
	}
}
