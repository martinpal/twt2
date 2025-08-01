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
		if twt2.GetApp().ConnectionPool != nil {
			log.Infof("Connection pool length: %d", twt2.GetApp().ConnectionPool.Len())
		}
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
	peerHost := flag.String("h", "127.0.0.1", "Address of the peer host")
	peerPort := flag.Int("p", 33333, "Port of the peer on peer host")
	listenPort := flag.Int("b", 33333, "Our protobuf port to listen on")
	poolInit := flag.Int("i", 100, "Initial size of the connection pool between the ends of tunnel")
	poolCap := flag.Int("c", 500, "Cap of the connection pool size")
	pingPool := flag.Bool("ping", false, "To ping or not to ping on the connection pool connections")
	flag.Parse()
	setLogLevel(logLevel)
	log.Infof("Proxy port %d\n", *proxyport)
	log.Infof("Peer host:port %s:%d\n", *peerHost, *peerPort)
	log.Infof("Listening port %d\n", *listenPort)
	log.Infof("Initial pool size %d\n", *poolInit)
	log.Infof("Maximum pool size %d\n", *poolCap)
	if *pingPool {
		log.Info("Will ping connections in pool")
	} else {
		log.Info("Will not ping connections in pool")
	}

	// remote side
	go twt2.ProtobufServer(*listenPort)
	time.Sleep(5000 * time.Millisecond)
	// http proxy side
	twt2.NewApp(twt2.Hijack, *listenPort, *peerHost, *peerPort, *poolInit, *poolCap, *pingPool)
	defer func() {
		if twt2.GetApp().ConnectionPool != nil {
			twt2.GetApp().ConnectionPool.Release()
		}
	}()
	log.Warn("Ready to serve")
	go stats()
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *proxyport), twt2.GetApp()))
}
