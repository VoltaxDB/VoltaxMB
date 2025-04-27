package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"github.com/voltaxmq/voltaxmq/internal/broker"
	"github.com/voltaxmq/voltaxmq/internal/metrics"
	"github.com/voltaxmq/voltaxmq/internal/persistence"
	"github.com/voltaxmq/voltaxmq/internal/queue"
)

var (
	configFile string
)

func init() {
	flag.StringVar(&configFile, "config", "config.yaml", "path to config file")
	flag.Parse()
}

func main() {
	viper.SetConfigFile(configFile)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	metricsEnabled := viper.GetBool("metrics.enabled")
	metricsPort := viper.GetInt("metrics.port")
	metricsManager := metrics.NewMetrics()

	if metricsEnabled {
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			metricsAddr := fmt.Sprintf(":%d", metricsPort)
			log.Printf("Starting metrics server on %s", metricsAddr)
			if err := http.ListenAndServe(metricsAddr, nil); err != nil {
				log.Printf("Metrics server error: %v", err)
			}
		}()
	}

	queueManager := queue.NewManager()
	queueManager.SetMetrics(metricsManager)

	scheduler := queue.NewScheduler(queueManager)
	scheduler.Start()
	defer scheduler.Stop()

	dataDir := viper.GetString("persistence.data_dir")
	aofEnabled := viper.GetBool("persistence.aof_enabled")
	snapshotInterval := viper.GetDuration("persistence.snapshot_interval")

	var aofWriter *persistence.AOFWriter
	var snapshotManager *persistence.SnapshotManager

	if aofEnabled {
		var err error
		aofWriter, err = persistence.NewAOFWriter(dataDir)
		if err != nil {
			log.Fatalf("Failed to initialize AOF writer: %v", err)
		}
		defer aofWriter.Close()

		snapshotManager = persistence.NewSnapshotManager(dataDir, snapshotInterval, queueManager, aofWriter)
		if err := snapshotManager.LoadSnapshot(); err != nil {
			log.Printf("Warning: Failed to load snapshot: %v", err)
		}
		snapshotManager.Start()
		defer snapshotManager.Stop()
	}

	host := viper.GetString("server.host")
	tcpPort := viper.GetInt("server.port")
	httpPort := viper.GetInt("server.http_port")

	tcpServer := broker.NewTCPServer(queueManager, aofWriter)
	tcpServer.SetMetrics(metricsManager)

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, tcpPort))
	if err != nil {
		log.Fatalf("Failed to start TCP server: %v", err)
	}
	defer listener.Close()

	httpServer := broker.NewHTTPServer(queueManager, scheduler, httpPort)
	httpServer.SetMetrics(metricsManager)
	go func() {
		if err := httpServer.Start(); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Printf("VoltaxMQ broker started on TCP %s:%d and HTTP %s:%d", host, tcpPort, host, httpPort)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down broker...")
		listener.Close()
		os.Exit(0)
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go tcpServer.HandleConnection(conn)
	}
}
