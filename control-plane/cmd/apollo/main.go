package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	apollov1 "github.com/tm1944/Apollo-v2/control-plane/gen/apollo/v1"
	"github.com/tm1944/Apollo-v2/control-plane/internal/api"
	"github.com/tm1944/Apollo-v2/control-plane/internal/scheduler"
	"google.golang.org/grpc"
)

func main() {
	grpcAddress := flag.String("grpc-address", ":50051", "gRPC listen address")
	heartbeatTimeout := flag.Duration("heartbeat-timeout", 3*time.Second, "time without a heartbeat before a worker is removed")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	listener, err := net.Listen("tcp", *grpcAddress)
	if err != nil {
		logger.Error("listen failed", "error", err)
		os.Exit(1)
	}
	jobScheduler := scheduler.New(scheduler.Config{})
	server := api.NewServer(jobScheduler)
	grpcServer := grpc.NewServer()
	apollov1.RegisterJobServiceServer(grpcServer, server)
	apollov1.RegisterWorkerServiceServer(grpcServer, server)

	stopMonitor := make(chan struct{})
	go monitorWorkers(jobScheduler, *heartbeatTimeout, stopMonitor, logger)
	go func() {
		logger.Info("control plane listening", "address", *grpcAddress)
		if serveErr := grpcServer.Serve(listener); serveErr != nil {
			logger.Error("gRPC server stopped", "error", serveErr)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	close(stopMonitor)
	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		grpcServer.Stop()
	}
	logger.Info("control plane stopped")
}

func monitorWorkers(s *scheduler.Scheduler, timeout time.Duration, stop <-chan struct{}, logger *slog.Logger) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			for _, workerID := range s.RemoveUnhealthy(timeout) {
				logger.Warn("worker heartbeat timed out", "worker_id", workerID)
			}
		}
	}
}
