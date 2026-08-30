package main

import (
	"context"
	"flag"
	"os"
	"time"

	apollov1 "github.com/tm1944/Apollo-v2/control-plane/gen/apollo/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	address := flag.String("address", "localhost:50051", "control-plane address")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	conn, err := grpc.NewClient(*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()
	if _, err = apollov1.NewJobServiceClient(conn).Health(ctx, &apollov1.HealthRequest{}); err != nil {
		os.Exit(1)
	}
}
