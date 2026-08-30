package api

import (
	"context"
	"net"
	"testing"
	"time"

	apollov1 "github.com/tm1944/Apollo-v2/control-plane/gen/apollo/v1"
	"github.com/tm1944/Apollo-v2/control-plane/internal/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func testClients(t *testing.T) (apollov1.JobServiceClient, apollov1.WorkerServiceClient) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	server := NewServer(scheduler.New(scheduler.Config{RetryDelay: func(uint32) time.Duration { return 0 }}))
	apollov1.RegisterJobServiceServer(grpcServer, server)
	apollov1.RegisterWorkerServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return apollov1.NewJobServiceClient(conn), apollov1.NewWorkerServiceClient(conn)
}

func TestSubmitAssignCompleteAndGet(t *testing.T) {
	jobs, workers := testClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := workers.Work(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&apollov1.WorkRequest{Message: &apollov1.WorkRequest_Hello{Hello: &apollov1.WorkerHello{WorkerId: "test-worker", Capacity: 1}}}); err != nil {
		t.Fatal(err)
	}
	response, err := jobs.SubmitJob(ctx, &apollov1.SubmitJobRequest{Task: &apollov1.Task{Type: apollov1.TaskType_TASK_TYPE_ADD, Operands: []float64{2, 3}}})
	if err != nil {
		t.Fatal(err)
	}
	job := response.GetJob()
	message, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	assignment := message.GetAssignment()
	if assignment.GetJobId() != job.GetId() {
		t.Fatalf("assignment job = %q, want %q", assignment.GetJobId(), job.GetId())
	}
	if err := stream.Send(&apollov1.WorkRequest{Message: &apollov1.WorkRequest_Result{Result: &apollov1.JobResult{JobId: job.GetId(), AttemptId: assignment.GetAttemptId(), Result: "5"}}}); err != nil {
		t.Fatal(err)
	}
	for {
		getResponse, getErr := jobs.GetJob(ctx, &apollov1.GetJobRequest{JobId: job.GetId()})
		if getErr != nil {
			t.Fatal(getErr)
		}
		got := getResponse.GetJob()
		if got.GetState() == apollov1.JobState_JOB_STATE_SUCCEEDED {
			if got.GetResult() != "5" {
				t.Fatalf("result = %q", got.GetResult())
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRejectsMessageBeforeHello(t *testing.T) {
	_, workers := testClients(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, _ := workers.Work(ctx)
	if err := stream.Send(&apollov1.WorkRequest{Message: &apollov1.WorkRequest_Heartbeat{Heartbeat: &apollov1.WorkerHeartbeat{}}}); err != nil {
		t.Fatal(err)
	}
	_, err := stream.Recv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Recv error = %v", err)
	}
}
