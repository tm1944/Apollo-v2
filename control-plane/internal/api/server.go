package api

import (
	"context"
	"errors"
	"io"

	apollov1 "github.com/tm1944/Apollo-v2/control-plane/gen/apollo/v1"
	"github.com/tm1944/Apollo-v2/control-plane/internal/scheduler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	apollov1.UnimplementedJobServiceServer
	apollov1.UnimplementedWorkerServiceServer
	scheduler *scheduler.Scheduler
}

func NewServer(s *scheduler.Scheduler) *Server {
	return &Server{scheduler: s}
}

func (s *Server) SubmitJob(_ context.Context, req *apollov1.SubmitJobRequest) (*apollov1.SubmitJobResponse, error) {
	job, err := s.scheduler.Submit(req.GetTask(), req.GetMaxAttempts())
	if err != nil {
		return nil, mapError(err)
	}
	return &apollov1.SubmitJobResponse{Job: job}, nil
}

func (s *Server) GetJob(_ context.Context, req *apollov1.GetJobRequest) (*apollov1.GetJobResponse, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	job, err := s.scheduler.Get(req.GetJobId())
	if err != nil {
		return nil, mapError(err)
	}
	return &apollov1.GetJobResponse{Job: job}, nil
}

func (s *Server) Health(context.Context, *apollov1.HealthRequest) (*apollov1.HealthResponse, error) {
	return &apollov1.HealthResponse{}, nil
}

type received struct {
	message *apollov1.WorkRequest
	err     error
}

func (s *Server) Work(stream apollov1.WorkerService_WorkServer) error {
	first, err := stream.Recv()
	if err != nil {
		return mapStreamError(err)
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "first worker message must be hello")
	}
	assignments, err := s.scheduler.RegisterWorker(hello.GetWorkerId(), hello.GetCapacity())
	if err != nil {
		return mapError(err)
	}
	workerID := hello.GetWorkerId()
	defer func() { _ = s.scheduler.UnregisterWorker(workerID) }()

	receivedMessages := make(chan received)
	go func() {
		for {
			message, recvErr := stream.Recv()
			select {
			case receivedMessages <- received{message: message, err: recvErr}:
			case <-stream.Context().Done():
				return
			}
			if recvErr != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			return mapStreamError(stream.Context().Err())
		case assignment, ok := <-assignments:
			if !ok {
				return status.Error(codes.Unavailable, "worker removed after heartbeat timeout")
			}
			if err := stream.Send(&apollov1.WorkResponse{
				Message: &apollov1.WorkResponse_Assignment{Assignment: assignment},
			}); err != nil {
				return mapStreamError(err)
			}
		case packet := <-receivedMessages:
			if packet.err != nil {
				return mapStreamError(packet.err)
			}
			if err := s.handleWorkerMessage(workerID, packet.message); err != nil {
				return mapError(err)
			}
		}
	}
}

func (s *Server) handleWorkerMessage(workerID string, message *apollov1.WorkRequest) error {
	switch value := message.GetMessage().(type) {
	case *apollov1.WorkRequest_Heartbeat:
		return s.scheduler.Heartbeat(workerID)
	case *apollov1.WorkRequest_Result:
		return s.scheduler.Complete(workerID, value.Result.GetJobId(), value.Result.GetAttemptId(), value.Result.GetResult())
	case *apollov1.WorkRequest_Failure:
		return s.scheduler.Fail(workerID, value.Failure.GetJobId(), value.Failure.GetAttemptId(), value.Failure.GetError(), value.Failure.GetRetryable())
	case *apollov1.WorkRequest_Hello:
		return errors.New("hello may only be sent once")
	default:
		return errors.New("worker message is empty")
	}
}

func mapError(err error) error {
	switch {
	case errors.Is(err, scheduler.ErrJobNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, scheduler.ErrWorkerExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, scheduler.ErrStaleAttempt):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, scheduler.ErrInvalidTask), errors.Is(err, scheduler.ErrInvalidWorker), errors.Is(err, scheduler.ErrAttemptsOutOfRange):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, scheduler.ErrWorkerNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.InvalidArgument, err.Error())
	}
}

func mapStreamError(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "stream canceled")
	}
	return err
}
