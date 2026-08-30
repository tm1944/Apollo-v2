package scheduler

import (
	"errors"
	"fmt"
	"testing"
	"time"

	apollov1 "github.com/tm1944/Apollo-v2/control-plane/gen/apollo/v1"
)

type fixture struct {
	scheduler *Scheduler
	now       time.Time
	nextID    int
}

func newFixture() *fixture {
	f := &fixture{now: time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)}
	f.scheduler = New(Config{
		Now: func() time.Time { return f.now },
		NewID: func() string {
			f.nextID++
			return fmt.Sprintf("id-%d", f.nextID)
		},
		RetryDelay: func(uint32) time.Duration { return time.Second },
	})
	return f
}

func addTask(a, b float64) *apollov1.Task {
	return &apollov1.Task{Type: apollov1.TaskType_TASK_TYPE_ADD, Operands: []float64{a, b}}
}

func TestSubmitValidatesAndDefaults(t *testing.T) {
	f := newFixture()
	if _, err := f.scheduler.Submit(nil, 0); !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("Submit(nil) error = %v", err)
	}
	job, err := f.scheduler.Submit(addTask(1, 2), 0)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != apollov1.JobState_JOB_STATE_QUEUED || job.MaxAttempts != 3 || job.Attempts != 0 {
		t.Fatalf("unexpected job: %+v", job)
	}
}

func TestDispatchesFIFOAndBalancesByCapacity(t *testing.T) {
	f := newFixture()
	workerA, _ := f.scheduler.RegisterWorker("a", 1)
	workerB, _ := f.scheduler.RegisterWorker("b", 2)
	job1, _ := f.scheduler.Submit(addTask(1, 1), 1)
	job2, _ := f.scheduler.Submit(addTask(2, 2), 1)
	job3, _ := f.scheduler.Submit(addTask(3, 3), 1)

	a1 := <-workerA
	b1 := <-workerB
	b2 := <-workerB
	if a1.JobId != job1.Id || b1.JobId != job2.Id || b2.JobId != job3.Id {
		t.Fatalf("assignments not FIFO: %s %s %s", a1.JobId, b1.JobId, b2.JobId)
	}
}

func TestRetryWaitsAndStopsAtLimit(t *testing.T) {
	f := newFixture()
	assignments, _ := f.scheduler.RegisterWorker("worker", 1)
	job, _ := f.scheduler.Submit(addTask(1, 2), 2)
	first := <-assignments
	if err := f.scheduler.Fail("worker", job.Id, first.AttemptId, "temporary", true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-assignments:
		t.Fatal("retry dispatched before backoff elapsed")
	default:
	}
	f.now = f.now.Add(time.Second)
	f.scheduler.Dispatch()
	second := <-assignments
	if err := f.scheduler.Fail("worker", job.Id, second.AttemptId, "still broken", true); err != nil {
		t.Fatal(err)
	}
	got, _ := f.scheduler.Get(job.Id)
	if got.State != apollov1.JobState_JOB_STATE_FAILED || got.Attempts != 2 {
		t.Fatalf("unexpected terminal job: %+v", got)
	}
}

func TestRejectsStaleResultAfterReassignment(t *testing.T) {
	f := newFixture()
	workerA, _ := f.scheduler.RegisterWorker("a", 1)
	job, _ := f.scheduler.Submit(addTask(1, 2), 3)
	first := <-workerA
	if err := f.scheduler.UnregisterWorker("a"); err != nil {
		t.Fatal(err)
	}
	workerB, _ := f.scheduler.RegisterWorker("b", 1)
	second := <-workerB
	if err := f.scheduler.Complete("b", job.Id, first.AttemptId, "bad"); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale result error = %v", err)
	}
	if err := f.scheduler.Complete("b", job.Id, second.AttemptId, "3"); err != nil {
		t.Fatal(err)
	}
	got, _ := f.scheduler.Get(job.Id)
	if got.State != apollov1.JobState_JOB_STATE_SUCCEEDED || got.Result != "3" || got.Attempts != 2 {
		t.Fatalf("unexpected completed job: %+v", got)
	}
}

func TestHeartbeatTimeoutRequeuesActiveJob(t *testing.T) {
	f := newFixture()
	workerA, _ := f.scheduler.RegisterWorker("a", 1)
	job, _ := f.scheduler.Submit(addTask(1, 2), 3)
	<-workerA
	f.now = f.now.Add(4 * time.Second)
	removed := f.scheduler.RemoveUnhealthy(3 * time.Second)
	if len(removed) != 1 || removed[0] != "a" {
		t.Fatalf("removed = %v", removed)
	}
	workerB, _ := f.scheduler.RegisterWorker("b", 1)
	assignment := <-workerB
	if assignment.JobId != job.Id {
		t.Fatalf("reassigned job = %s", assignment.JobId)
	}
}

func TestNonRetryableFailureIsTerminal(t *testing.T) {
	f := newFixture()
	assignments, _ := f.scheduler.RegisterWorker("worker", 1)
	job, _ := f.scheduler.Submit(addTask(1, 2), 3)
	attempt := <-assignments
	if err := f.scheduler.Fail("worker", job.Id, attempt.AttemptId, "invalid", false); err != nil {
		t.Fatal(err)
	}
	got, _ := f.scheduler.Get(job.Id)
	if got.State != apollov1.JobState_JOB_STATE_FAILED || got.Error != "invalid" || got.Attempts != 1 {
		t.Fatalf("unexpected job: %+v", got)
	}
}
