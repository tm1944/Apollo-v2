package scheduler

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	apollov1 "github.com/tm1944/Apollo-v2/control-plane/gen/apollo/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const DefaultMaxAttempts uint32 = 3

var (
	ErrJobNotFound        = errors.New("job not found")
	ErrWorkerNotFound     = errors.New("worker not found")
	ErrWorkerExists       = errors.New("worker already connected")
	ErrStaleAttempt       = errors.New("stale or unknown attempt")
	ErrInvalidTask        = errors.New("invalid task")
	ErrInvalidWorker      = errors.New("invalid worker")
	ErrAttemptsOutOfRange = errors.New("max attempts must be between 1 and 10")
)

type Config struct {
	Now        func() time.Time
	NewID      func() string
	RetryDelay func(attempt uint32) time.Duration
	Observer   Observer
}

type Observer interface {
	JobSubmitted(task string)
	JobStarted(task string, schedulingLatency time.Duration)
	JobFinished(task, status string, duration time.Duration)
	JobRetried(reason string, reassigned bool)
	SetState(queued, running, workers, activeSlots, capacitySlots int)
}

type queuedJob struct {
	jobID   string
	readyAt time.Time
}

type activeAttempt struct {
	jobID     string
	attemptID string
}

type worker struct {
	id            string
	capacity      uint32
	active        map[string]activeAttempt
	assignments   chan *apollov1.Assignment
	lastHeartbeat time.Time
}

type Scheduler struct {
	mu       sync.Mutex
	jobs     map[string]*apollov1.Job
	queue    []queuedJob
	workers  map[string]*worker
	now      func() time.Time
	newID    func() string
	retry    func(uint32) time.Duration
	observer Observer
}

func New(cfg Config) *Scheduler {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.NewID == nil {
		cfg.NewID = func() string { return uuid.NewString() }
	}
	if cfg.RetryDelay == nil {
		cfg.RetryDelay = func(attempt uint32) time.Duration {
			return time.Duration(1<<min(attempt-1, 5)) * 100 * time.Millisecond
		}
	}
	return &Scheduler{
		jobs:     make(map[string]*apollov1.Job),
		workers:  make(map[string]*worker),
		now:      cfg.Now,
		newID:    cfg.NewID,
		retry:    cfg.RetryDelay,
		observer: cfg.Observer,
	}
}

func (s *Scheduler) Submit(task *apollov1.Task, maxAttempts uint32) (*apollov1.Job, error) {
	if err := validateTask(task); err != nil {
		return nil, err
	}
	if maxAttempts == 0 {
		maxAttempts = DefaultMaxAttempts
	}
	if maxAttempts > 10 {
		return nil, ErrAttemptsOutOfRange
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	job := &apollov1.Job{
		Id:          s.newID(),
		Task:        proto.Clone(task).(*apollov1.Task),
		State:       apollov1.JobState_JOB_STATE_QUEUED,
		MaxAttempts: maxAttempts,
		SubmittedAt: timestamppb.New(now),
	}
	s.jobs[job.Id] = job
	s.queue = append(s.queue, queuedJob{jobID: job.Id, readyAt: now})
	s.observeSubmitted(job)
	s.dispatchLocked()
	s.syncStateLocked()
	return cloneJob(job), nil
}

func (s *Scheduler) Get(jobID string) (*apollov1.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[jobID]
	if !ok {
		return nil, ErrJobNotFound
	}
	return cloneJob(job), nil
}

func (s *Scheduler) RegisterWorker(workerID string, capacity uint32) (<-chan *apollov1.Assignment, error) {
	if workerID == "" || capacity == 0 || capacity > 1024 {
		return nil, ErrInvalidWorker
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.workers[workerID]; exists {
		return nil, ErrWorkerExists
	}
	w := &worker{
		id:            workerID,
		capacity:      capacity,
		active:        make(map[string]activeAttempt),
		assignments:   make(chan *apollov1.Assignment, capacity),
		lastHeartbeat: s.now(),
	}
	s.workers[workerID] = w
	s.dispatchLocked()
	s.syncStateLocked()
	return w.assignments, nil
}

func (s *Scheduler) Heartbeat(workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workers[workerID]
	if !ok {
		return ErrWorkerNotFound
	}
	w.lastHeartbeat = s.now()
	s.dispatchLocked()
	s.syncStateLocked()
	return nil
}

func (s *Scheduler) Complete(workerID, jobID, attemptID, result string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, job, err := s.activeJobLocked(workerID, jobID, attemptID)
	if err != nil {
		return err
	}
	delete(w.active, attemptID)
	now := s.now()
	job.State = apollov1.JobState_JOB_STATE_SUCCEEDED
	job.Result = result
	job.Error = ""
	job.FinishedAt = timestamppb.New(now)
	s.observeFinished(job, "succeeded")
	s.dispatchLocked()
	s.syncStateLocked()
	return nil
}

func (s *Scheduler) Fail(workerID, jobID, attemptID, message string, retryable bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, job, err := s.activeJobLocked(workerID, jobID, attemptID)
	if err != nil {
		return err
	}
	delete(w.active, attemptID)
	s.failOrRetryLocked(job, message, retryable, s.retry(job.Attempts), "task_failure")
	s.dispatchLocked()
	s.syncStateLocked()
	return nil
}

func (s *Scheduler) UnregisterWorker(workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workers[workerID]
	if !ok {
		return ErrWorkerNotFound
	}
	delete(s.workers, workerID)
	for _, attempt := range w.active {
		job := s.jobs[attempt.jobID]
		s.failOrRetryLocked(job, "worker disconnected", true, 0, "worker_disconnect")
	}
	close(w.assignments)
	s.dispatchLocked()
	s.syncStateLocked()
	return nil
}

func (s *Scheduler) RemoveUnhealthy(timeout time.Duration) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cutoff := s.now().Add(-timeout)
	var removed []string
	for id, w := range s.workers {
		if w.lastHeartbeat.After(cutoff) {
			continue
		}
		delete(s.workers, id)
		for _, attempt := range w.active {
			s.failOrRetryLocked(s.jobs[attempt.jobID], "worker heartbeat timed out", true, 0, "heartbeat_timeout")
		}
		close(w.assignments)
		removed = append(removed, id)
	}
	sort.Strings(removed)
	s.dispatchLocked()
	s.syncStateLocked()
	return removed
}

func (s *Scheduler) Dispatch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dispatchLocked()
	s.syncStateLocked()
}

func (s *Scheduler) Counts() (queued, running int, workers int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, job := range s.jobs {
		switch job.State {
		case apollov1.JobState_JOB_STATE_QUEUED:
			queued++
		case apollov1.JobState_JOB_STATE_RUNNING:
			running++
		}
	}
	return queued, running, len(s.workers)
}

func (s *Scheduler) activeJobLocked(workerID, jobID, attemptID string) (*worker, *apollov1.Job, error) {
	w, ok := s.workers[workerID]
	if !ok {
		return nil, nil, ErrWorkerNotFound
	}
	attempt, ok := w.active[attemptID]
	if !ok || attempt.jobID != jobID {
		return nil, nil, ErrStaleAttempt
	}
	job, ok := s.jobs[jobID]
	if !ok || job.State != apollov1.JobState_JOB_STATE_RUNNING || job.WorkerId != workerID {
		return nil, nil, ErrStaleAttempt
	}
	return w, job, nil
}

func (s *Scheduler) failOrRetryLocked(job *apollov1.Job, message string, retryable bool, delay time.Duration, reason string) {
	job.WorkerId = ""
	job.Error = message
	if retryable && job.Attempts < job.MaxAttempts {
		job.State = apollov1.JobState_JOB_STATE_QUEUED
		s.queue = append(s.queue, queuedJob{jobID: job.Id, readyAt: s.now().Add(delay)})
		if s.observer != nil {
			s.observer.JobRetried(reason, reason == "worker_disconnect" || reason == "heartbeat_timeout")
		}
		return
	}
	job.State = apollov1.JobState_JOB_STATE_FAILED
	job.FinishedAt = timestamppb.New(s.now())
	s.observeFinished(job, "failed")
}

func (s *Scheduler) dispatchLocked() {
	for {
		worker := s.leastLoadedWorkerLocked()
		queueIndex := s.readyJobIndexLocked()
		if worker == nil || queueIndex < 0 {
			return
		}
		entry := s.queue[queueIndex]
		s.queue = append(s.queue[:queueIndex], s.queue[queueIndex+1:]...)
		job := s.jobs[entry.jobID]
		if job == nil || job.State != apollov1.JobState_JOB_STATE_QUEUED {
			continue
		}
		attemptID := s.newID()
		job.State = apollov1.JobState_JOB_STATE_RUNNING
		job.Attempts++
		job.WorkerId = worker.id
		job.StartedAt = timestamppb.New(s.now())
		worker.active[attemptID] = activeAttempt{jobID: job.Id, attemptID: attemptID}
		if s.observer != nil {
			s.observer.JobStarted(taskLabel(job.Task), s.now().Sub(job.SubmittedAt.AsTime()))
		}
		worker.assignments <- &apollov1.Assignment{
			JobId:     job.Id,
			AttemptId: attemptID,
			Task:      proto.Clone(job.Task).(*apollov1.Task),
		}
	}
}

func (s *Scheduler) observeSubmitted(job *apollov1.Job) {
	if s.observer != nil {
		s.observer.JobSubmitted(taskLabel(job.Task))
	}
}

func (s *Scheduler) observeFinished(job *apollov1.Job, status string) {
	if s.observer == nil {
		return
	}
	duration := time.Duration(0)
	if job.StartedAt != nil {
		duration = s.now().Sub(job.StartedAt.AsTime())
	}
	s.observer.JobFinished(taskLabel(job.Task), status, duration)
}

func (s *Scheduler) syncStateLocked() {
	if s.observer == nil {
		return
	}
	queued, running, activeSlots, capacitySlots := 0, 0, 0, 0
	for _, job := range s.jobs {
		switch job.State {
		case apollov1.JobState_JOB_STATE_QUEUED:
			queued++
		case apollov1.JobState_JOB_STATE_RUNNING:
			running++
		}
	}
	for _, worker := range s.workers {
		activeSlots += len(worker.active)
		capacitySlots += int(worker.capacity)
	}
	s.observer.SetState(queued, running, len(s.workers), activeSlots, capacitySlots)
}

func taskLabel(task *apollov1.Task) string {
	return task.GetType().String()
}

func (s *Scheduler) readyJobIndexLocked() int {
	now := s.now()
	for i, entry := range s.queue {
		if !entry.readyAt.After(now) {
			return i
		}
	}
	return -1
}

func (s *Scheduler) leastLoadedWorkerLocked() *worker {
	var selected *worker
	for _, candidate := range s.workers {
		if uint32(len(candidate.active)) >= candidate.capacity {
			continue
		}
		if selected == nil ||
			len(candidate.active)*int(selected.capacity) < len(selected.active)*int(candidate.capacity) ||
			(len(candidate.active)*int(selected.capacity) == len(selected.active)*int(candidate.capacity) && candidate.id < selected.id) {
			selected = candidate
		}
	}
	return selected
}

func validateTask(task *apollov1.Task) error {
	if task == nil {
		return fmt.Errorf("%w: task is required", ErrInvalidTask)
	}
	switch task.Type {
	case apollov1.TaskType_TASK_TYPE_ADD:
		if len(task.Operands) != 2 {
			return fmt.Errorf("%w: ADD requires two operands", ErrInvalidTask)
		}
	case apollov1.TaskType_TASK_TYPE_SLEEP, apollov1.TaskType_TASK_TYPE_CPU_BURN:
		if task.DurationMs == 0 || task.DurationMs > 300_000 {
			return fmt.Errorf("%w: duration must be between 1 and 300000 ms", ErrInvalidTask)
		}
	default:
		return fmt.Errorf("%w: unsupported task type", ErrInvalidTask)
	}
	return nil
}

func cloneJob(job *apollov1.Job) *apollov1.Job {
	return proto.Clone(job).(*apollov1.Job)
}
