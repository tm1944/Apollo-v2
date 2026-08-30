package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	apollov1 "github.com/tm1944/Apollo-v2/control-plane/gen/apollo/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	Address      string  `json:"address"`
	Rate         float64 `json:"rate_jobs_per_second"`
	Duration     string  `json:"duration"`
	Concurrency  int     `json:"concurrency"`
	TaskMix      string  `json:"task_mix"`
	SleepMS      uint    `json:"sleep_ms"`
	CPUMS        uint    `json:"cpu_ms"`
	MaxAttempts  uint    `json:"max_attempts"`
	PollInterval string  `json:"poll_interval"`
}

type report struct {
	Version                 int     `json:"version"`
	StartedAt               string  `json:"started_at"`
	Config                  config  `json:"config"`
	Submitted               int     `json:"submitted"`
	Succeeded               int     `json:"succeeded"`
	Failed                  int     `json:"failed"`
	RPCErrors               int     `json:"rpc_errors"`
	WallSeconds             float64 `json:"wall_seconds"`
	ThroughputJobsPerSecond float64 `json:"throughput_jobs_per_second"`
	SchedulingLatencyP50MS  float64 `json:"scheduling_latency_p50_ms"`
	SchedulingLatencyP95MS  float64 `json:"scheduling_latency_p95_ms"`
	SchedulingLatencyP99MS  float64 `json:"scheduling_latency_p99_ms"`
	CompletionLatencyP95MS  float64 `json:"completion_latency_p95_ms"`
}

type taskMix struct {
	add, sleep, cpu int
}

type result struct {
	succeeded         bool
	rpcError          bool
	schedulingLatency time.Duration
	completionLatency time.Duration
}

func main() {
	cfg := config{}
	var duration time.Duration
	var pollInterval time.Duration
	var output string
	flag.StringVar(&cfg.Address, "address", "localhost:50051", "control-plane gRPC address")
	flag.Float64Var(&cfg.Rate, "rate", 100, "maximum submitted jobs per second; zero means unpaced")
	flag.DurationVar(&duration, "duration", 10*time.Second, "submission duration")
	flag.IntVar(&cfg.Concurrency, "concurrency", 64, "maximum jobs in flight")
	flag.StringVar(&cfg.TaskMix, "task-mix", "add=100,sleep=0,cpu=0", "weighted task mix")
	flag.UintVar(&cfg.SleepMS, "sleep-ms", 10, "SLEEP task duration")
	flag.UintVar(&cfg.CPUMS, "cpu-ms", 10, "CPU_BURN task duration")
	flag.UintVar(&cfg.MaxAttempts, "max-attempts", 3, "attempt limit for each job")
	flag.DurationVar(&pollInterval, "poll-interval", 2*time.Millisecond, "job status polling interval")
	flag.StringVar(&output, "output", "", "write JSON to this path instead of stdout")
	flag.Parse()
	cfg.Duration = duration.String()
	cfg.PollInterval = pollInterval.String()

	mix, err := parseTaskMix(cfg.TaskMix)
	if err != nil || duration <= 0 || cfg.Concurrency <= 0 || cfg.Rate < 0 || cfg.MaxAttempts < 1 || cfg.MaxAttempts > 10 {
		fmt.Fprintf(os.Stderr, "invalid arguments: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration+2*time.Minute)
	defer cancel()
	conn, err := grpc.NewClient(cfg.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()

	started := time.Now()
	results := run(ctx, apollov1.NewJobServiceClient(conn), cfg, duration, pollInterval, mix)
	report := makeReport(cfg, started, time.Since(started), results)
	data, _ := json.MarshalIndent(report, "", "  ")
	data = append(data, '\n')
	if output == "" {
		_, _ = os.Stdout.Write(data)
		return
	}
	if err := os.WriteFile(output, data, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, client apollov1.JobServiceClient, cfg config, duration, pollInterval time.Duration, mix taskMix) []result {
	tasks := make(chan *apollov1.Task, cfg.Concurrency)
	results := make(chan result, cfg.Concurrency)
	var workers sync.WaitGroup
	for range cfg.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for task := range tasks {
				results <- execute(ctx, client, task, uint32(cfg.MaxAttempts), pollInterval)
			}
		}()
	}

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		defer close(tasks)
		deadline := time.Now().Add(duration)
		var pace <-chan time.Time
		var ticker *time.Ticker
		if cfg.Rate > 0 {
			ticker = time.NewTicker(time.Duration(float64(time.Second) / cfg.Rate))
			defer ticker.Stop()
			pace = ticker.C
		}
		for time.Now().Before(deadline) {
			if pace != nil {
				select {
				case <-ctx.Done():
					return
				case <-pace:
				}
			}
			select {
			case tasks <- makeTask(mix, uint32(cfg.SleepMS), uint32(cfg.CPUMS)):
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		<-producerDone
		workers.Wait()
		close(results)
	}()
	var collected []result
	for item := range results {
		collected = append(collected, item)
	}
	return collected
}

func execute(ctx context.Context, client apollov1.JobServiceClient, task *apollov1.Task, maxAttempts uint32, pollInterval time.Duration) result {
	submitResponse, err := client.SubmitJob(ctx, &apollov1.SubmitJobRequest{Task: task, MaxAttempts: maxAttempts})
	if err != nil {
		return result{rpcError: true}
	}
	job := submitResponse.GetJob()
	for {
		getResponse, getErr := client.GetJob(ctx, &apollov1.GetJobRequest{JobId: job.GetId()})
		err = getErr
		if err != nil {
			return result{rpcError: true}
		}
		job = getResponse.GetJob()
		switch job.GetState() {
		case apollov1.JobState_JOB_STATE_SUCCEEDED, apollov1.JobState_JOB_STATE_FAILED:
			result := result{succeeded: job.GetState() == apollov1.JobState_JOB_STATE_SUCCEEDED}
			if job.GetStartedAt() != nil {
				result.schedulingLatency = job.GetStartedAt().AsTime().Sub(job.GetSubmittedAt().AsTime())
			}
			if job.GetFinishedAt() != nil {
				result.completionLatency = job.GetFinishedAt().AsTime().Sub(job.GetSubmittedAt().AsTime())
			}
			return result
		}
		select {
		case <-ctx.Done():
			return result{rpcError: true}
		case <-time.After(pollInterval):
		}
	}
}

func parseTaskMix(value string) (taskMix, error) {
	var mix taskMix
	for _, part := range strings.Split(value, ",") {
		key, raw, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			return mix, errors.New("task mix must use name=weight entries")
		}
		weight, err := strconv.Atoi(raw)
		if err != nil || weight < 0 {
			return mix, fmt.Errorf("invalid weight %q", raw)
		}
		switch key {
		case "add":
			mix.add = weight
		case "sleep":
			mix.sleep = weight
		case "cpu":
			mix.cpu = weight
		default:
			return mix, fmt.Errorf("unknown task %q", key)
		}
	}
	if mix.add+mix.sleep+mix.cpu == 0 {
		return mix, errors.New("at least one task weight must be positive")
	}
	return mix, nil
}

func makeTask(mix taskMix, sleepMS, cpuMS uint32) *apollov1.Task {
	draw := rand.IntN(mix.add + mix.sleep + mix.cpu)
	if draw < mix.add {
		return &apollov1.Task{Type: apollov1.TaskType_TASK_TYPE_ADD, Operands: []float64{1, 2}}
	}
	if draw < mix.add+mix.sleep {
		return &apollov1.Task{Type: apollov1.TaskType_TASK_TYPE_SLEEP, DurationMs: sleepMS}
	}
	return &apollov1.Task{Type: apollov1.TaskType_TASK_TYPE_CPU_BURN, DurationMs: cpuMS}
}

func makeReport(cfg config, started time.Time, wall time.Duration, results []result) report {
	report := report{Version: 1, StartedAt: started.UTC().Format(time.RFC3339), Config: cfg, Submitted: len(results), WallSeconds: wall.Seconds()}
	var scheduling, completion []time.Duration
	for _, item := range results {
		switch {
		case item.rpcError:
			report.RPCErrors++
		case item.succeeded:
			report.Succeeded++
		default:
			report.Failed++
		}
		if item.schedulingLatency > 0 {
			scheduling = append(scheduling, item.schedulingLatency)
		}
		if item.completionLatency > 0 {
			completion = append(completion, item.completionLatency)
		}
	}
	if wall > 0 {
		report.ThroughputJobsPerSecond = float64(report.Succeeded) / wall.Seconds()
	}
	report.SchedulingLatencyP50MS = percentile(scheduling, 0.50)
	report.SchedulingLatencyP95MS = percentile(scheduling, 0.95)
	report.SchedulingLatencyP99MS = percentile(scheduling, 0.99)
	report.CompletionLatencyP95MS = percentile(completion, 0.95)
	return report
}

func percentile(values []time.Duration, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	index := int(float64(len(values)-1) * quantile)
	return float64(values[index]) / float64(time.Millisecond)
}
