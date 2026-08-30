package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	jobsSubmitted       *prometheus.CounterVec
	jobsStarted         *prometheus.CounterVec
	jobsCompleted       *prometheus.CounterVec
	retries             *prometheus.CounterVec
	reassignments       prometheus.Counter
	schedulingLatency   prometheus.Histogram
	jobDuration         *prometheus.HistogramVec
	queueDepth          prometheus.Gauge
	runningJobs         prometheus.Gauge
	healthyWorkers      prometheus.Gauge
	workerActiveSlots   prometheus.Gauge
	workerCapacitySlots prometheus.Gauge
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	m := &Metrics{
		jobsSubmitted:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "apollo_jobs_submitted_total", Help: "Jobs accepted by task type."}, []string{"task"}),
		jobsStarted:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "apollo_jobs_started_total", Help: "Job attempts assigned by task type."}, []string{"task"}),
		jobsCompleted:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "apollo_jobs_completed_total", Help: "Terminal jobs by task type and status."}, []string{"task", "status"}),
		retries:             prometheus.NewCounterVec(prometheus.CounterOpts{Name: "apollo_job_retries_total", Help: "Job retries by cause."}, []string{"reason"}),
		reassignments:       prometheus.NewCounter(prometheus.CounterOpts{Name: "apollo_job_reassignments_total", Help: "Jobs requeued after worker loss."}),
		schedulingLatency:   prometheus.NewHistogram(prometheus.HistogramOpts{Name: "apollo_scheduling_latency_seconds", Help: "Time from submission to assignment.", Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16)}),
		jobDuration:         prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "apollo_job_duration_seconds", Help: "Worker execution time observed by the control plane.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 16)}, []string{"task", "status"}),
		queueDepth:          prometheus.NewGauge(prometheus.GaugeOpts{Name: "apollo_queue_depth", Help: "Jobs waiting for assignment."}),
		runningJobs:         prometheus.NewGauge(prometheus.GaugeOpts{Name: "apollo_running_jobs", Help: "Jobs assigned to workers."}),
		healthyWorkers:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "apollo_healthy_workers", Help: "Workers with an active stream."}),
		workerActiveSlots:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "apollo_worker_active_slots", Help: "Slots occupied across connected workers."}),
		workerCapacitySlots: prometheus.NewGauge(prometheus.GaugeOpts{Name: "apollo_worker_capacity_slots", Help: "Total advertised capacity across connected workers."}),
	}
	registerer.MustRegister(
		m.jobsSubmitted, m.jobsStarted, m.jobsCompleted, m.retries, m.reassignments,
		m.schedulingLatency, m.jobDuration, m.queueDepth, m.runningJobs, m.healthyWorkers,
		m.workerActiveSlots, m.workerCapacitySlots,
	)
	return m
}

func (m *Metrics) JobSubmitted(task string) {
	m.jobsSubmitted.WithLabelValues(task).Inc()
}

func (m *Metrics) JobStarted(task string, latency time.Duration) {
	m.jobsStarted.WithLabelValues(task).Inc()
	m.schedulingLatency.Observe(latency.Seconds())
}

func (m *Metrics) JobFinished(task, status string, duration time.Duration) {
	m.jobsCompleted.WithLabelValues(task, status).Inc()
	m.jobDuration.WithLabelValues(task, status).Observe(duration.Seconds())
}

func (m *Metrics) JobRetried(reason string, reassigned bool) {
	m.retries.WithLabelValues(reason).Inc()
	if reassigned {
		m.reassignments.Inc()
	}
}

func (m *Metrics) SetState(queued, running, workers, activeSlots, capacitySlots int) {
	m.queueDepth.Set(float64(queued))
	m.runningJobs.Set(float64(running))
	m.healthyWorkers.Set(float64(workers))
	m.workerActiveSlots.Set(float64(activeSlots))
	m.workerCapacitySlots.Set(float64(capacitySlots))
}
