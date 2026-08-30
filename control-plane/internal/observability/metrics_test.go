package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsRecordSchedulerEvents(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	metrics.JobSubmitted("ADD")
	metrics.JobStarted("ADD", 2*time.Millisecond)
	metrics.JobFinished("ADD", "succeeded", 4*time.Millisecond)
	metrics.JobRetried("heartbeat_timeout", true)
	metrics.SetState(2, 1, 3, 1, 12)

	if got := testutil.ToFloat64(metrics.jobsSubmitted.WithLabelValues("ADD")); got != 1 {
		t.Fatalf("submitted = %v", got)
	}
	if got := testutil.ToFloat64(metrics.reassignments); got != 1 {
		t.Fatalf("reassignments = %v", got)
	}
	if got := testutil.ToFloat64(metrics.queueDepth); got != 2 {
		t.Fatalf("queue depth = %v", got)
	}
}
