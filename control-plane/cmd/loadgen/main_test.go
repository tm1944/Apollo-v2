package main

import (
	"testing"
	"time"
)

func TestParseTaskMix(t *testing.T) {
	mix, err := parseTaskMix("add=80,sleep=15,cpu=5")
	if err != nil || mix.add != 80 || mix.sleep != 15 || mix.cpu != 5 {
		t.Fatalf("mix = %+v, error = %v", mix, err)
	}
	if _, err := parseTaskMix("add=0,sleep=0,cpu=0"); err == nil {
		t.Fatal("zero task mix accepted")
	}
}

func TestPercentile(t *testing.T) {
	values := []time.Duration{time.Millisecond, 5 * time.Millisecond, 3 * time.Millisecond}
	if got := percentile(values, 0.50); got != 3 {
		t.Fatalf("p50 = %v", got)
	}
}
