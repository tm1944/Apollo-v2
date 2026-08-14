// Package scheduler will select a worker for each inference request.
//
// HEL-009: least-loaded among healthy workers.
// HEL-010: concurrent select + active-count increment/decrement.
package scheduler
