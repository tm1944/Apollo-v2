// Package shutdown will coordinate SIGINT/SIGTERM handling: drain HTTP,
// stop background loops, close gRPC connections.
//
// HEL-013: graceful shutdown with a deadline.
package shutdown
