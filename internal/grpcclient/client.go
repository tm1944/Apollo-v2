// Package grpcclient will wrap generated gRPC stubs so HTTP and the
// scheduler depend on a small interface, not concrete dial code.
//
// HEL-004: dial a worker, call Infer (and Health).
// HEL-012: honor context deadlines and cancellation.
package grpcclient
