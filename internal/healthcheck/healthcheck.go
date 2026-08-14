// Package healthcheck will periodically call each worker's Health RPC and
// update the registry.
//
// HEL-011: ticker loop, timeouts, mark healthy/unhealthy, stoppable.
package healthcheck
