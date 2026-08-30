# Latest benchmark results

Measured at 2026-08-30 00:52 UTC on `macOS-15.7.7-arm64-arm-64bit-Mach-O`.
The Kind cluster used one control-plane pod and worker pods with four slots each.

## Results

| Scenario | Median throughput | Median scheduling p95 | Trials |
| --- | ---: | ---: | ---: |
| One worker | 203.6 jobs/sec | 1575.3 ms | 5 |
| Four workers | 668.2 jobs/sec | 421.3 ms | 5 |

Four workers improved median throughput by 228.1% over one worker.
The four-worker run sustained 100 jobs/sec.
Retries and reassignment reduced terminal failures from 4 to 0, a measured reduction of 100.0%.

## Method

Throughput trials submitted 10 ms SLEEP jobs at up to 800 jobs/sec with 256 client slots.
Each worker-count group ran an unrecorded two-second warmup followed by five four-second trials.
The fault test submitted 500 ms SLEEP jobs for six seconds and deleted a worker pod after two seconds.
The disabled case allowed one attempt; the enabled case allowed three attempts.
Throughput is successful jobs divided by wall time, including backlog drain.
Raw load-generator JSON is checked into `bench/results`.

These numbers describe this machine and workload. Run `./bench/run-kind.sh` before using them elsewhere.
