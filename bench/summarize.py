from __future__ import annotations

import json
import platform
import statistics
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast


def load(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text()))


def median(reports: list[dict[str, Any]], key: str) -> float:
    return statistics.median(float(report[key]) for report in reports)


def main() -> None:
    results_dir = Path(sys.argv[1])
    output = Path(sys.argv[2])
    one_worker = [load(path) for path in sorted(results_dir.glob("throughput-1-worker-*.json"))]
    four_workers = [load(path) for path in sorted(results_dir.glob("throughput-4-worker-*.json"))]
    disabled = load(results_dir / "failure-retries-disabled.json")
    enabled = load(results_dir / "failure-retries-enabled.json")
    if len(one_worker) != 5 or len(four_workers) != 5:
        raise SystemExit("expected five one-worker and five four-worker reports")

    baseline = median(one_worker, "throughput_jobs_per_second")
    distributed = median(four_workers, "throughput_jobs_per_second")
    improvement = (distributed / baseline - 1) * 100 if baseline else 0
    baseline_failed = int(disabled["failed"])
    enabled_failed = int(enabled["failed"])
    baseline_p95 = median(one_worker, "scheduling_latency_p95_ms")
    distributed_p95 = median(four_workers, "scheduling_latency_p95_ms")
    failure_reduction = (
        (baseline_failed - enabled_failed) / baseline_failed * 100 if baseline_failed else None
    )

    if failure_reduction is None:
        failure_sentence = (
            "The failure comparison was inconclusive because the disabled run lost no active jobs."
        )
    else:
        failure_sentence = (
            f"Retries and reassignment reduced terminal failures from {baseline_failed} to "
            f"{enabled_failed}, a measured reduction of {failure_reduction:.1f}%."
        )

    lines = [
        "# Latest benchmark results",
        "",
        (
            f"Measured at {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')} "
            f"on `{platform.platform()}`."
        ),
        "The Kind cluster used one control-plane pod and worker pods with four slots each.",
        "",
        "## Results",
        "",
        "| Scenario | Median throughput | Median scheduling p95 | Trials |",
        "| --- | ---: | ---: | ---: |",
        f"| One worker | {baseline:.1f} jobs/sec | {baseline_p95:.1f} ms | 5 |",
        f"| Four workers | {distributed:.1f} jobs/sec | {distributed_p95:.1f} ms | 5 |",
        "",
        f"Four workers improved median throughput by {improvement:.1f}% over one worker.",
        (
            "The four-worker run "
            f"{'sustained' if distributed >= 100 else 'did not sustain'} 100 jobs/sec."
        ),
        failure_sentence,
        "",
        "## Method",
        "",
        "Throughput trials submitted 10 ms SLEEP jobs at up to 800 jobs/sec with 256 client slots.",
        (
            "Each worker-count group ran an unrecorded two-second warmup followed by five "
            "four-second trials."
        ),
        (
            "The fault test submitted 500 ms SLEEP jobs for six seconds and deleted a worker "
            "pod after two seconds."
        ),
        "The disabled case allowed one attempt; the enabled case allowed three attempts.",
        "Throughput is successful jobs divided by wall time, including backlog drain.",
        "Raw load-generator JSON is checked into `bench/results`.",
        "",
        (
            "These numbers describe this machine and workload. Run `./bench/run-kind.sh` "
            "before using them elsewhere."
        ),
        "",
    ]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
