from __future__ import annotations

import queue
import threading
import time

import pytest
from apollo.v1 import scheduler_pb2

from worker.runtime import TaskRunner, Worker, WorkerConfig, execute_task


def test_execute_add() -> None:
    task = scheduler_pb2.Task(type=scheduler_pb2.TASK_TYPE_ADD, operands=[1.25, 2.5])
    assert execute_task(task) == "3.75"


def test_execute_rejects_invalid_task() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        execute_task(scheduler_pb2.Task())


def test_runner_bounds_concurrency() -> None:
    output: queue.Queue[scheduler_pb2.WorkRequest] = queue.Queue()
    lock = threading.Lock()
    active = 0
    high_water = 0

    def execute(_task: scheduler_pb2.Task) -> str:
        nonlocal active, high_water
        with lock:
            active += 1
            high_water = max(high_water, active)
        time.sleep(0.02)
        with lock:
            active -= 1
        return "done"

    runner = TaskRunner(2, output, execute)
    for index in range(6):
        runner.submit(
            scheduler_pb2.Assignment(
                job_id=f"job-{index}",
                attempt_id=f"attempt-{index}",
                task=scheduler_pb2.Task(type=scheduler_pb2.TASK_TYPE_ADD, operands=[1, 2]),
            )
        )
    results = [output.get(timeout=2) for _ in range(6)]
    runner.close()
    assert high_water == 2
    assert all(message.result.result == "done" for message in results)


def test_runner_reports_value_errors_as_non_retryable() -> None:
    output: queue.Queue[scheduler_pb2.WorkRequest] = queue.Queue()

    def fail(_task: scheduler_pb2.Task) -> str:
        raise ValueError("bad input")

    runner = TaskRunner(1, output, fail)
    runner.submit(
        scheduler_pb2.Assignment(
            job_id="job",
            attempt_id="attempt",
            task=scheduler_pb2.Task(type=scheduler_pb2.TASK_TYPE_ADD, operands=[1, 2]),
        )
    )
    message = output.get(timeout=2)
    runner.close()
    assert message.failure.error == "bad input"
    assert message.failure.retryable is False


def test_worker_validates_capacity() -> None:
    with pytest.raises(ValueError, match="capacity"):
        Worker(WorkerConfig(worker_id="worker", capacity=0))


def test_heartbeat_is_not_starved_by_results() -> None:
    outbound: queue.Queue[scheduler_pb2.WorkRequest] = queue.Queue()
    runner = TaskRunner(1, outbound)
    worker = Worker(
        WorkerConfig(worker_id="worker", capacity=1, heartbeat_seconds=0.01)
    )
    stop = threading.Event()
    requests = worker._requests(outbound, runner, stop)
    assert next(requests).HasField("hello")
    outbound.put(
        scheduler_pb2.WorkRequest(
            result=scheduler_pb2.JobResult(job_id="job-1", attempt_id="attempt-1", result="3")
        )
    )
    assert next(requests).HasField("result")
    outbound.put(
        scheduler_pb2.WorkRequest(
            result=scheduler_pb2.JobResult(job_id="job-2", attempt_id="attempt-2", result="3")
        )
    )
    time.sleep(0.02)
    assert next(requests).HasField("heartbeat")
    assert next(requests).HasField("result")
    stop.set()
    runner.close()
