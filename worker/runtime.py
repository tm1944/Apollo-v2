from __future__ import annotations

import json
import logging
import os
import queue
import signal
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

import grpc
import psutil
from apollo.v1 import scheduler_pb2, scheduler_pb2_grpc
from prometheus_client import Counter, Gauge, Histogram, start_http_server

LOGGER = logging.getLogger("apollo.worker")

ACTIVE_JOBS = Gauge("apollo_worker_active_jobs", "Jobs executing in this worker")
CAPACITY = Gauge("apollo_worker_capacity", "Maximum concurrent jobs in this worker")
TASKS = Counter("apollo_worker_tasks_total", "Tasks finished by this worker", ["status", "type"])
TASK_DURATION = Histogram(
    "apollo_worker_task_duration_seconds",
    "Time spent executing a task",
    ["type"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30),
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        for key in ("worker_id", "job_id", "attempt_id", "delay_seconds"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return json.dumps(payload, separators=(",", ":"))


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    LOGGER.handlers = [handler]
    LOGGER.setLevel(logging.INFO)


def task_name(task_type: int) -> str:
    return str(scheduler_pb2.TaskType.Name(task_type)).removeprefix("TASK_TYPE_").lower()


def execute_task(task: scheduler_pb2.Task) -> str:
    if task.type == scheduler_pb2.TASK_TYPE_ADD:
        if len(task.operands) != 2:
            raise ValueError("ADD requires exactly two operands")
        return format(task.operands[0] + task.operands[1], ".15g")
    if task.type == scheduler_pb2.TASK_TYPE_SLEEP:
        if not 1 <= task.duration_ms <= 300_000:
            raise ValueError("SLEEP duration must be between 1 and 300000 ms")
        time.sleep(task.duration_ms / 1000)
        return f"slept {task.duration_ms}ms"
    if task.type == scheduler_pb2.TASK_TYPE_CPU_BURN:
        if not 1 <= task.duration_ms <= 300_000:
            raise ValueError("CPU_BURN duration must be between 1 and 300000 ms")
        deadline = time.perf_counter() + task.duration_ms / 1000
        iterations = 0
        value = 1
        while time.perf_counter() < deadline:
            value = (value * 1_664_525 + 1_013_904_223) & 0xFFFFFFFF
            iterations += 1
        return f"iterations={iterations},checksum={value}"
    raise ValueError("unsupported task type")


class TaskRunner:
    def __init__(
        self,
        capacity: int,
        output: queue.Queue[scheduler_pb2.WorkerMessage],
        execute: Callable[[scheduler_pb2.Task], str] = execute_task,
    ) -> None:
        self._output = output
        self._execute = execute
        self._executor = ThreadPoolExecutor(max_workers=capacity, thread_name_prefix="apollo-job")
        self._futures: set[Future[str]] = set()
        self._lock = threading.Lock()
        CAPACITY.set(capacity)

    @property
    def active(self) -> int:
        with self._lock:
            return len(self._futures)

    def submit(self, assignment: scheduler_pb2.Assignment) -> None:
        future = self._executor.submit(self._run, assignment.task)
        with self._lock:
            self._futures.add(future)
            ACTIVE_JOBS.set(len(self._futures))
        future.add_done_callback(lambda completed: self._finished(assignment, completed))

    def _run(self, task: scheduler_pb2.Task) -> str:
        name = task_name(task.type)
        started = time.perf_counter()
        try:
            result = self._execute(task)
            TASKS.labels(status="succeeded", type=name).inc()
            return result
        except Exception:
            TASKS.labels(status="failed", type=name).inc()
            raise
        finally:
            TASK_DURATION.labels(type=name).observe(time.perf_counter() - started)

    def _finished(self, assignment: scheduler_pb2.Assignment, future: Future[str]) -> None:
        with self._lock:
            self._futures.discard(future)
            ACTIVE_JOBS.set(len(self._futures))
        try:
            message = scheduler_pb2.WorkerMessage(
                result=scheduler_pb2.JobResult(
                    job_id=assignment.job_id,
                    attempt_id=assignment.attempt_id,
                    result=future.result(),
                )
            )
        except Exception as exc:
            message = scheduler_pb2.WorkerMessage(
                failure=scheduler_pb2.JobFailure(
                    job_id=assignment.job_id,
                    attempt_id=assignment.attempt_id,
                    error=str(exc)[:1000],
                    retryable=not isinstance(exc, ValueError),
                )
            )
        self._output.put(message)

    def close(self, wait: bool = True) -> None:
        self._executor.shutdown(wait=wait, cancel_futures=True)


@dataclass(frozen=True)
class WorkerConfig:
    scheduler_address: str = "localhost:50051"
    worker_id: str = ""
    capacity: int = 4
    heartbeat_seconds: float = 1.0
    metrics_port: int = 8000

    @classmethod
    def from_env(cls) -> WorkerConfig:
        return cls(
            scheduler_address=os.getenv("APOLLO_SCHEDULER_ADDRESS", "localhost:50051"),
            worker_id=os.getenv("APOLLO_WORKER_ID", f"worker-{uuid.uuid4().hex[:12]}"),
            capacity=int(os.getenv("APOLLO_WORKER_CAPACITY", "4")),
            heartbeat_seconds=float(os.getenv("APOLLO_HEARTBEAT_SECONDS", "1")),
            metrics_port=int(os.getenv("APOLLO_METRICS_PORT", "8000")),
        )

    def validate(self) -> None:
        if not self.worker_id:
            raise ValueError("worker_id is required")
        if not 1 <= self.capacity <= 1024:
            raise ValueError("capacity must be between 1 and 1024")
        if self.heartbeat_seconds <= 0:
            raise ValueError("heartbeat_seconds must be positive")


class Worker:
    def __init__(self, config: WorkerConfig) -> None:
        config.validate()
        self.config = config
        self.process = psutil.Process()

    def run(self, stop: threading.Event) -> None:
        delay = 0.25
        while not stop.is_set():
            channel = grpc.insecure_channel(self.config.scheduler_address)
            try:
                self.run_connection(scheduler_pb2_grpc.WorkerServiceStub(channel), stop)
                delay = 0.25
            except grpc.RpcError:
                if stop.is_set():
                    break
                LOGGER.warning(
                    "worker stream disconnected",
                    extra={
                        "worker_id": self.config.worker_id,
                        "delay_seconds": delay,
                    },
                )
                stop.wait(delay)
                delay = min(delay * 2, 10)
            finally:
                channel.close()

    def run_connection(self, stub: Any, stop: threading.Event) -> None:
        outbound: queue.Queue[scheduler_pb2.WorkerMessage] = queue.Queue()
        runner = TaskRunner(self.config.capacity, outbound)
        try:
            for message in stub.Work(self._requests(outbound, runner, stop)):
                assignment = message.assignment
                if not assignment.job_id:
                    continue
                LOGGER.info(
                    "job assigned",
                    extra={
                        "worker_id": self.config.worker_id,
                        "job_id": assignment.job_id,
                        "attempt_id": assignment.attempt_id,
                    },
                )
                runner.submit(assignment)
        finally:
            runner.close(wait=False)

    def _requests(
        self,
        outbound: queue.Queue[scheduler_pb2.WorkerMessage],
        runner: TaskRunner,
        stop: threading.Event,
    ) -> Iterator[scheduler_pb2.WorkerMessage]:
        yield scheduler_pb2.WorkerMessage(
            hello=scheduler_pb2.WorkerHello(
                worker_id=self.config.worker_id,
                capacity=self.config.capacity,
            )
        )
        while not stop.is_set():
            try:
                yield outbound.get(timeout=self.config.heartbeat_seconds)
            except queue.Empty:
                memory = self.process.memory_info().rss
                yield scheduler_pb2.WorkerMessage(
                    heartbeat=scheduler_pb2.WorkerHeartbeat(
                        active_jobs=runner.active,
                        cpu_percent=self.process.cpu_percent(),
                        memory_bytes=memory,
                    )
                )


def main() -> None:
    configure_logging()
    config = WorkerConfig.from_env()
    config.validate()
    start_http_server(config.metrics_port)
    stop = threading.Event()

    def request_stop(_signum: int, _frame: Any) -> None:
        stop.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    LOGGER.info("worker starting", extra={"worker_id": config.worker_id})
    Worker(config).run(stop)
    LOGGER.info("worker stopped", extra={"worker_id": config.worker_id})
