from __future__ import annotations

import argparse
import time

import grpc
from apollo.v1 import scheduler_pb2, scheduler_pb2_grpc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="localhost:50051")
    parser.add_argument("--jobs", type=int, default=16)
    args = parser.parse_args()

    channel = grpc.insecure_channel(args.address)
    grpc.channel_ready_future(channel).result(timeout=15)
    client = scheduler_pb2_grpc.JobServiceStub(channel)
    jobs = [
        client.SubmitJob(
            scheduler_pb2.SubmitJobRequest(
                task=scheduler_pb2.Task(type=scheduler_pb2.TASK_TYPE_ADD, operands=[index, 1])
            ),
            timeout=5,
        ).job
        for index in range(args.jobs)
    ]
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        completed = [
            client.GetJob(scheduler_pb2.GetJobRequest(job_id=job.id), timeout=5).job
            for job in jobs
        ]
        if all(job.state == scheduler_pb2.JOB_STATE_SUCCEEDED for job in completed):
            expected = [str(index + 1) for index in range(args.jobs)]
            actual = [job.result for job in completed]
            if actual != expected:
                raise SystemExit(f"unexpected results: {actual}")
            print(f"completed {len(completed)} jobs across the gRPC boundary")
            return
        time.sleep(0.05)
    raise SystemExit("jobs did not complete before the smoke-test deadline")


if __name__ == "__main__":
    main()
