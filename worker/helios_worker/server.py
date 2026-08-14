"""gRPC server entrypoint.

HEL-003: bind a port, register generated servicer, implement stub Infer/Health.
HEL-005: keep server wiring here; put model load/inference in model.py.
"""


def main() -> None:
    # TODO(HEL-003): start the gRPC server.
    raise NotImplementedError("HEL-003: implement the Python gRPC worker")


if __name__ == "__main__":
    main()
