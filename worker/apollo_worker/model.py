"""Model loading and inference.

HEL-005: load a small pretrained sentiment (or classification) model once at
startup. Do not load the model per request.
"""


def load_model() -> None:
    # TODO(HEL-005): load the model once.
    raise NotImplementedError("HEL-005: load PyTorch model")


def infer(text: str) -> tuple[str, float]:
    # TODO(HEL-005): run inference; return (prediction, confidence).
    raise NotImplementedError("HEL-005: implement inference")
