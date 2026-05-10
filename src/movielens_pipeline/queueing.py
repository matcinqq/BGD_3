from dataclasses import dataclass
from queue import Queue
from typing import Any, Dict


@dataclass(frozen=True)
class QueueEnvelope:
    channel: str
    payload: Any


class PipelineQueue:
    def __init__(self):
        self._queue: Queue[QueueEnvelope] = Queue()
        self._push_count = 0
        self._pop_count = 0

    def push(self, channel: str, payload: Any):
        self._queue.put(QueueEnvelope(channel=channel, payload=payload))
        self._push_count += 1

    def pop(self, expected_channel: str):
        envelope = self._queue.get(timeout=5)
        # Quick safety check so we do not mix in/out payloads.
        if envelope.channel != expected_channel:
            raise RuntimeError(
                f"Queue channel mismatch. Expected '{expected_channel}', got '{envelope.channel}'."
            )
        self._pop_count += 1
        return envelope.payload

    def metrics(self) -> Dict[str, int]:
        return {
            "queue_push_count": self._push_count,
            "queue_pop_count": self._pop_count,
        }
