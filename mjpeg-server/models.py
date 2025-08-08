from dataclasses import dataclass, field
from collections import deque
import asyncio


@dataclass
class Frame:
    """Dataclass to store the latest image data and fetch intervals."""

    data: bytes = b""  # Latest image data
    last_updated: float = 0.0  # Timestamp of the last update
    frame_intervals: deque = field(
        default_factory=lambda: deque(maxlen=100)
    )  # Stores intervals between fetches for FPS calculation
    lock: asyncio.Lock = field(
        default_factory=asyncio.Lock
    )  # Asynchronous lock for thread safety when accessing frame data


@dataclass
class Stream:
    """Dataclass to store information about each client stream."""

    requested_fps: float  # FPS requested by the client
    stream_fps: float  # Actual FPS being streamed
    start_time: float  # Timestamp when the stream started
    frame_intervals: deque = field(
        default_factory=lambda: deque(maxlen=100)
    )  # Stores intervals between frames for FPS calculation
    last_frame_time: float = 0.0  # Timestamp of the last sent frame
