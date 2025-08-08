from collections import deque, defaultdict
import asyncio
import time
from logging_config import logger
from models import Frame  # Import Frame for instantiation

# Centralized configuration dictionary with default values
config = {
    "server": {
        "ignore_certificate_errors": False,
        "stream_fps": 5.0,
        "fetch_minimum_fps": 1.0,
        "fetch_maximum_fps": 30.0,
        "image_timeout_seconds": 30.0,
        "image_timeout_behavior": "disconnect",
        "stale_image_path": None,  # Path to the stale image file
        "image_validation": "valid_jpeg",
        "fetch_agent_string": "ImageFetcher/1.0",  # Server-level fetch agent string
    },
    "images": {},  # Dictionary to hold configurations for each image_id
}

# Global dictionary to store image configurations
images_config = {}

# Using defaultdict for lookup performance
frames = defaultdict(lambda: None)  # frames[image_id] = FrameData()

streams = defaultdict(dict)  # streams[image_id] = {client_id: StreamInfo()}

streams_locks = defaultdict(lambda: asyncio.Lock())

streams_peak_fps = defaultdict(
    lambda: None
)  # streams_peak_fps[image_id] = float or None

# Global dictionary to track ImageFetcherManager instances
fetcher_managers = {}  # image_id: ImageFetcherManager instance


