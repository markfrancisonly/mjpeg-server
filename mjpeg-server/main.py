import time
import logging
import logging.config
import asyncio
import aiohttp
import aiofiles
import httpx
import datetime
import yaml
import json
import re
from collections import deque, defaultdict
from io import BytesIO
from dataclasses import dataclass, field
from PIL import Image
from yarl import URL

from fastapi import FastAPI, Response, Request, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse

import uvloop
import platform

# Set uvloop as the default event loop policy for improved performance on Unix systems
if platform.system() != "Windows":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# ---------------- Logging Configuration ---------------- #

# Define a custom logging configuration
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,  # Retain existing loggers
    "formatters": {
        "default": {
            "format": "%(asctime)s [%(levelname)s]: %(message)s",  # Log message format
        },
    },
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",  # Stream logs to stdout
            "formatter": "default",
            "stream": "ext://sys.stdout",
        },
    },
    "root": {
        "level": "INFO",  # Set root logger level
        "handlers": ["default"],
    },
    "loggers": {
        "uvicorn": {  # Logger for uvicorn
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.error": {  # Logger for uvicorn errors
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.access": {  # Logger for uvicorn access logs
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        # Add additional loggers if necessary
    },
}

# Apply the custom logging configuration
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger(__name__)  # Get the root logger for the current module

# ---------------- FastAPI App Initialization ---------------- #

app = FastAPI()  # Initialize the FastAPI application

# ---------------- Server Configuration ---------------- #

CONFIG_FILE = "config.yaml"  # Configuration file path

MAX_FPS = 240.0

# Define constants for default values
DEFAULT_IGNORE_CERTIFICATE_ERRORS = False  # Whether to ignore SSL certificate errors
DEFAULT_STREAM_FPS = 5.0  # Default frames per second for streaming
DEFAULT_FETCH_MINIMUM_FPS = 1.0  # Minimum fetch FPS to maintain
DEFAULT_FETCH_MAXIMUM_FPS = 30.0  # Maximum fetch FPS allowed
DEFAULT_STALE_FRAME_TIMEOUT_SECONDS = 30.0  # Timeout in seconds for stale frames

# Options: 'disconnect', 'stale_image', 'last_frame'
DEFAULT_STALE_FRAME_ACTION = "disconnect"

# Options: 'valid_jpeg', 'conversion', 'zero_bytes', 'none'
DEFAULT_IMAGE_VALIDATION = "valid_jpeg"

# Default fetch agent string
DEFAULT_FETCH_AGENT_STRING = "ImageFetcher/1.0"

# Fallback offline image data (1x1 pixel black image)
DEFAULT_FALLBACK_IMAGE_DATA = (
    b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01"
    b"\x00\x01\x00\x00\xff\xdb\x00\x43\x00\x03\x02\x02\x02"
    b"\x02\x02\x03\x02\x02\x02\x03\x03\x03\x03\x04\x06\x04"
    b"\x04\x03\x03\x04\x08\x06\x06\x05\x06\x09\x08\x0a\x0a"
    b"\x09\x08\x09\x09\x0a\x0c\x0f\x0c\x0a\x0b\x0e\x0b\x09"
    b"\x09\x0d\x11\x0d\x0e\x0f\x10\x10\x11\x10\x0a\x0c\x12"
    b"\x13\x12\x10\x13\x0f\x10\x10\x10\xff\xc0\x00\x0b\x08"
    b"\x00\x01\x00\x01\x01\x01\x11\x00\xff\xc4\x00\x14\x00"
    b"\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    b"\x00\x00\x00\x00\xff\xda\x00\x08\x01\x01\x00"
    b"\x00\x3f\x00\xd2\xcf\x20\xff\xd9"
)

# Centralized configuration dictionary with default values
config = {
    "server": {
        "ignore_certificate_errors": DEFAULT_IGNORE_CERTIFICATE_ERRORS,
        "stream_fps": DEFAULT_STREAM_FPS,
        "fetch_minimum_fps": DEFAULT_FETCH_MINIMUM_FPS,
        "fetch_maximum_fps": DEFAULT_FETCH_MAXIMUM_FPS,
        "image_timeout_seconds": DEFAULT_STALE_FRAME_TIMEOUT_SECONDS,
        "image_timeout_behavior": DEFAULT_STALE_FRAME_ACTION,
        "stale_image_path": None,  # Path to the stale image file
        "image_validation": DEFAULT_IMAGE_VALIDATION,
        "fetch_agent_string": DEFAULT_FETCH_AGENT_STRING,  # Server-level fetch agent string
    },
    "images": {},  # Dictionary to hold configurations for each image_id
}
images_config = {}  # Global dictionary to store image configurations

# Using defaultdict for lookup performance
frames = defaultdict(lambda: None)  # frames[image_id] = FrameData()
streams = defaultdict(dict)  # streams[image_id] = {client_id: StreamInfo()}
streams_locks = defaultdict(lambda: asyncio.Lock())
streams_peak_fps = defaultdict(
    lambda: None
)  # streams_peak_fps[image_id] = float or None

# Global dictionary to track ImageFetcherManager instances
fetcher_managers = {}  # image_id: ImageFetcherManager instance

# ---------------- Helper Classes ---------------- #


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


class ImageFetcherManager:
    """
    Helper class to manage image fetching using httpx or aiohttp.
    It abstracts the fetching logic, manages connection reuse, and provides a unified interface.
    """

    def __init__(
        self,
        image_id: str,
        url: str,
        ignore_certificate_errors: bool,
        fetch_minimum_fps: float,
        fetch_maximum_fps: float,
        image_timeout_seconds: float,
        image_validation: str,
        fetch_agent_string: str,  # Added fetch_agent_string parameter
        auth_type: str = None,
        username: str = None,
        password: str = None,
        token: str = None,
    ):
        """
        Initializes the ImageFetcherManager with configuration parameters.

        Args:
            image_id (str): The identifier for the image.
            url (str): The URL to fetch the image from.
            ignore_certificate_errors (bool): Whether to ignore SSL certificate errors.
            fetch_minimum_fps (float): Minimum frames per second to fetch.
            fetch_maximum_fps (float): Maximum frames per second to fetch.
            image_timeout_seconds (float): Timeout in seconds for stale images.
            image_validation (str): Method to validate fetched images.
            fetch_agent_string (str): The User-Agent string to use for fetching.
            auth_type (str, optional): Authentication type ('basic', 'digest', 'bearer'). Defaults to None.
            username (str, optional): Username for authentication. Defaults to None.
            password (str, optional): Password for authentication. Defaults to None.
            token (str, optional): Token for bearer authentication. Defaults to None.
        """
        self.ignore_certificate_errors = ignore_certificate_errors
        self.fetch_minimum_fps = fetch_minimum_fps
        self.fetch_maximum_fps = fetch_maximum_fps
        self.image_timeout_seconds = image_timeout_seconds
        self.image_validation = image_validation
        self.image_id = image_id
        self.url = url
        self.auth_type = auth_type
        self.username = username
        self.password = password
        self.token = token
        self.fetch_agent_string = fetch_agent_string  # Store the fetch agent string

        # Initialize single client/session instances
        self.httpx_client = None  # httpx.AsyncClient instance
        self.aiohttp_session = None  # aiohttp.ClientSession instance

        # Shutdown event to gracefully exit the fetch loop
        self._shutdown_event = asyncio.Event()

    async def fetch_frames(self):
        """
        Unified method to fetch frames using httpx or aiohttp based on the URL's authentication.
        This method determines whether to use httpx or aiohttp based on the presence of
        authentication credentials and initiates the fetching process accordingly.
        """
        image_id = self.image_id
        parsed_url = URL(self.url)  # Parse the URL to extract components

        if not self.username:
            self.username = parsed_url.user
        if not self.password:
            self.password = parsed_url.password

        clean_url = str(
            parsed_url.with_user(None).with_password(None)
        )  # Remove auth from URL

        # Determine if authentication is required
        auth_required = bool(self.auth_type) and (
            (self.auth_type in ["basic", "digest"] and self.username and self.password)
            or (self.auth_type == "bearer" and self.token)
        )

        # Select library based on authentication requirement
        if auth_required:
            # Use httpx for authenticated requests
            client = self._get_httpx_client()
            library = "httpx"
        else:
            # Use aiohttp for unauthenticated requests
            session = self._get_aiohttp_session()
            client = session
            library = "aiohttp"

        retry_backoff_duration = (
            1.0 / self.fetch_minimum_fps
        )  # Initial backoff time based on minimum FPS
        previous_fetch_time = None  # Timestamp of the previous successful fetch

        while not self._shutdown_event.is_set():
            try:
                fetch_start_time = (
                    time.time()
                )  # Record the start time of the fetch attempt

                # Acquire the per-image lock to safely read peak_stream_fps
                async with streams_locks[image_id]:
                    peak_stream_fps = streams_peak_fps[image_id]

                if peak_stream_fps is None:
                    # If there are no active streams, use the minimum fetch FPS
                    target_fetch_fps = self.fetch_minimum_fps
                else:
                    # Calculate target fetch FPS based on active streams and maximum limit
                    target_fetch_fps = min(peak_stream_fps, self.fetch_maximum_fps)
                    target_fetch_fps = max(target_fetch_fps, self.fetch_minimum_fps)

                if library == "httpx":
                    # Perform an HTTP GET request using httpx
                    headers = {"User-Agent": self.fetch_agent_string}  # Set User-Agent
                    response = await client.get(clean_url, headers=headers)
                    if response.status_code == 200:
                        image_data = response.content  # Successful fetch
                    elif response.status_code == 401:
                        # Unauthorized access; raise an exception to trigger retry
                        raise ValueError(
                            f"Unauthorized access for image_id '{image_id}'."
                        )
                    else:
                        # Unexpected HTTP status code; raise an exception to trigger retry
                        raise ValueError(
                            f"Unexpected response status: {response.status_code} for image_id '{image_id}'."
                        )
                elif library == "aiohttp":
                    # Perform an HTTP GET request using aiohttp within a context manager
                    headers = {"User-Agent": self.fetch_agent_string}  # Set User-Agent
                    async with client.get(clean_url, headers=headers) as response:
                        if response.status != 200:
                            # Non-200 response; raise an exception to trigger retry
                            raise ValueError(
                                f"Non-200 response code: {response.status} for image_id '{image_id}'."
                            )
                        image_data = await response.read()  # Read the image data
                else:
                    # Unsupported library; raise an exception
                    raise ValueError(
                        f"Unsupported method '{library}' for image_id '{image_id}'."
                    )

                # Validate and possibly convert the fetched image data
                validated_data = await assert_valid_image_data(
                    image_data, self.image_validation
                )

                fetch_end_time = time.time()  # Record the end time of the fetch

                # Update frame data with the newly fetched image
                frame = frames[image_id]
                if frame is None:
                    frame = Frame()
                    frames[image_id] = frame

                async with frame.lock:
                    frame.data = validated_data  # Update the latest image data
                    frame.last_updated = (
                        fetch_end_time  # Update the last updated timestamp
                    )

                    if previous_fetch_time is not None:
                        # Calculate the interval since the last successful fetch
                        interval = fetch_end_time - previous_fetch_time
                        frame.frame_intervals.append(
                            interval
                        )  # Store the interval for FPS calculation
                    previous_fetch_time = (
                        fetch_end_time  # Update the previous fetch time
                    )

                # Reset retry backoff duration after a successful fetch
                retry_backoff_duration = 1.0 / target_fetch_fps

                # Maintain the frame rate by ensuring appropriate delays
                if target_fetch_fps <= MAX_FPS:
                    await maintain_frame_rate(
                        image_id, target_fetch_fps, fetch_start_time
                    )

            except asyncio.CancelledError:
                # Handle task cancellation gracefully
                logger.info(f"Fetch task for '{image_id}' has been cancelled.")
                break
            except Exception as e:
                # Log warnings for any exceptions during fetching or validation
                logger.warning(f"Fetch or validation failed for '{image_id}': {e}")
                logger.debug(f"Exception traceback: {e}", exc_info=True)

                # Log the retry attempt and sleep before retrying
                logger.debug(
                    f"Retrying '{image_id}' in {retry_backoff_duration} seconds..."
                )
                await asyncio.sleep(retry_backoff_duration)
                # Implement exponential backoff with a maximum limit
                retry_backoff_duration = min(
                    retry_backoff_duration * 2, self.image_timeout_seconds
                )

    def _get_httpx_client(self) -> httpx.AsyncClient:
        """
        Retrieves or creates a single httpx.AsyncClient instance.

        Returns:
            httpx.AsyncClient: The httpx client instance.

        Raises:
            ValueError: If authentication type is set but required credentials are missing.
        """
        if not self.httpx_client:
            auth = None

            if self.auth_type == "basic":
                if self.username and self.password:
                    auth = httpx.BasicAuth(self.username, self.password)
                    logger.debug(
                        f"Image '{self.image_id}': Using Basic Authentication."
                    )
                else:
                    error_msg = f"Image '{self.image_id}': Basic auth_type requires username and password."
                    logger.error(error_msg)
                    raise ValueError(error_msg)
            elif self.auth_type == "digest":
                if self.username and self.password:
                    auth = httpx.DigestAuth(self.username, self.password)
                    logger.debug(
                        f"Image '{self.image_id}': Using Digest Authentication."
                    )
                else:
                    error_msg = f"Image '{self.image_id}': Digest auth_type requires username and password."
                    logger.error(error_msg)
                    raise ValueError(error_msg)
            elif self.auth_type == "bearer":
                if self.token:
                    auth = httpx.BearerAuth(self.token)
                    logger.debug(
                        f"Image '{self.image_id}': Using Bearer Authentication."
                    )
                else:
                    error_msg = (
                        f"Image '{self.image_id}': Bearer auth_type requires a token."
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)
            elif self.auth_type:
                # Log a warning if an unsupported auth type is specified
                logger.warning(
                    f"Image '{self.image_id}': Unsupported auth_type '{self.auth_type}'. Skipping authentication."
                )

            # Create an httpx.AsyncClient with the appropriate settings
            self.httpx_client = httpx.AsyncClient(
                timeout=10.0,  # Set a timeout for requests
                verify=not self.ignore_certificate_errors,  # SSL verification
                auth=auth,  # Authentication credentials
                headers={"User-Agent": self.fetch_agent_string},  # Set User-Agent
            )
        return self.httpx_client

    def _get_aiohttp_session(self) -> aiohttp.ClientSession:
        """
        Retrieves or creates a single aiohttp.ClientSession instance.

        Returns:
            aiohttp.ClientSession: The aiohttp session instance.
        """
        if not self.aiohttp_session:
            # Create a TCPConnector with SSL settings and connection limits
            connector = aiohttp.TCPConnector(
                ssl=not self.ignore_certificate_errors, limit_per_host=10
            )
            # Initialize the aiohttp.ClientSession with the connector and headers
            self.aiohttp_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                connector=connector,
                headers={"User-Agent": self.fetch_agent_string},
            )
        return self.aiohttp_session

    async def close(self):
        """Closes all persistent clients and sessions and signals shutdown."""
        self._shutdown_event.set()
        if self.httpx_client:
            await self.httpx_client.aclose()  # Close the httpx.AsyncClient
        if self.aiohttp_session:
            await self.aiohttp_session.close()  # Close the aiohttp.ClientSession


def format_time(timestamp):
    """Convert a Unix timestamp to a human-readable ISO 8601 format."""
    if not timestamp:
        return None
    return (
        datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc).isoformat()
        + "Z"
    )


def validate_fps(fps: float, default_fps: float, maximum_fps: float = None):
    """
    Ensure fps is valid, falling back to a default value if invalid.

    Args:
        fps (float): The frames per second value to validate.
        default_fps (float): The default FPS to use if validation fails.
        maximum_fps (float, optional): The maximum FPS allowed. Defaults to None.

    Returns:
        float: The validated FPS value.
    """
    try:
        fps = float(fps)
        if fps <= 0:
            raise ValueError("Frame rate must be positive.")
        if maximum_fps is not None:
            fps = min(fps, maximum_fps)  # Ensure FPS does not exceed maximum
        return fps
    except (ValueError, TypeError):
        # Log a warning and return the default FPS if validation fails
        logger.warning(
            f"Invalid frame rate provided: {fps}. Using default: {default_fps}"
        )
        return default_fps


def calculate_average_fps(intervals):
    """
    Calculate the average frame rate based on given intervals.

    Args:
        intervals (deque): A deque containing time intervals between frames.

    Returns:
        float or None: The calculated average FPS or None if not enough data.
    """
    if len(intervals) > 1:
        avg_interval = sum(intervals) / len(intervals)  # Average time between frames
        if avg_interval > 0:
            return 1.0 / avg_interval  # Convert interval to FPS
    return None  # Return None if not enough data to calculate FPS


async def maintain_frame_rate(image_id, target_fps, start_time):
    """
    Ensure that the frame fetching maintains the target frames per second.

    Args:
        image_id (str): The identifier for the image.
        target_fps (float): The target frames per second to maintain.
        start_time (float): The timestamp when the fetch attempt started.
    """
    # Cap target_fps to a reasonable maximum, e.g., 60 FPS
    target_fps = min(target_fps, MAX_FPS)
    min_delay = 1.0 / target_fps  # Minimum delay between frames

    # Calculate elapsed time since the fetch started
    elapsed = time.time() - start_time
    sleep_duration = max(0, min_delay - elapsed)  # Calculate how long to sleep

    if elapsed > min_delay:
        # Log a debug message if the fetch took longer than the frame interval
        logger.debug(
            f"Unable to keep up with frame rate for image_id '{image_id}'. "
            f"Elapsed time {elapsed:.2f}s exceeds frame interval {min_delay:.2f}s."
        )

    await asyncio.sleep(sleep_duration)  # Sleep to maintain the target FPS


def sanitize_image_id(image_id: str) -> str:
    """
    Sanitize and validate the image_id to prevent misuse.

    Args:
        image_id (str): The image identifier to sanitize.

    Returns:
        str: The sanitized image_id.

    Raises:
        ValueError: If the image_id contains invalid characters.
    """
    IMAGE_ID_REGEX = re.compile(r"^[a-zA-Z0-9_-]+$")
    if IMAGE_ID_REGEX.match(image_id):
        return image_id
    else:
        raise ValueError(
            f"Invalid image_id '{image_id}'. Must be alphanumeric with underscores or hyphens."
        )


async def assert_valid_image_data(image_data: bytes, validation_method: str) -> bytes:
    """
    Validate and possibly convert the image data based on the specified validation method.

    Args:
        image_data (bytes): The image data to validate.
        image_validation (str): The validation method ('none', 'valid_jpeg', 'zero_bytes', 'conversion').

    Returns:
        bytes: The validated (and possibly converted) image data.

    Raises:
        ValueError: If the image data is invalid according to the validation method.
    """
    match validation_method:
        case "none":
            return image_data  # No validation or conversion required

        case "zero_bytes":
            if len(image_data) == 0:
                raise ValueError("Downloaded image is empty (0 bytes)")
            return image_data

        case "valid_jpeg":
            try:
                def validate_jpeg(data):
                    image = Image.open(BytesIO(data))
                    image.verify()
                    if image.format != "JPEG":
                        raise ValueError(f"Image is not a JPEG, but {image.format}")
                    return data

                # Run synchronous validation in a separate thread
                validated_data = await asyncio.to_thread(validate_jpeg, image_data)
                return validated_data
            except Exception as e:
                raise ValueError(f"Invalid JPEG image data: {e}")

        case "conversion":
            try:
                def convert_to_yuv420(data: bytes) -> bytes:
                    with Image.open(BytesIO(data)) as img:
                        # Convert image to YCbCr (equivalent to YUV)
                        ycbcr_img = img.convert("YCbCr")
                        
                        # Save the image to a BytesIO buffer with JPEG format and 4:2:0 subsampling
                        output_io = BytesIO()
                        ycbcr_img.save(output_io, format="JPEG", subsampling=2, quality=95)
                        converted_data = output_io.getvalue()
                        if not converted_data:
                            raise ValueError("Converted image is empty.")
                        return converted_data

                # Run the conversion in a separate thread
                converted_image_data = await asyncio.to_thread(convert_to_yuv420, image_data)
                logger.info("Image successfully converted to YUV 4:2:0 format.")
                return converted_image_data

            except Exception as e:
                raise ValueError(f"Failed to convert image to YUV 4:2:0: {e}")
    
        case _:
            raise ValueError(f"Unknown validation method '{validation_method}'")


async def get_image_data(
    image_id: str,
    image_timeout_seconds: float,
    image_timeout_behavior: str,
    stale_image_data: bytes,
    data: bytes = None,
) -> tuple[bytes, bool]:
    """
    Retrieve the current image data based on the image_timeout_behavior.

    Args:
        image_id (str): The identifier for the image.
        image_timeout_seconds (float): Timeout threshold for stale images.
        image_timeout_behavior (str): Behavior when images become stale.
        stale_image_data (bytes): Data to use when images are stale.
        data (bytes, optional): The latest image data. Defaults to None.

    Returns:
        tuple[bytes, bool]: A tuple containing the image data and a boolean indicating
                            whether to disconnect (True) or not (False).
    """
    frame = frames[image_id]
    if frame is None:
        frame = Frame()
        frames[image_id] = frame

    async with frame.lock:
        if data is None:
            data = frame.data  # Latest image data
            last_updated = frame.last_updated  # Timestamp of the last update
        else:
            # If data is provided, use it directly (useful for streams)
            last_updated = time.time()

    # Determine if the image data is stale based on the timeout
    offline = data is None or time.time() - last_updated > image_timeout_seconds

    if offline:
        # Log that the image is stale
        logger.debug(f"Latest image for '{image_id}' is not available.")
        # Use match-case for handling image_timeout_behavior
        match image_timeout_behavior:
            case "disconnect":
                return (None, True)
            case "stale_image":
                return (stale_image_data, False)
            case "last_frame":
                if data is None:
                    return (stale_image_data, False)
                else:
                    return (data, False)
            case _:
                # Log a warning for unknown behaviors and default to disconnect
                logger.warning(
                    f"Unknown stale image behavior '{image_timeout_behavior}' for image '{image_id}'."
                )
                return (None, True)

    return (data, False)  # Return the valid image data and no need to disconnect


# ---------------- FastAPI Endpoints ---------------- #


@app.get("/images/{image_id}")
async def serve_latest_frame(image_id: str):
    """Serve the latest image for a given key as a JPEG file."""
    try:
        sanitized_image_id = sanitize_image_id(image_id)
    except ValueError as ve:
        logger.warning(f"Invalid image_id requested: '{image_id}'")
        raise HTTPException(status_code=400, detail=str(ve))

    image_config = images_config.get(
        sanitized_image_id
    )  # Retrieve the image configuration
    if image_config is None:
        # If the image_id is not found in configurations, return 404
        logger.warning(f"Image requested for unknown image_id: '{sanitized_image_id}'")
        raise HTTPException(status_code=404, detail="Image not found.")

    # Extract necessary configuration parameters
    image_timeout_seconds = image_config["image_timeout_seconds"]
    image_timeout_behavior = image_config["image_timeout_behavior"]
    stale_image_data = image_config["stale_image_data"]

    try:
        data, should_disconnect = await get_image_data(
            sanitized_image_id,
            image_timeout_seconds,
            image_timeout_behavior,
            stale_image_data,
            data=None,  # No data is provided; fetch from frame
        )
        if should_disconnect:
            # If behavior is to disconnect, raise a 503 Service Unavailable
            raise HTTPException(status_code=503, detail="Image not available.")
    except HTTPException as he:
        raise he

    return Response(
        content=data, media_type="image/jpeg"
    )  # Return the image as a JPEG response


@app.get("/images/{image_id}/stream")
async def serve_image_stream(request: Request, image_id: str, frame_rate: float = None):
    """Serve the latest image for a given key as an MJPEG stream."""
    try:
        sanitized_image_id = sanitize_image_id(image_id)
    except ValueError as ve:
        logger.warning(f"Invalid image_id requested for stream: '{image_id}'")
        raise HTTPException(status_code=400, detail=str(ve))

    image_config = images_config.get(
        sanitized_image_id
    )  # Retrieve the image configuration
    if image_config is None:
        # If the image_id is not found in configurations, return 404
        logger.warning(f"Stream requested for unknown image_id: '{sanitized_image_id}'")
        raise HTTPException(status_code=404, detail="Image not found.")

    async def stream_mjpeg(
        request: Request,
        image_id: str,
        requested_fps: float,
        stream_fps: float,
        fetch_maximum_fps: float,
        image_timeout_seconds: float,
        image_timeout_behavior: str,
        stale_image_data: bytes,
    ):
        """
        Asynchronous generator function for MJPEG streaming of images from memory.

        This generator yields image frames in the MJPEG format, adhering to the specified frame rate.

        Args:
            request (Request): The incoming HTTP request.
            image_id (str): The identifier for the image to stream.
            requested_fps (float): The FPS requested by the client.
            stream_fps (float): The actual FPS being streamed.
            fetch_maximum_fps (float): The maximum FPS allowed for fetching.
            image_timeout_seconds (float): Timeout threshold for stale images.
            image_timeout_behavior (str): Behavior when images become stale.
            stale_image_data (bytes): Data to use when images are stale.
        """
        # Validate and adjust the frame rate based on configuration limits
        stream_fps = validate_fps(
            requested_fps if requested_fps else stream_fps,
            default_fps=stream_fps,
            maximum_fps=None,
        )

        if requested_fps and stream_fps > fetch_maximum_fps:
            # Log if the requested streaming FPS exceeds the maximum fetch FPS
            logger.info(
                f"Streaming rate {stream_fps} fps exceeds maximum fetch rate {fetch_maximum_fps}."
            )

        # Get client information for logging and identification
        client_host = request.client.host
        client_port = request.client.port
        client_id = f"{client_host}:{client_port}"

        stream_start_time = time.time()  # Record the start time of the stream
        stream = Stream(
            requested_fps=requested_fps if requested_fps else stream_fps,
            stream_fps=stream_fps,
            start_time=stream_start_time,
        )

        # Acquire per-image lock to safely modify stream tracking structures
        stream_lock = streams_locks[image_id]
        async with stream_lock:
            streams[image_id][client_id] = stream  # Register the new stream

            # Update the peak_stream_fps based on current active streams
            image_active_streams = streams[image_id]
            streams_peak_fps[image_id] = max(
                stream.stream_fps for stream in image_active_streams.values()
            )

        logger.info(
            f"Client {client_id} connected to stream for '{image_id}' at {stream_fps} fps."
        )

        try:
            while True:
                frame_start_time = (
                    time.time()
                )  # Record the start time of frame processing

                # Extract necessary configuration parameters
                image_timeout_seconds = image_timeout_seconds
                image_timeout_behavior = image_timeout_behavior
                stale_image_data = stale_image_data

                # Use the helper function to get image data and determine if disconnect is needed
                data, should_disconnect = await get_image_data(
                    image_id,
                    image_timeout_seconds,
                    image_timeout_behavior,
                    stale_image_data,
                    data=None,  # No data is provided; fetch from frame
                )

                if should_disconnect:
                    # If behavior is to disconnect, terminate the stream
                    logger.info(
                        f"Disconnecting stream for client {client_id} due to stale image."
                    )
                    break

                if data is None:
                    # Log a warning if no valid frame is available
                    logger.warning(f"No valid frame available for '{image_id}'.")
                    # Depending on the behavior, either send the stale image or terminate
                    if image_timeout_behavior in ["stale_image", "last_frame"]:
                        data = stale_image_data
                    else:
                        break

                content_length = len(data)  # Calculate the length of the image data

                # Yield the image data in MJPEG frame format
                yield (
                    b"--frame\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Content-Length: "
                    + str(content_length).encode()
                    + b"\r\n\r\n"
                    + data
                    + b"\r\n"
                )

                # Calculate and update average_stream_fps based on frame intervals
                current_time = time.time()
                if stream.last_frame_time > 0.0:
                    interval = current_time - stream.last_frame_time
                    if interval > 0:
                        stream.frame_intervals.append(
                            interval
                        )  # Store interval for FPS calculation
                stream.last_frame_time = current_time  # Update the last frame time

                # Check if the client has disconnected to gracefully terminate the stream
                if await request.is_disconnected():
                    logger.info(
                        f"Client {client_id} disconnected from stream for '{image_id}'."
                    )
                    break

                # Maintain the frame rate by ensuring appropriate delays
                await maintain_frame_rate(image_id, stream_fps, frame_start_time)

        except asyncio.CancelledError:
            # Handle client disconnection due to cancellation
            logger.info(
                f"Client {client_id} disconnected from stream for '{image_id}'."
            )
        except Exception as e:
            # Log any unexpected errors during streaming
            logger.error(
                f"Error in MJPEG stream for '{image_id}' and client {client_id}: {e}"
            )
        finally:
            # Remove the stream from tracking structures upon termination
            async with stream_lock:
                streams[image_id].pop(client_id, None)  # Remove the client stream
                # Recalculate the peak_stream_fps after removing the stream
                image_active_streams = streams[image_id]
                if image_active_streams:
                    streams_peak_fps[image_id] = max(
                        stream.stream_fps for stream in image_active_streams.values()
                    )
                else:
                    streams_peak_fps[image_id] = (
                        None  # Set to None if no active streams remain
                    )

            logger.info(f"Stream for '{image_id}' terminated for client {client_id}.")

    return StreamingResponse(
        stream_mjpeg(
            request,
            sanitized_image_id,
            requested_fps=frame_rate,
            stream_fps=image_config["stream_fps"],
            fetch_maximum_fps=image_config["fetch_maximum_fps"],
            image_timeout_seconds=image_config["image_timeout_seconds"],
            image_timeout_behavior=image_config["image_timeout_behavior"],
            stale_image_data=image_config["stale_image_data"],
        ),
        media_type="multipart/x-mixed-replace; boundary=frame",  # MIME type for MJPEG
    )


@app.get("/images")
async def list_available_images(request: Request):
    """Report the health status of all streams, including active connections."""
    health_info = {}  # Dictionary to hold health information for each image

    # Get base URL to construct accessible URLs
    base_url = str(request.base_url).rstrip("/")

    # Iterate over all configured images to gather their health status
    for image_id, image_config in images_config.items():
        # Read last_updated and fetch_intervals with thread safety
        frame = frames[image_id]
        if frame is None:
            frame = Frame()
            frames[image_id] = frame

        async with frame.lock:
            last_updated = frame.last_updated
            average_fetch_fps = calculate_average_fps(frame.frame_intervals)

        # Prepare a copy of the image configuration without large data
        image_config_copy = image_config.copy()
        stale_image_data = image_config_copy.get("stale_image_data")
        if stale_image_data is not None:
            image_config_copy["stale_image_data_length"] = len(stale_image_data)
            del image_config_copy[
                "stale_image_data"
            ]  # Remove the actual data to reduce payload
        else:
            image_config_copy["stale_image_data_length"] = 0

        # Collect active connections for this image_id
        async with streams_locks[image_id]:
            stream = streams[image_id].copy()
            peak_stream_fps = streams_peak_fps[image_id]

        stream_list = []  # List to hold information about each active stream
        for conn_id, conn_data in stream.items():
            # Calculate average stream FPS using frame intervals
            average_stream_fps = calculate_average_fps(conn_data.frame_intervals)

            # Append stream information to the list
            stream_list.append(
                {
                    "client_id": conn_id,
                    "start_time": format_time(conn_data.start_time),
                    "requested_fps": conn_data.requested_fps,
                    "stream_fps": conn_data.stream_fps,
                    "average_stream_fps": average_stream_fps,
                }
            )

        # Populate the health_info dictionary with gathered data
        health_info[image_id] = {
            "image_url": f"{base_url}/images/{image_id}",
            "stream_url": f"{base_url}/images/{image_id}/stream",
            "last_updated": format_time(last_updated),
            "average_fetch_fps": average_fetch_fps,
            "peak_stream_fps": peak_stream_fps,
            "config": image_config_copy,
            "streams": stream_list,
        }

    def format_floats(obj):
        """Helper function to format float values to three decimal places."""
        if isinstance(obj, float):
            return round(obj, 3)
        elif isinstance(obj, dict):
            return {k: format_floats(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [format_floats(i) for i in obj]
        return obj

    # Format float values in health_info
    formatted_health_info = format_floats(health_info)

    # Convert health_info to pretty-printed JSON
    pretty_json = json.dumps(formatted_health_info, indent=4)

    # Return the pretty-printed JSON response
    return Response(content=pretty_json, media_type="application/json", status_code=200)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


# ---------------- Startup and Shutdown Event Handlers ---------------- #

async def load_stale_image_from_file_system(
    image_id: str,
    image_stale_image_path: str = None,
    default_fallback_image_data: bytes = b""
) -> bytes:
    """
    Asynchronously load and convert the offline (stale) image for a given image_id to YUV 4:2:0.

    The function prioritizes the image-specific stale_image_path. If it's not provided
    or fails to load, it falls back to the server-level stale_image_path. If both
    paths are unavailable or loading fails, it uses the predefined default fallback image data.

    After loading, the image is converted to YUV 4:2:0 chroma subsampling.

    Args:
        image_id (str): Identifier for the image.
        image_stale_image_path (str, optional): Path to the image-specific stale image.
            Defaults to None.
        default_fallback_image_data (bytes, optional): Predefined fallback image data.
            Defaults to empty bytes.

    Returns:
        bytes: The converted stale image data in YUV 4:2:0 format.
    """
    # Determine the fallback path: image-specific > server-level
    fallback_path = image_stale_image_path 

    if fallback_path:
        logger.debug(
            f"Image '{image_id}': Attempting to load stale image from '{fallback_path}'."
        )
        try:
            async with aiofiles.open(fallback_path, "rb") as f:
                stale_image_data = await f.read()
                if not stale_image_data:
                    raise ValueError("Offline image file is empty.")
            logger.info(
                f"Image '{image_id}': Successfully loaded stale image from '{fallback_path}'."
            )
        except Exception as e:
            logger.error(
                f"Image '{image_id}': Failed to load stale image from '{fallback_path}': {e}"
            )
            stale_image_data = None
    else:
        stale_image_data = None

    # Use default fallback image data if loading from file fails or paths are not provided
    if not stale_image_data and default_fallback_image_data:
        logger.info(
            f"Image '{image_id}': Using predefined fallback offline image."
        )
        stale_image_data = default_fallback_image_data

    # If no fallback is available, log a critical error and return empty bytes
    if not stale_image_data:
        logger.critical(
            f"Image '{image_id}': No valid stale image available. Returning empty bytes."
        )
        return b""

    try:
        # Convert the loaded image data to YUV 4:2:0
        validated_data = await assert_valid_image_data(
            stale_image_data, "conversion"
        )

        logger.info(
            f"Image '{image_id}': Successfully converted stale image to YUV 4:2:0."
        )
        return validated_data

    except Exception as e:
        logger.error(
            f"Image '{image_id}': Failed to convert stale image to YUV 4:2:0: {e}"
        )
        # As a fallback, return the original stale_image_data
        return stale_image_data


async def startup_event_handler():
    """Load configuration and start image polling tasks on startup."""
    global config

    # Load configuration from the YAML file
    try:
        async with aiofiles.open(CONFIG_FILE, "r") as f:
            content = await f.read()
            loaded_config = yaml.safe_load(content)
            if loaded_config is None:
                loaded_config = {"server": {}, "images": {}}
            config["server"].update(loaded_config.get("server", {}))
            config["images"].update(loaded_config.get("images", {}))
            logger.info("Configuration loaded successfully from config.yaml.")
    except FileNotFoundError:
        # Warn if the configuration file is not found and proceed with defaults
        logger.warning(
            f"Configuration file '{CONFIG_FILE}' not found. Using default configurations."
        )
    except yaml.YAMLError as e:
        # Log YAML parsing errors and proceed with defaults
        logger.error(
            f"Error parsing configuration file: {e}. Using default configurations."
        )
    except Exception as e:
        # Log any unexpected errors during configuration loading
        logger.error(
            f"Unexpected error loading configuration: {e}. Using default configurations."
        )

    # Extract server configurations with defaults
    server_config = config.get("server", {})
    default_ignore_certificate_errors = server_config.get(
        "ignore_certificate_errors", DEFAULT_IGNORE_CERTIFICATE_ERRORS
    )
    default_fetch_agent_string = server_config.get(
        "fetch_agent_string", DEFAULT_FETCH_AGENT_STRING
    )  # Extract server-level fetch agent string

    # Validate and set default fetch_maximum_fps
    default_fetch_maximum_fps = validate_fps(
        server_config.get("fetch_maximum_fps", DEFAULT_FETCH_MAXIMUM_FPS),
        DEFAULT_FETCH_MAXIMUM_FPS,
        maximum_fps=None,
    )
    # Validate and set default fetch_minimum_fps with maximum constraint
    default_fetch_minimum_fps = validate_fps(
        server_config.get("fetch_minimum_fps", DEFAULT_FETCH_MINIMUM_FPS),
        DEFAULT_FETCH_MINIMUM_FPS,
        maximum_fps=default_fetch_maximum_fps,
    )
    # Validate and set default stream_fps
    stream_fps = validate_fps(
        server_config.get("stream_fps", DEFAULT_STREAM_FPS),
        DEFAULT_STREAM_FPS,
        maximum_fps=None,
    )

    # Extract and set default image timeout configurations
    default_image_timeout_seconds = server_config.get(
        "image_timeout_seconds", DEFAULT_STALE_FRAME_TIMEOUT_SECONDS
    )
    default_image_timeout_behavior = server_config.get(
        "image_timeout_behavior", DEFAULT_STALE_FRAME_ACTION
    )
    default_image_validation = server_config.get(
        "image_validation", DEFAULT_IMAGE_VALIDATION
    )

    # Load the default offline image if a path is specified
    default_stale_image_path = server_config.get("stale_image_path", None)

    # Start image polling tasks for each configured image
    await start_image_polling_tasks(
        stream_fps=stream_fps,
        default_fetch_minimum_fps=default_fetch_minimum_fps,
        default_fetch_maximum_fps=default_fetch_maximum_fps,
        default_image_timeout_seconds=default_image_timeout_seconds,
        default_image_timeout_behavior=default_image_timeout_behavior,
        default_image_validation=default_image_validation,
        default_ignore_certificate_errors=default_ignore_certificate_errors,
        default_stale_image_path=default_stale_image_path,
        default_fetch_agent_string=default_fetch_agent_string,  # Pass server-level fetch agent string
    )


async def start_image_polling_tasks(
    stream_fps: float,
    default_fetch_minimum_fps: float,
    default_fetch_maximum_fps: float,
    default_image_timeout_seconds: float,
    default_image_timeout_behavior: str,
    default_image_validation: str,
    default_ignore_certificate_errors: bool,
    default_stale_image_path: str,
    default_fetch_agent_string: str,  # Added parameter for server-level fetch agent string
):
    """Start polling tasks for each image."""

    global images_config

    loaded_images_config = config.get("images", {})  # Get image-specific configurations
    for image_id, image_config_raw in loaded_images_config.items():
        try:
            # Sanitize and validate image_id
            sanitized_image_id = sanitize_image_id(image_id)

            # Initialize variables with defaults
            url = image_config_raw.get("url")
            if not url:
                # If URL is missing, raise an error
                raise ValueError(f"Missing 'url' for image_id '{sanitized_image_id}'")

            # Validate URL
            try:
                parsed_url = URL(url)
                if not parsed_url.scheme.startswith("http"):
                    raise ValueError(
                        f"URL scheme must be HTTP or HTTPS for image_id '{sanitized_image_id}'."
                    )
            except Exception as ve:
                raise ValueError(
                    f"Invalid URL for image_id '{sanitized_image_id}': {ve}"
                )

            # Apply defaults and validate frame rates
            fetch_maximum_fps_config = image_config_raw.get(
                "fetch_maximum_fps", default_fetch_maximum_fps
            )
            validated_fetch_maximum_fps = validate_fps(
                fetch_maximum_fps_config, default_fetch_maximum_fps, maximum_fps=None
            )

            validated_fetch_minimum_fps = validate_fps(
                image_config_raw.get("fetch_minimum_fps", default_fetch_minimum_fps),
                default_fetch_minimum_fps,
                maximum_fps=validated_fetch_maximum_fps,
            )

            validated_stream_fps = validate_fps(
                image_config_raw.get("stream_fps", stream_fps),
                stream_fps,
                maximum_fps=None,
            )

            # Determine fetch_agent_string, image-level overrides server-level
            fetch_agent_string = image_config_raw.get(
                "fetch_agent_string", default_fetch_agent_string
            )

            # Prepare the image_config dictionary with all necessary settings
            image_config = {
                "url": url,
                "ignore_certificate_errors": image_config_raw.get(
                    "ignore_certificate_errors", default_ignore_certificate_errors
                ),
                "stream_fps": validated_stream_fps,
                "fetch_minimum_fps": validated_fetch_minimum_fps,
                "fetch_maximum_fps": validated_fetch_maximum_fps,
                "image_timeout_seconds": image_config_raw.get(
                    "image_timeout_seconds", default_image_timeout_seconds
                ),
                "image_timeout_behavior": image_config_raw.get(
                    "image_timeout_behavior", default_image_timeout_behavior
                ),
                "stale_image_path": image_config_raw.get("stale_image_path", default_stale_image_path),
                "image_validation": image_config_raw.get(
                    "image_validation", default_image_validation
                ),
                "auth_type": image_config_raw.get("auth_type", None),
                "username": image_config_raw.get("username", None),
                "password": image_config_raw.get("password", None),
                "token": image_config_raw.get("token", None),
                "fetch_agent_string": fetch_agent_string,  # Add fetch_agent_string to image config
            }

            # Validate stale image behavior
            if image_config["image_timeout_behavior"] not in [
                "disconnect",
                "stale_image",
                "last_frame",
            ]:
                # Log a warning and set to default if an invalid behavior is specified
                logger.warning(
                    f"Invalid image_timeout_behavior '{image_config['image_timeout_behavior']}' for image_id '{sanitized_image_id}'. Using default '{default_image_timeout_behavior}'."
                )
                image_config["image_timeout_behavior"] = default_image_timeout_behavior

            # Validate image validation method
            if image_config["image_validation"] not in [
                "none",
                "zero_bytes",
                "valid_jpeg",
                "conversion",
            ]:
                # Log a warning and set to default if an invalid validation method is specified
                logger.warning(
                    f"Invalid image_validation '{image_config['image_validation']}' for image_id '{sanitized_image_id}'. Using default '{default_image_validation}'."
                )
                image_config["image_validation"] = default_image_validation

            # Validate authentication type
            if image_config["auth_type"] not in ["basic", "digest", "bearer", None]:
                # Log a warning and set to None if an invalid auth type is specified
                logger.warning(
                    f"Invalid auth_type '{image_config['auth_type']}' for image_id '{sanitized_image_id}'. Skipping authentication."
                )
                image_config["auth_type"] = None

            # Load offline image data based on the configuration
            stale_image_data = await load_stale_image_from_file_system (
                    image_id=sanitized_image_id,
                    image_stale_image_path=image_config.get("stale_image_path"),
                    default_fallback_image_data=DEFAULT_FALLBACK_IMAGE_DATA
            )

            image_config["stale_image_data"] = (
                stale_image_data  # Assign the loaded stale image data
            )

            # Update the global images_config with the current image's configuration
            images_config[sanitized_image_id] = image_config

            # Initialize connection tracking structures for the image_id
            streams[
                sanitized_image_id
            ]  # Ensures the key exists in the streams dictionary

            # Initialize Frame instance for the image_id to store image data and fetch intervals
            frames[sanitized_image_id] = Frame()

            # Initialize ImageFetcherManager with the image's configuration
            fetcher_manager: ImageFetcherManager = ImageFetcherManager(
                image_id=sanitized_image_id,
                url=image_config["url"],
                ignore_certificate_errors=image_config["ignore_certificate_errors"],
                fetch_minimum_fps=image_config["fetch_minimum_fps"],
                fetch_maximum_fps=image_config["fetch_maximum_fps"],
                image_timeout_seconds=image_config["image_timeout_seconds"],
                image_validation=image_config["image_validation"],
                fetch_agent_string=image_config[
                    "fetch_agent_string"
                ],  # Pass fetch_agent_string
                auth_type=image_config.get("auth_type"),
                username=image_config.get("username"),
                password=image_config.get("password"),
                token=image_config.get("token"),
            )

            # Store the fetcher_manager instance for shutdown handling
            fetcher_managers[sanitized_image_id] = fetcher_manager

            # Start the fetching task using ImageFetcherManager
            asyncio.create_task(fetcher_manager.fetch_frames())

            logger.info(
                f"Started polling for '{image_config['url']}' with image_id '{sanitized_image_id}'."
            )

        except Exception as e:
            # Log critical errors if fetching tasks fail to start
            logger.critical(f"Failed to start fetch task for '{image_id}': {e}")


async def shutdown_event_handler():
    """Handle application shutdown by closing all ImageFetcherManager instances."""
    logger.info("Shutting down application. Closing all fetcher managers.")
    shutdown_tasks = []
    for image_id, manager in fetcher_managers.items():
        logger.info(f"Closing fetcher manager for image_id '{image_id}'.")
        shutdown_tasks.append(manager.close())
    if shutdown_tasks:
        await asyncio.gather(*shutdown_tasks)
    logger.info("All fetcher managers have been closed.")


app.add_event_handler("shutdown", shutdown_event_handler)
app.add_event_handler("startup", startup_event_handler)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",  # Bind to all interfaces
        port=8080,  # Port number
        log_config=None,  # Use the existing logging configuration
    )
