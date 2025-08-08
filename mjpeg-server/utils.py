import time
import datetime
import re
import asyncio
from io import BytesIO
from PIL import Image
from collections import deque
from logging_config import logger

MAX_FPS = 240.0  # Define constants for default values

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
                        ycbcr_img.save(
                            output_io, format="JPEG", subsampling=2, quality=95
                        )
                        converted_data = output_io.getvalue()
                        if not converted_data:
                            raise ValueError("Converted image is empty.")
                        return converted_data

                # Run the conversion in a separate thread
                converted_image_data = await asyncio.to_thread(
                    convert_to_yuv420, image_data
                )
                logger.info("Image successfully converted to YUV 4:2:0 format.")
                return converted_image_data
            except Exception as e:
                raise ValueError(f"Failed to convert image to YUV 4:2:0: {e}")
        case _:
            raise ValueError(f"Unknown validation method '{validation_method}'")
