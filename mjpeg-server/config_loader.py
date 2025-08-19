import asyncio

import aiofiles
import yaml
from fetcher import ImageFetcherManager
from logging_config import logger
from models import Frame
from state import config, fetcher_managers, frames, images_config, streams
from utils import (
    DEFAULT_FALLBACK_IMAGE_DATA,
    assert_valid_image_data,
    sanitize_image_id,
    validate_fps,
)
from yarl import URL

CONFIG_FILE = "config.yaml"  # Configuration file path
DEFAULT_IGNORE_CERTIFICATE_ERRORS = False  # Whether to ignore SSL certificate errors
DEFAULT_STREAM_FPS = 5.0  # Default frames per second for streaming
DEFAULT_FETCH_MINIMUM_FPS = 1.0  # Minimum fetch FPS to maintain
DEFAULT_FETCH_MAXIMUM_FPS = 30.0  # Maximum fetch FPS allowed
DEFAULT_STALE_FRAME_TIMEOUT_SECONDS = 30.0  # Timeout in seconds for stale frames
DEFAULT_STALE_FRAME_ACTION = (
    "disconnect"  # Options: 'disconnect', 'stale_image', 'last_frame'
)
DEFAULT_IMAGE_VALIDATION = (
    "valid_jpeg"  # Options: 'valid_jpeg', 'conversion', 'zero_bytes', 'none'
)
DEFAULT_FETCH_AGENT_STRING = "ImageFetcher/1.0"  # Default fetch agent string


async def load_stale_image_from_file_system(
    image_id: str,
    image_stale_image_path: str = None,
    default_fallback_image_data: bytes = b"",
) -> bytes:
    """
    Asynchronously load and convert the offline (stale) image for a given image_id to YUV 4:2:0.
    The function prioritizes the image-specific stale_image_path. If it's not provided or fails to load,
    it falls back to the server-level stale_image_path. If both paths are unavailable or loading fails,
    it uses the predefined default fallback image data. After loading, the image is converted to YUV 4:2:0
    chroma subsampling.

    Args:
        image_id (str): Identifier for the image.
        image_stale_image_path (str, optional): Path to the image-specific stale image. Defaults to None.
        default_fallback_image_data (bytes, optional): Predefined fallback image data. Defaults to empty bytes.

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
            logger.exception(
                f"Image '{image_id}': Failed to load stale image from '{fallback_path}': {e}"
            )
            stale_image_data = None
    else:
        stale_image_data = None

    # Use default fallback image data if loading from file fails or paths are not provided
    if not stale_image_data and default_fallback_image_data:
        logger.info(f"Image '{image_id}': Using predefined fallback offline image.")
        stale_image_data = default_fallback_image_data

    # If no fallback is available, log a critical error and return empty bytes
    if not stale_image_data:
        logger.critical(
            f"Image '{image_id}': No valid stale image available. Returning empty bytes."
        )
        return b""

    try:
        # Convert the loaded image data to YUV 4:2:0
        validated_data = await assert_valid_image_data(stale_image_data, "conversion")
        logger.info(
            f"Image '{image_id}': Successfully converted stale image to YUV 4:2:0."
        )
        return validated_data
    except Exception as e:
        logger.exception(
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
            loaded_config = {"mjpeg-server": {}, "images": {}}
        config["mjpeg-server"].update(loaded_config.get("mjpeg-server", {}))
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
    server_config = config.get("mjpeg-server", {})
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
                "stale_image_path": image_config_raw.get(
                    "stale_image_path", default_stale_image_path
                ),
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
            stale_image_data = await load_stale_image_from_file_system(
                image_id=sanitized_image_id,
                image_stale_image_path=image_config.get("stale_image_path"),
                default_fallback_image_data=DEFAULT_FALLBACK_IMAGE_DATA,
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

            logger.debug(
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