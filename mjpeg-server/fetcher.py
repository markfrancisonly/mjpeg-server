import asyncio
import time
import httpx
import aiohttp
from yarl import URL
from logging_config import logger
from state import streams_locks, streams_peak_fps, frames
from models import Frame
from utils import maintain_frame_rate, assert_valid_image_data


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
        This method determines whether to use httpx or aiohttp based on the presence of authentication
        credentials and initiates the fetching process accordingly.
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
                await maintain_frame_rate(image_id, target_fetch_fps, fetch_start_time)

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
