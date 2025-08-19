import asyncio
import json
import time

from config_loader import shutdown_event_handler, startup_event_handler
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from logging_config import logger
from models import Frame, Stream
from state import frames, images_config, streams, streams_locks, streams_peak_fps
from utils import (
    calculate_average_fps,
    format_time,
    maintain_frame_rate,
    sanitize_image_id,
    validate_fps,
)

app = FastAPI()  # Initialize the FastAPI application


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

        if data is None:
            # Lockless snapshot read (bytes assignment is atomic; minor staleness is acceptable here)
            data = frame.data
            last_updated = frame.last_updated
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
                logger.debug(
                    f"Unknown stale image behavior '{image_timeout_behavior}' for image '{image_id}'."
                )
                return (None, True)

    return (data, False)  # Return the valid image data and no need to disconnect


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
            logger.debug(
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

        logger.debug(
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
                    logger.debug(
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
                    logger.debug(
                        f"Client {client_id} disconnected from stream for '{image_id}'."
                    )
                    break

                # Maintain the frame rate by ensuring appropriate delays
                await maintain_frame_rate(image_id, stream_fps, frame_start_time)
        except asyncio.CancelledError:
            # Handle client disconnection due to cancellation
            logger.debug(
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

            logger.debug(f"Stream for '{image_id}' terminated for client {client_id}.")

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
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
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


app.add_event_handler("startup", startup_event_handler)
app.add_event_handler("shutdown", shutdown_event_handler)
