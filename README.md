
# FastAPI Image Streaming Server

This repository contains a FastAPI-based server for efficient image fetching, validation, and streaming in both static image format and MJPEG streams.

## Features

- **Image Streaming**: Serve static images and MJPEG streams.
- **Asynchronous Fetching**: Efficient image fetching with configurable FPS rates.
- **Validation**: Validate and convert images to ensure compatibility.
- **Logging**: Custom logging configuration for detailed debugging.
- **Health Check**: Endpoints to monitor the health and status of image streams.

## Prerequisites

- Python 3.9+
- `pip` package manager

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/markfrancisonly/mjpeg-server.git
   cd fastapi-image-server
   ```

2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Create a `config.yaml` file for server and image configurations.

2. Start the server:

   ```bash
   python main.py
   ```

3. Access endpoints:
   - Static image: `/images/{image_id}`
   - MJPEG stream: `/images/{image_id}/stream`
   - List all images: `/images`
   - Health check: `/health`

## Configuration

The server uses a `config.yaml` file for defining the following:

- **Server settings** (e.g., timeout, FPS limits).
- **Image-specific settings** (e.g., URLs, validation methods).

Refer to the provided `config.example.yaml` for structure and examples.

## Endpoints

### Static Image
Retrieve the latest frame for an image ID:

```http
GET /images/{image_id}
```

### MJPEG Stream
Stream the latest frames in MJPEG format:

```http
GET /images/{image_id}/stream
```

### List Available Images
Get a summary of all configured images:

```http
GET /images
```

### Health Check
Check the server's status:

```http
GET /health
```

## Deployment

For production deployment, use a robust ASGI server such as `uvicorn` with multiple workers.

```bash
uvicorn main:app --host 0.0.0.0 --port 8080 --workers 4
```

## Contributing

This code needs to be modularized bfore contributions would be meaningful.
