
# MJPEG Server 

FastAPI-based server for fetching and streaming static images as a reliable MJPEG stream, intended to be run in a docker container. May be used to provide a stable mjpeg stream for transcoding into a RTSP feed using [go2rtc](https://github.com/AlexxIT/go2rtc).

## Features

- Efficient and high performance web server
- Protects image source from being overloaded
- Fetch and stream FPS individually adjustable
- Provides image validation, conversion, and fallback image behaviors

### Server config example
```yaml
mjpeg-server:
  ignore_certificate_errors: true
  stream_fps: 15
  fetch_minimum_fps: 1
  fetch_maximum_fps: 5
  image_timeout_behavior: stale_image # disconnect, stale_image, or last_frame
  image_timeout_seconds: 30
  image_validation: valid_jpeg # valid_jpeg, conversion, zero_bytes, none
  stale_image_path: images/white_square.jpg

images:
  fully_desk_tablet:
    url: http://fully_desk_tablet:2323/?cmd=getCamshot&password=secret
    image_timeout_seconds: 5
   
```
### Docker compose

```yaml
services:
  mjpeg-server:
    user: 1000:1000
    container_name: mjpeg-server
    restart: always
    build: ./mjpeg-server/
    image: mjpeg-server:latest

    volumes:
      - ./config.yaml:/mjpeg-server/config.yaml
      - ./images/:/mjpeg-server/images/
      
    ports:
       - "8080:8080"

```

### RTSP stream using Go2rtc
```yaml
go2rtc:
  ffmpeg:
    mjpeg: -f mjpeg -use_wallclock_as_timestamps 1 -fflags nobuffer -flags low_delay -i {input}
   streams:
    fully_desk_tablet:
      - ffmpeg:http://docker:8080/images/fully_desk_tablet/stream#input=mjpeg#video=h264
  api:
    listen: :1984
    origin: '*'
  rtsp:
    listen: :8554 

```
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
 


