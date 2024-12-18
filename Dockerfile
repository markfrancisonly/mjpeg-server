# Use lightweight Python image based on Debian slim
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install support for the most common image formats (JPEG, PNG, TIFF, WebP) 
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libjpeg62-turbo-dev \
    libpng-dev \
    zlib1g-dev \
    libtiff5-dev \
    libwebp-dev \
    && rm -rf /var/lib/apt/lists/*

COPY mjpeg-server/requirements.txt /mjpeg-server/requirements.txt
WORKDIR /mjpeg-server

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt 

COPY mjpeg-server /mjpeg-server

# Server HTTP port
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
  CMD curl --silent --fail http://127.0.0.1:8080/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]