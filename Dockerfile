FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libjpeg62-turbo \
    zlib1g \
    libpng16-16 \
    libwebp7 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /mjpeg-server

COPY ./mjpeg-server/requirements.txt ./requirements.txt
RUN pip install --upgrade pip --root-user-action=ignore \
    && pip install -r requirements.txt --root-user-action=ignore

COPY ./mjpeg-server /mjpeg-server
ENV PYTHONPATH=/mjpeg-server

EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --start-period=5s --retries=3 \
  CMD curl --silent --fail http://127.0.0.1:8080/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
