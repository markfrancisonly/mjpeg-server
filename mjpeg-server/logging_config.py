import logging
import logging.config
import sys

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
