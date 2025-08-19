import logging
import logging.config
import os


def _parse_log_level(value: str | int, default: int = logging.INFO) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    v = str(value).strip().upper()
    if v.isdigit():
        try:
            return int(v)
        except ValueError:
            return default
    mapping = {
        "CRIT": logging.CRITICAL,
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARN": logging.WARNING,
        "WARNING": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
        "TRACE": 5,  # custom low level if you want it
    }
    return mapping.get(v, default)


if logging.getLevelName(5) == "Level 5":
    logging.addLevelName(5, "TRACE")

    def _trace(self, msg, *args, **kwargs):
        if self.isEnabledFor(5):
            self._log(5, msg, args, **kwargs)

    logging.Logger.trace = _trace  # type: ignore[attr-defined]

_env_level_raw = os.getenv("LOG_LEVEL") or os.getenv("MJPEG_LOG_LEVEL")
_ENV_LEVEL = _parse_log_level(_env_level_raw, default=logging.INFO)

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "default": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "default": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "stream": "ext://sys.stdout",
            # level is inherited from the logger unless set here
        },
    },
    "root": {
        "level": _ENV_LEVEL,  # pick up env at startup for root
        "handlers": ["default"],
    },
    "loggers": {
        # Make uvicorn honor the env level too. Keep propagate=False so formatting stays predictable.
        "uvicorn": {"handlers": ["default"], "level": _ENV_LEVEL, "propagate": False},
        "uvicorn.error": {
            "handlers": ["default"],
            "level": _ENV_LEVEL,
            "propagate": False,
        },
        "uvicorn.access": {
            "handlers": ["default"],
            "level": "WARNING",
            "propagate": False,
        },
    },
}

logging.config.dictConfig(LOGGING_CONFIG)

# Use the ROOT logger for app-wide messages (module-specific loggers are still available via getLogger(__name__))
logger = logging.getLogger()


def _set_all_levels(level: int) -> None:
    """
    Apply level to root, its handlers, and all explicitly configured loggers + handlers.
    """
    # Root
    root = logging.getLogger()
    root.setLevel(level)
    for h in root.handlers:
        try:
            h.setLevel(level)
        except Exception:
            pass

    # Any named loggers we declared in LOGGING_CONFIG
    for name in LOGGING_CONFIG.get("loggers", {}).keys():
        lg = logging.getLogger(name)
        lg.setLevel(level)
        for h in lg.handlers:
            try:
                h.setLevel(level)
            except Exception:
                pass


def set_log_level(level_str_or_int: str | int) -> int:
    """
    Update the global logging level at runtime (root + known child loggers and their handlers).
    Returns the numeric level applied.
    """
    level = _parse_log_level(level_str_or_int, default=logging.INFO)
    _set_all_levels(level)
    return level


# Log what we applied (only visible if level <= DEBUG)
if _env_level_raw:
    logger.debug(
        "Applied LOG_LEVEL from env: %r -> %s",
        _env_level_raw,
        logging.getLevelName(_ENV_LEVEL),
    )
