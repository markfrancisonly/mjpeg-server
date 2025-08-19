import asyncio
import platform

try:
    import uvloop  # pyright: ignore[reportMissingImports]
except ImportError:
    uvloop = None
import uvicorn
from app import app

# Set uvloop as the default event loop policy for improved performance on Unix systems
if platform.system() != "Windows" and uvloop is not None:
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",  # Bind to all interfaces
        port=8080,  # Port number
        log_config=None,  # Use the existing logging configuration
    )
