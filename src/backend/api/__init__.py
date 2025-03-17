from .app import create_app, configure_app
from ..utils.logging.logger import get_logger

# Initialize logger
logger = get_logger(__name__)

# Create FastAPI application instance
app = create_app()

__all__ = ["app", "create_app", "configure_app"]

logger.info("API initialized successfully")