import os
import sys
import argparse

import uvicorn  # version ^0.22.0
from dotenv import load_dotenv  # version ^1.0.0

from .config import get_config  # src/backend/config.py
from .constants import ENV_DEVELOPMENT, ENV_STAGING, ENV_PRODUCTION, DEFAULT_LOG_LEVEL  # src/backend/constants.py
from .logging_config import configure_logging  # src/backend/logging_config.py
from .utils.logging.logger import get_logger  # src/backend/utils/logging/logger.py
from .db import initialize_database  # src/backend/db/__init__.py
from .api.app import create_app  # src/backend/api/app.py

# Initialize logger
logger = get_logger(__name__)


def main() -> int:
    """Main entry point for the application"""
    # Parse command line arguments
    args = parse_args()

    # Load environment variables from .env file if it exists
    load_dotenv()

    # Initialize configuration
    config = get_config()

    # Configure logging based on environment and log level
    log_level = args.log_level or config.get("logging.level", DEFAULT_LOG_LEVEL)
    configure_logging(log_level=log_level)

    # Initialize database if --init-db flag is provided
    if args.init_db:
        seed_data = args.seed_db
        if not init_database(config=config, seed_data=seed_data):
            logger.error("Database initialization failed")
            return 1

    # Create and configure FastAPI application
    app = create_app()

    # If --run-server flag is provided, start the Uvicorn server
    if args.run_server:
        host = args.host or config.get("api.host", "0.0.0.0")
        port = args.port or config.get("api.port", 8000)
        reload = args.reload if args.reload is not None else config.get("api.reload", False)
        run_server(host=host, port=port, reload=reload)

    # Return exit code (0 for success)
    return 0


def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    # Create ArgumentParser instance
    parser = argparse.ArgumentParser(description="Run the self-healing data pipeline backend application")

    # Add --env argument for environment selection
    parser.add_argument("--env", type=str, help="Environment to run the application in (development, staging, production)")

    # Add --log-level argument for log level selection
    parser.add_argument("--log-level", type=str, help="Log level to use (DEBUG, INFO, WARNING, ERROR, CRITICAL)")

    # Add --config argument for custom config path
    parser.add_argument("--config", type=str, help="Path to the configuration file")

    # Add --init-db flag for database initialization
    parser.add_argument("--init-db", action="store_true", help="Initialize the database schema")

    # Add --seed-db flag for database seeding
    parser.add_argument("--seed-db", action="store_true", help="Seed the database with initial data")

    # Add --run-server flag to start the API server
    parser.add_argument("--run-server", action="store_true", help="Start the Uvicorn server")

    # Add --host argument for server host
    parser.add_argument("--host", type=str, help="Host for the Uvicorn server")

    # Add --port argument for server port
    parser.add_argument("--port", type=int, help="Port for the Uvicorn server")

    # Add --reload flag for development auto-reload
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development")

    # Parse and return arguments
    return parser.parse_args()


def init_database(seed_data: bool) -> bool:
    """Initialize the database schema and seed data if requested"""
    # Log database initialization start
    logger.info("Initializing database...")

    # Get database configuration from application config
    config = get_config()

    # Call db.initialize_database with configuration and seed_data flag
    try:
        success = initialize_database(config=config, seed_data=seed_data)
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False

    # Log success or failure of database initialization
    if success:
        logger.info("Database initialized successfully")
    else:
        logger.error("Database initialization failed")

    # Return success status
    return success


def run_server(host: str, port: int, reload: bool) -> None:
    """Start the Uvicorn server with the FastAPI application"""
    # Log server startup with host and port
    logger.info(f"Starting Uvicorn server on {host}:{port} with reload={reload}")

    # Configure Uvicorn with application, host, port, and reload flag
    uvicorn.run(
        "backend.api.app:app",  # Specify module:app for Uvicorn
        host=host,
        port=port,
        reload=reload,
        reload_dirs=["src"]  # Specify reload directories
    )

    # Handle keyboard interrupts gracefully
    try:
        pass
    except KeyboardInterrupt:
        logger.info("Uvicorn server stopped")


# Execute main function if script is run directly
if __name__ == "__main__":
    sys.exit(main())