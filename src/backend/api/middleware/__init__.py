"""Entry point for the API middleware package that exports middleware classes
and utility functions for authentication, CORS, error handling, and request
logging in the self-healing data pipeline API.
"""

from .auth_middleware import AuthMiddleware, JWTBearer, require_auth, require_role  # src/backend/api/middleware/auth_middleware.py
from .cors_middleware import CORSMiddleware, setup_cors_middleware  # src/backend/api/middleware/cors_middleware.py
from .error_middleware import ErrorMiddleware, get_request_id  # src/backend/api/middleware/error_middleware.py
from .logging_middleware import LoggingMiddleware  # src/backend/api/middleware/logging_middleware.py


__all__ = [
    "AuthMiddleware",
    "JWTBearer",
    "require_auth",
    "require_role",
    "CORSMiddleware",
    "setup_cors_middleware",
    "ErrorMiddleware",
    "LoggingMiddleware",
    "get_request_id"
]