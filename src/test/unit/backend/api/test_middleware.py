import pytest  # pytest ^7.0.0
from unittest.mock import MagicMock, patch  # unittest.mock std:library
from fastapi import FastAPI  # fastapi ^0.95.0
from fastapi.testclient import TestClient  # fastapi ^0.95.0
from starlette.middleware import Middleware  # starlette ^0.26.0
from starlette.requests import Request  # starlette ^0.26.0
from starlette.responses import Response  # starlette ^0.26.0
from starlette.types import ASGIApp  # starlette ^0.26.0
import jwt  # pyjwt ^2.4.0

from src.backend.api.middleware.auth_middleware import AuthMiddleware, JWTBearer, get_current_user, require_auth, require_role  # src/backend/api/middleware/auth_middleware.py
from src.backend.api.middleware.cors_middleware import CORSMiddleware, setup_cors_middleware  # src/backend/api/middleware/cors_middleware.py
from src.backend.api.middleware.error_middleware import ErrorMiddleware, get_request_id  # src/backend/api/middleware/error_middleware.py
from src.backend.api.middleware.logging_middleware import LoggingMiddleware  # src/backend/api/middleware/logging_middleware.py
from src.backend.api.models.error_models import PipelineError, ValidationError, ErrorCategory, ErrorSeverity  # src/backend/api/models/error_models.py
from src.backend.api.utils.response_utils import create_error_response, handle_exception  # src/backend/api/utils/response_utils.py
from src.test.fixtures.backend.api_fixtures import create_test_app  # src/test/fixtures/backend/api_fixtures.py
from src.test.utils.test_helpers import MockResponseBuilder  # src/test/utils/test_helpers.py


def create_mock_app():
    """Creates a minimal FastAPI application for middleware testing"""
    app = FastAPI()
    @app.get("/test")
    async def test_endpoint():
        return {"message": "Success"}
    return app


def create_mock_request(path: str, method: str, headers: dict, query_params: dict):
    """Creates a mock request object for middleware testing"""
    request = MagicMock()
    request.url.path = path
    request.method = method
    request.headers = headers
    request.query_params = query_params
    request.state = MagicMock()
    return request


def create_mock_response(status_code: int, headers: dict, body: any):
    """Creates a mock response object for middleware testing"""
    response = MagicMock()
    response.status_code = status_code
    response.headers = headers
    response.body = body
    return response


def create_test_token(payload: dict, secret_key: str, algorithm: str):
    """Creates a JWT token for authentication middleware testing"""
    token = jwt.encode(payload, secret_key, algorithm=algorithm)
    return token


class TestAuthMiddleware:
    """Test cases for the authentication middleware"""

    def __init__(self):
        """Initialize the test class"""
        pass

    @pytest.mark.asyncio
    async def test_auth_middleware_excluded_path(self):
        """Test that excluded paths bypass authentication"""
        middleware = AuthMiddleware(exclude_paths=["/excluded"], auto_error=True)
        request = create_mock_request(path="/excluded", method="GET", headers={}, query_params={})
        call_next = MagicMock()

        await middleware.async_dispatch(request, call_next)

        call_next.assert_called_once_with(request)
        assert not hasattr(request.state, "user")

    @pytest.mark.asyncio
    async def test_auth_middleware_missing_token(self):
        """Test handling of requests with missing authentication token"""
        middleware = AuthMiddleware(exclude_paths=[], auto_error=True)
        request = create_mock_request(path="/test", method="GET", headers={}, query_params={})
        call_next = MagicMock()

        with pytest.raises(HTTPException) as exc_info:
            await middleware.async_dispatch(request, call_next)

        assert exc_info.value.status_code == 401
        assert "Missing authentication token" in str(exc_info.value.detail)

        middleware = AuthMiddleware(exclude_paths=[], auto_error=False)
        request = create_mock_request(path="/test", method="GET", headers={}, query_params={})
        call_next = MagicMock()

        await middleware.async_dispatch(request, call_next)

        call_next.assert_called_once_with(request)
        assert not hasattr(request.state, "user")

    @pytest.mark.asyncio
    async def test_auth_middleware_invalid_token(self):
        """Test handling of requests with invalid authentication token"""
        middleware = AuthMiddleware(exclude_paths=[], auto_error=True)
        request = create_mock_request(path="/test", method="GET", headers={"Authorization": "Bearer invalid_token"}, query_params={})
        call_next = MagicMock()

        with patch("src.backend.api.middleware.auth_middleware.verify_token", side_effect=Exception("Invalid token")):
            with pytest.raises(HTTPException) as exc_info:
                await middleware.async_dispatch(request, call_next)

        assert exc_info.value.status_code == 401
        assert "Invalid authentication token" in str(exc_info.value.detail)

    @pytest.mark.asyncio
    async def test_auth_middleware_valid_token(self):
        """Test successful authentication with valid token"""
        middleware = AuthMiddleware(exclude_paths=[], auto_error=True)
        request = create_mock_request(path="/test", method="GET", headers={"Authorization": "Bearer valid_token"}, query_params={})
        call_next = MagicMock()

        user_payload = {"user_id": "123", "username": "testuser", "email": "test@example.com", "roles": ["admin"]}
        with patch("src.backend.api.middleware.auth_middleware.verify_token", return_value=user_payload):
            await middleware.async_dispatch(request, call_next)

        call_next.assert_called_once_with(request)
        assert request.state.user == user_payload

    def test_jwt_bearer_authentication(self):
        """Test the JWTBearer security dependency"""
        jwt_bearer = JWTBearer()
        request = MagicMock()
        request.headers = {"Authorization": "Bearer test_token"}

        with patch("src.backend.api.middleware.auth_middleware.get_current_user", return_value={"user_id": "123"}):
            user = jwt_bearer(request)
            assert user == {"user_id": "123"}

    def test_require_role_authorization(self):
        """Test the require_role authorization dependency"""
        request = MagicMock()
        request.headers = {"Authorization": "Bearer test_token"}

        with patch("src.backend.api.middleware.auth_middleware.get_current_user", return_value={"user_id": "123", "roles": ["admin"]}):
            require_role(["admin"])(request)

        with patch("src.backend.api.middleware.auth_middleware.get_current_user", return_value={"user_id": "123", "roles": ["user"]}):
            with pytest.raises(HTTPException) as exc_info:
                require_role(["admin"])(request)
            assert exc_info.value.status_code == 403


class TestCORSMiddleware:
    """Test cases for the CORS middleware"""

    @pytest.mark.asyncio
    async def test_cors_middleware_preflight(self):
        """Test handling of CORS preflight requests"""
        middleware = CORSMiddleware(allow_origins=["http://example.com"])
        request = create_mock_request(
            path="/test",
            method="OPTIONS",
            headers={"Origin": "http://example.com", "Access-Control-Request-Method": "GET"},
            query_params={}
        )
        call_next = MagicMock()

        response = await middleware.async_dispatch(request, call_next)

        assert response.status_code == 200
        assert response.headers["access-control-allow-origin"] == "http://example.com"
        assert "access-control-allow-methods" in response.headers
        call_next.assert_not_called()

    @pytest.mark.asyncio
    async def test_cors_middleware_allowed_origin(self):
        """Test CORS headers for requests from allowed origins"""
        middleware = CORSMiddleware(allow_origins=["http://example.com"])
        request = create_mock_request(
            path="/test",
            method="GET",
            headers={"Origin": "http://example.com"},
            query_params={}
        )
        call_next = MagicMock(return_value=create_mock_response(status_code=200, headers={}, body=""))

        response = await middleware.async_dispatch(request, call_next)

        call_next.assert_called_once_with(request)
        assert response.headers["access-control-allow-origin"] == "http://example.com"

    @pytest.mark.asyncio
    async def test_cors_middleware_disallowed_origin(self):
        """Test CORS headers for requests from disallowed origins"""
        middleware = CORSMiddleware(allow_origins=["http://example.com"])
        request = create_mock_request(
            path="/test",
            method="GET",
            headers={"Origin": "http://disallowed.com"},
            query_params={}
        )
        call_next = MagicMock(return_value=create_mock_response(status_code=200, headers={}, body=""))

        response = await middleware.async_dispatch(request, call_next)

        call_next.assert_called_once_with(request)
        assert "access-control-allow-origin" not in response.headers

    @pytest.mark.asyncio
    async def test_cors_middleware_wildcard_origin(self):
        """Test CORS headers when wildcard origin is allowed"""
        middleware = CORSMiddleware(allow_origins=["*"])
        request = create_mock_request(
            path="/test",
            method="GET",
            headers={"Origin": "http://any.com"},
            query_params={}
        )
        call_next = MagicMock(return_value=create_mock_response(status_code=200, headers={}, body=""))

        response = await middleware.async_dispatch(request, call_next)

        assert response.headers["access-control-allow-origin"] == "*"

    def test_setup_cors_middleware(self):
        """Test the setup_cors_middleware helper function"""
        app = MagicMock()
        with patch("src.backend.api.middleware.cors_middleware.get_config") as mock_get_config:
            mock_get_config.return_value = MagicMock()
            mock_get_config.return_value.get.side_effect = lambda key, default: {
                "api.cors.enabled": True,
                "api.cors.allow_origins": ["http://example.com"],
                "api.cors.allow_methods": ["GET"],
                "api.cors.allow_headers": ["Content-Type"],
                "api.cors.allow_credentials": True,
                "api.cors.max_age": 600
            }[key]

            setup_cors_middleware(app)

            app.add_middleware.assert_called_once()
            middleware_args = app.add_middleware.call_args[1]
            assert middleware_args["middleware_class"] == CORSMiddleware
            assert middleware_args["allow_origins"] == ["http://example.com"]
            assert middleware_args["allow_methods"] == ["GET"]
            assert middleware_args["allow_headers"] == ["Content-Type"]
            assert middleware_args["allow_credentials"] == True
            assert middleware_args["max_age"] == 600


class TestErrorMiddleware:
    """Test cases for the error handling middleware"""

    def __init__(self):
        """Initialize the test class"""
        pass

    @pytest.mark.asyncio
    async def test_error_middleware_no_exception(self):
        """Test normal request processing without exceptions"""
        middleware = ErrorMiddleware(app=None, debug_mode=False, enable_self_healing=False)
        request = create_mock_request(path="/test", method="GET", headers={}, query_params={})
        call_next = MagicMock(return_value=create_mock_response(status_code=200, headers={}, body=""))

        response = await middleware.dispatch(request, call_next)

        call_next.assert_called_once_with(request)
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_error_middleware_http_exception(self):
        """Test handling of HTTPException"""
        middleware = ErrorMiddleware(app=None, debug_mode=False, enable_self_healing=False)
        request = create_mock_request(path="/test", method="GET", headers={}, query_params={})
        call_next = MagicMock(side_effect=HTTPException(status_code=404, detail="Not Found"))

        response = await middleware.dispatch(request, call_next)

        assert response.status_code == 404
        assert "Not Found" in str(response.body)

    @pytest.mark.asyncio
    async def test_error_middleware_validation_error(self):
        """Test handling of ValidationError"""
        middleware = ErrorMiddleware(app=None, debug_mode=False, enable_self_healing=False)
        request = create_mock_request(path="/test", method="GET", headers={}, query_params={})
        call_next = MagicMock(side_effect=ValidationError(message="Validation Error", validation_errors=[]))

        response = await middleware.dispatch(request, call_next)

        assert response.status_code == 422
        assert "Validation Error" in str(response.body)

    @pytest.mark.asyncio
    async def test_error_middleware_pipeline_error(self):
        """Test handling of PipelineError"""
        middleware = ErrorMiddleware(app=None, debug_mode=False, enable_self_healing=False)
        request = create_mock_request(path="/test", method="GET", headers={}, query_params={})
        call_next = MagicMock(side_effect=PipelineError(message="Pipeline Error", category=ErrorCategory.SYSTEM, severity=ErrorSeverity.CRITICAL))

        response = await middleware.dispatch(request, call_next)

        assert response.status_code == 500
        assert "Pipeline Error" in str(response.body)

    @pytest.mark.asyncio
    async def test_error_middleware_generic_exception(self):
        """Test handling of generic exceptions"""
        middleware = ErrorMiddleware(app=None, debug_mode=False, enable_self_healing=False)
        request = create_mock_request(path="/test", method="GET", headers={}, query_params={})
        call_next = MagicMock(side_effect=Exception("Generic Error"))

        response = await middleware.dispatch(request, call_next)

        assert response.status_code == 500
        assert "Generic Error" in str(response.body)

    @pytest.mark.asyncio
    async def test_error_middleware_self_healing(self):
        """Test self-healing attempt for errors"""
        middleware = ErrorMiddleware(app=None, debug_mode=False, enable_self_healing=True)
        request = create_mock_request(path="/test", method="GET", headers={}, query_params={})
        call_next = MagicMock(side_effect=Exception("Generic Error"))

        with patch.object(middleware, "attempt_self_healing", return_value={"success": True, "actions_taken": ["action1"]}):
            response = await middleware.dispatch(request, call_next)

        assert response.status_code == 500
        assert "Generic Error" in str(response.body)
        assert "actions_taken" in str(response.body)

    def test_get_request_id(self):
        """Test the get_request_id utility function"""
        request = MagicMock()
        request.headers = {"X-Request-ID": "test_id"}
        assert get_request_id(request) == "test_id"

        request.headers = {}
        request_id = get_request_id(request)
        assert isinstance(request_id, str)
        assert len(request_id) == 36


class TestLoggingMiddleware:
    """Test cases for the logging middleware"""

    def __init__(self):
        """Initialize the test class"""
        pass

    @pytest.mark.asyncio
    async def test_logging_middleware_request_logging(self):
        """Test that requests are properly logged"""
        middleware = LoggingMiddleware()
        request = create_mock_request(path="/test", method="GET", headers={"Content-Type": "application/json"}, query_params={"param1": "value1"})
        call_next = MagicMock(return_value=create_mock_response(status_code=200, headers={}, body=""))

        with patch("src.backend.api.middleware.logging_middleware.logger") as mock_logger:
            await middleware.async_dispatch(request, call_next)

            mock_logger.info.assert_called()
            log_message = mock_logger.info.call_args[0][0]
            assert "Request received" in log_message
            assert "GET" in log_message
            assert "/test" in log_message
            assert "client_ip" in str(mock_logger.info.call_args[1])

    @pytest.mark.asyncio
    async def test_logging_middleware_response_logging(self):
        """Test that responses are properly logged"""
        middleware = LoggingMiddleware()
        request = create_mock_request(path="/test", method="GET", headers={}, query_params={})
        call_next = MagicMock(return_value=create_mock_response(status_code=200, headers={"Content-Type": "application/json"}, body='{"message": "success"}'))

        with patch("src.backend.api.middleware.logging_middleware.logger") as mock_logger:
            await middleware.async_dispatch(request, call_next)

            mock_logger.info.assert_called()
            log_message = mock_logger.info.call_args[0][0]
            assert "Response sent" in log_message
            assert "200" in log_message
            assert "GET" in log_message
            assert "/test" in log_message
            assert "process_time" in str(mock_logger.info.call_args[1])

    @pytest.mark.asyncio
    async def test_logging_middleware_excluded_path(self):
        """Test that excluded paths are not logged"""
        middleware = LoggingMiddleware(exclude_paths=["/excluded"])
        request = create_mock_request(path="/excluded", method="GET", headers={}, query_params={})
        call_next = MagicMock(return_value=create_mock_response(status_code=200, headers={}, body=""))

        with patch("src.backend.api.middleware.logging_middleware.logger") as mock_logger:
            await middleware.async_dispatch(request, call_next)

            mock_logger.info.assert_not_called()
            call_next.assert_called_once_with(request)

    @pytest.mark.asyncio
    async def test_logging_middleware_request_body(self):
        """Test logging of request body when enabled"""
        middleware = LoggingMiddleware(log_request_body=True)
        request = create_mock_request(path="/test", method="POST", headers={"Content-Type": "application/json"}, query_params={},)
        request._body = b'{"key": "value"}'
        call_next = MagicMock(return_value=create_mock_response(status_code=200, headers={}, body=""))

        with patch("src.backend.api.middleware.logging_middleware.logger") as mock_logger:
            await middleware.async_dispatch(request, call_next)

            mock_logger.debug.assert_called()
            log_message = mock_logger.debug.call_args[0][0]
            assert "Request body" in log_message
            assert '{"key": "value"}' in log_message

    @pytest.mark.asyncio
    async def test_logging_middleware_response_body(self):
        """Test logging of response body when enabled"""
        middleware = LoggingMiddleware(log_response_body=True)
        request = create_mock_request(path="/test", method="GET", headers={}, query_params={})
        call_next = MagicMock(return_value=create_mock_response(status_code=200, headers={"Content-Type": "application/json"}, body='{"message": "success"}'))

        with patch("src.backend.api.middleware.logging_middleware.logger") as mock_logger:
            await middleware.async_dispatch(request, call_next)

            mock_logger.debug.assert_called()
            log_message = mock_logger.debug.call_args[0][0]
            assert "Response body" in log_message
            assert '{"message": "success"}' in log_message

    @pytest.mark.asyncio
    async def test_logging_middleware_exception_handling(self):
        """Test logging when an exception occurs during processing"""
        middleware = LoggingMiddleware()
        request = create_mock_request(path="/test", method="GET", headers={}, query_params={})
        call_next = MagicMock(side_effect=Exception("Test Exception"))

        with patch("src.backend.api.middleware.logging_middleware.logger") as mock_logger:
            with pytest.raises(Exception) as exc_info:
                await middleware.async_dispatch(request, call_next)

            assert "Test Exception" in str(exc_info.value)
            mock_logger.error.assert_called()
            log_message = mock_logger.error.call_args[0][0]
            assert "Test Exception" in log_message
            assert "traceback" in str(mock_logger.error.call_args[1])


class TestMiddlewareIntegration:
    """Integration tests for middleware components working together"""

    def __init__(self):
        """Initialize the test class"""
        pass

    @pytest.mark.asyncio
    async def test_middleware_chain_execution_order(self):
        """Test that middleware components execute in the correct order"""
        app = FastAPI()
        execution_order = []

        class OrderMiddleware(BaseHTTPMiddleware):
            def __init__(self, app, name):
                super().__init__(app)
                self.name = name

            async def dispatch(self, request: Request, call_next):
                execution_order.append(self.name)
                response = await call_next(request)
                return response

        app.add_middleware(OrderMiddleware, name="logging")
        app.add_middleware(OrderMiddleware, name="auth")
        app.add_middleware(OrderMiddleware, name="cors")
        app.add_middleware(OrderMiddleware, name="error")

        @app.get("/test")
        async def test_endpoint():
            return {"message": "Success"}

        client = TestClient(app)
        client.get("/test")

        assert execution_order == ["error", "cors", "auth", "logging"]

    @pytest.mark.asyncio
    async def test_middleware_integration_success_path(self):
        """Test successful request through all middleware components"""
        app = FastAPI()
        app.add_middleware(ErrorMiddleware, debug_mode=False, enable_self_healing=False)
        app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["Content-Type"])
        app.add_middleware(AuthMiddleware, exclude_paths=[])
        app.add_middleware(LoggingMiddleware)

        @app.get("/test")
        async def test_endpoint():
            return {"message": "Success"}

        client = TestClient(app)
        headers = {"Authorization": "Bearer test_token", "Origin": "http://example.com"}
        with patch("src.backend.api.middleware.auth_middleware.verify_token", return_value={"user_id": "123"}):
            response = client.get("/test", headers=headers)

        assert response.status_code == 200
        assert response.json() == {"message": "Success"}
        assert "access-control-allow-origin" in response.headers
        assert "X-Request-ID" in response.headers

    @pytest.mark.asyncio
    async def test_middleware_integration_error_path(self):
        """Test error handling through all middleware components"""
        app = FastAPI()
        app.add_middleware(ErrorMiddleware, debug_mode=False, enable_self_healing=False)
        app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["Content-Type"])
        app.add_middleware(AuthMiddleware, exclude_paths=[])
        app.add_middleware(LoggingMiddleware)

        @app.get("/test")
        async def test_endpoint():
            raise ValueError("Test Error")

        client = TestClient(app)
        headers = {"Authorization": "Bearer test_token", "Origin": "http://example.com"}
        with patch("src.backend.api.middleware.auth_middleware.verify_token", return_value={"user_id": "123"}):
            response = client.get("/test", headers=headers)

        assert response.status_code == 500
        assert "Test Error" in str(response.json())
        assert "access-control-allow-origin" in response.headers
        assert "X-Request-ID" in response.headers