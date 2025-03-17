# Authentication

## Introduction

The Self-Healing Data Pipeline API uses OAuth 2.0 with JWT (JSON Web Tokens) for authentication. This document provides detailed information on how to authenticate with the API, manage tokens, and understand the authorization model.

All API endpoints, except for the authentication endpoints themselves, require a valid authentication token to be included in the request.

For a complete overview of all available API endpoints, please refer to the API Overview documentation.

## Authentication Flow

The API uses a standard OAuth 2.0 password flow for authentication:

1. Client submits credentials to the authentication endpoint
2. Server validates credentials and returns an access token
3. Client includes the access token in subsequent API requests
4. Server validates the token and processes the request if valid
5. When the token expires, client can use the refresh token to obtain a new access token

This flow provides secure authentication while minimizing the need to repeatedly send credentials.

## Authentication Endpoints

### Login

```
POST /auth/login
```

Authenticates a user and returns an access token and refresh token.

**Request Body:**

```json
{
  "username": "user@example.com",
  "password": "your-secure-password"
}
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "bearer",
    "expires_in": 1800,
    "user": {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "username": "user@example.com",
      "name": "John Doe",
      "roles": ["data_engineer", "quality_manager"]
    }
  }
}
```

### Token Refresh

```
POST /auth/refresh
```

Refreshes an expired access token using a valid refresh token.

**Request Body:**

```json
{
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "bearer",
    "expires_in": 1800
  }
}
```

### Logout

```
POST /auth/logout
```

Invalidates the current token.

**Request Headers:**

```
Authorization: Bearer YOUR_ACCESS_TOKEN
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "message": "Successfully logged out"
  }
}
```

## Using Authentication Tokens

After obtaining an access token, include it in the `Authorization` header of all API requests:

```
Authorization: Bearer YOUR_ACCESS_TOKEN
```

Example request with authentication:

```bash
curl -X GET \
  "https://api.example.com/api/v1/monitoring/metrics" \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." \
  -H "Content-Type: application/json"
```

### Token Expiration

Access tokens are valid for 30 minutes by default. After expiration, you'll need to use the refresh token to obtain a new access token. If you receive a `401 Unauthorized` response, it typically means your token has expired or is invalid.

### Token Security

Treat access tokens and refresh tokens as sensitive credentials:

- Store tokens securely (e.g., in secure HTTP-only cookies or secure storage)
- Never expose tokens in URLs or client-side code
- Implement proper token refresh logic to maintain sessions
- Use HTTPS for all API communications to prevent token interception

## Authorization Model

The API uses a role-based access control (RBAC) system to determine what actions users can perform. Each user is assigned one or more roles, and each role has a set of permissions.

### Roles

The system includes the following predefined roles:

- **admin**: Full system access
- **data_engineer**: Manage data pipelines and sources
- **quality_manager**: Manage data quality rules and validations
- **monitoring_user**: View monitoring data and alerts
- **read_only**: Read-only access to all data

### Permissions

Permissions are structured as `resource:action` pairs. For example:

- `pipeline:create` - Create new pipelines
- `quality:validate` - Execute data quality validations
- `monitoring:view` - View monitoring data
- `user:manage` - Manage user accounts

### Permission Checking

When you make an API request, the system checks if your user has the required permissions for the requested operation. If not, you'll receive a `403 Forbidden` response.

You can retrieve your user's roles and permissions with the following endpoint:

```
GET /auth/me
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "user": {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "username": "user@example.com",
      "name": "John Doe",
      "roles": ["data_engineer", "quality_manager"],
      "permissions": [
        "pipeline:create",
        "pipeline:update",
        "pipeline:execute",
        "quality:validate",
        "quality:rules:manage"
      ]
    }
  }
}
```

## Error Responses

Authentication and authorization errors return standard HTTP status codes with descriptive error messages:

### 401 Unauthorized

Returned when authentication fails or the token is invalid/expired.

```json
{
  "status": "error",
  "error": "AUTH_ERROR",
  "message": "Invalid or expired token"
}
```

### 403 Forbidden

Returned when the authenticated user doesn't have permission for the requested operation.

```json
{
  "status": "error",
  "error": "PERMISSION_DENIED",
  "message": "User does not have permission to perform this action"
}
```

### 400 Bad Request

Returned when the authentication request is malformed.

```json
{
  "status": "error",
  "error": "VALIDATION_ERROR",
  "message": "Username and password are required"
}
```

## Token Structure

The access tokens are JSON Web Tokens (JWT) with the following structure:

### Header

```json
{
  "alg": "RS256",
  "typ": "JWT"
}
```

### Payload

```json
{
  "sub": "550e8400-e29b-41d4-a716-446655440000",  // User ID
  "name": "John Doe",                           // User's full name
  "email": "user@example.com",                   // User's email
  "roles": ["data_engineer", "quality_manager"], // User's roles
  "iat": 1516239022,                            // Issued at timestamp
  "exp": 1516240822,                            // Expiration timestamp
  "iss": "self-healing-pipeline"                 // Token issuer
}
```

The token is signed using RS256 (RSA Signature with SHA-256) to ensure its integrity.

## Service-to-Service Authentication

For automated systems or services that need to interact with the API, we recommend using service accounts rather than user credentials.

Service accounts can be created through the administration interface and are assigned specific roles with limited permissions based on the principle of least privilege.

Service account authentication uses the same endpoints and token mechanism as user authentication, but with credentials specific to the service account.

## Multi-Factor Authentication

For enhanced security, the system supports multi-factor authentication (MFA) for user accounts. When MFA is enabled, the login process requires an additional verification step.

### MFA Login Flow

1. Submit username and password to `/auth/login`
2. If MFA is enabled, the response includes an `mfa_required` flag:

```json
{
  "status": "success",
  "data": {
    "mfa_required": true,
    "mfa_token": "temporary-mfa-token"
  }
}
```

3. Submit the MFA verification code to complete authentication:

```
POST /auth/mfa-verify
```

**Request Body:**

```json
{
  "mfa_token": "temporary-mfa-token",
  "verification_code": "123456"
}
```

4. If the verification code is valid, the response includes the access and refresh tokens as in the standard login flow.

## Security Best Practices

To ensure the security of your API integration, follow these best practices:

1. **Use HTTPS**: Always use HTTPS for all API communications to prevent token interception.

2. **Secure Token Storage**: Store access tokens securely. For web applications, use HttpOnly cookies or secure storage mechanisms.

3. **Implement Token Refresh**: Access tokens have a limited lifetime. Implement proper token refresh logic to maintain user sessions.

4. **Use Minimum Required Permissions**: Request only the permissions your application needs.

5. **Validate Input**: Always validate and sanitize input before sending it to the API.

6. **Handle Sensitive Data**: Be careful when logging API requests and responses to avoid exposing sensitive data.

7. **Implement Proper Error Handling**: Handle authentication and authorization errors gracefully in your application.

8. **Regular Token Validation**: Periodically validate tokens to ensure they haven't been revoked.

9. **Logout on Security Events**: Implement proper logout functionality, especially on security-related events like password changes.

## Rate Limiting

Authentication endpoints have rate limiting to prevent brute force attacks:

- Login attempts are limited to 5 per minute per IP address
- Failed login attempts are limited to 10 per hour per username
- Token refresh is limited to 10 per minute per user

When rate limits are exceeded, the API returns a `429 Too Many Requests` status code with headers indicating the rate limit and when it will reset:

```
X-RateLimit-Limit: 5
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1623766800
```

## Client Libraries

Our client libraries handle authentication automatically, simplifying the integration process:

### Python

```python
from selfhealing_pipeline_client import Client

# Authenticate with username and password
client = Client(username="user@example.com", password="your-secure-password")

# Or authenticate with tokens
client = Client(access_token="your-access-token", refresh_token="your-refresh-token")

# The client handles token refresh automatically
metrics = client.monitoring.get_metrics()
```

### JavaScript

```javascript
import { Client } from 'selfhealing-pipeline-client';

// Authenticate with username and password
const client = new Client({
  username: 'user@example.com',
  password: 'your-secure-password'
});

// Or authenticate with tokens
const client = new Client({
  accessToken: 'your-access-token',
  refreshToken: 'your-refresh-token'
});

// The client handles token refresh automatically
const metrics = await client.monitoring.getMetrics();
```

## Code Examples

### Authentication with Python

```python
import requests

def authenticate(username, password):
    """Authenticate with the API and return access token
    
    Args:
        username: User email or username
        password: User password
        
    Returns:
        dict: Authentication response with tokens
    """
    auth_url = "https://api.example.com/api/v1/auth/login"
    
    response = requests.post(
        auth_url,
        json={
            "username": username,
            "password": password
        }
    )
    
    response.raise_for_status()  # Raise exception for error status codes
    return response.json()['data']

def refresh_token(refresh_token):
    """Refresh an expired access token
    
    Args:
        refresh_token: The refresh token from previous authentication
        
    Returns:
        dict: Refresh response with new access token
    """
    refresh_url = "https://api.example.com/api/v1/auth/refresh"
    
    response = requests.post(
        refresh_url,
        json={
            "refresh_token": refresh_token
        }
    )
    
    response.raise_for_status()
    return response.json()['data']

def get_api_data(endpoint, token):
    """Make an authenticated API request
    
    Args:
        endpoint: API endpoint path (without base URL)
        token: Authentication token
        
    Returns:
        dict: API response data
    """
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    response = requests.get(
        f'https://api.example.com/api/v1{endpoint}',
        headers=headers
    )
    
    response.raise_for_status()
    return response.json()['data']

# Example usage
try:
    # Authenticate
    auth_data = authenticate("user@example.com", "your-secure-password")
    access_token = auth_data['access_token']
    refresh_token = auth_data['refresh_token']
    
    # Use the access token to make API requests
    user_info = get_api_data("/auth/me", access_token)
    print(f"Authenticated as: {user_info['user']['name']}")
    print(f"Roles: {', '.join(user_info['user']['roles'])}")
    
    # When the token expires, refresh it
    try:
        pipeline_data = get_api_data("/ingestion/pipelines", access_token)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:  # Unauthorized - token expired
            print("Token expired, refreshing...")
            refresh_data = refresh_token(refresh_token)
            access_token = refresh_data['access_token']
            
            # Retry with new token
            pipeline_data = get_api_data("/ingestion/pipelines", access_token)
    
except requests.exceptions.HTTPError as e:
    print(f'Authentication Error: {e}')
```

### Authentication with JavaScript

```javascript
/**
 * Authenticate with the API and return access token
 * 
 * @param {string} username - User email or username
 * @param {string} password - User password
 * @returns {Promise<Object>} Authentication response with tokens
 */
async function authenticate(username, password) {
  const authUrl = 'https://api.example.com/api/v1/auth/login';
  
  const response = await fetch(authUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      username,
      password
    })
  });
  
  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(`Authentication error: ${errorData.message}`);
  }
  
  const responseData = await response.json();
  return responseData.data;
}

/**
 * Refresh an expired access token
 * 
 * @param {string} refreshToken - The refresh token from previous authentication
 * @returns {Promise<Object>} Refresh response with new access token
 */
async function refreshToken(refreshToken) {
  const refreshUrl = 'https://api.example.com/api/v1/auth/refresh';
  
  const response = await fetch(refreshUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      refresh_token: refreshToken
    })
  });
  
  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(`Token refresh error: ${errorData.message}`);
  }
  
  const responseData = await response.json();
  return responseData.data;
}

/**
 * Make an authenticated API request
 * 
 * @param {string} endpoint - API endpoint path (without base URL)
 * @param {string} token - Authentication token
 * @returns {Promise<Object>} API response data
 */
async function getApiData(endpoint, token) {
  const response = await fetch(`https://api.example.com/api/v1${endpoint}`, {
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json'
    }
  });
  
  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(`API error: ${errorData.message}`);
  }
  
  const responseData = await response.json();
  return responseData.data;
}

// Example usage
async function main() {
  try {
    // Authenticate
    const authData = await authenticate('user@example.com', 'your-secure-password');
    let accessToken = authData.access_token;
    const refreshToken = authData.refresh_token;
    
    // Use the access token to make API requests
    const userInfo = await getApiData('/auth/me', accessToken);
    console.log(`Authenticated as: ${userInfo.user.name}`);
    console.log(`Roles: ${userInfo.user.roles.join(', ')}`);
    
    // When the token expires, refresh it
    try {
      const pipelineData = await getApiData('/ingestion/pipelines', accessToken);
      console.log(`Found ${pipelineData.length} pipelines`);
    } catch (error) {
      if (error.message.includes('401')) {  // Unauthorized - token expired
        console.log('Token expired, refreshing...');
        const refreshData = await refreshToken(refreshToken);
        accessToken = refreshData.access_token;
        
        // Retry with new token
        const pipelineData = await getApiData('/ingestion/pipelines', accessToken);
        console.log(`Found ${pipelineData.length} pipelines`);
      } else {
        throw error;
      }
    }
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

main();
```

### Authentication with cURL

```bash
# Authenticate and get tokens
curl -X POST \
  "https://api.example.com/api/v1/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "user@example.com",
    "password": "your-secure-password"
  }'

# Response will contain access_token and refresh_token
# {
#   "status": "success",
#   "data": {
#     "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
#     "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
#     "token_type": "bearer",
#     "expires_in": 1800,
#     "user": { ... }
#   }
# }

# Store the access token in a variable
ACCESS_TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

# Make an authenticated API request
curl -X GET \
  "https://api.example.com/api/v1/auth/me" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json"

# Refresh an expired token
REFRESH_TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."

curl -X POST \
  "https://api.example.com/api/v1/auth/refresh" \
  -H "Content-Type: application/json" \
  -d "{
    \"refresh_token\": \"$REFRESH_TOKEN\"
  }"

# Logout (invalidate token)
curl -X POST \
  "https://api.example.com/api/v1/auth/logout" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H "Content-Type: application/json"
```

## Best Practices

### Token Storage
Store tokens securely using appropriate mechanisms for your platform. For web applications, use HttpOnly cookies or secure browser storage. For mobile applications, use secure storage APIs. Never store tokens in local storage for web applications.

### Token Refresh
Implement proper token refresh logic to maintain user sessions without requiring frequent re-authentication. Handle 401 responses by attempting to refresh the token before prompting for re-authentication.

### Error Handling
Implement comprehensive error handling for authentication failures. Distinguish between different types of authentication errors (invalid credentials, expired tokens, insufficient permissions) and handle each appropriately.

### Logout Implementation
Always implement proper logout functionality that invalidates tokens on the server side. This prevents token reuse after logout and improves security.

### Secure Communication
Always use HTTPS for all API communications to prevent token interception. Enforce TLS 1.2+ with strong cipher suites for all API traffic.

## Security Considerations

### Token Expiration
Access tokens have a limited lifetime (30 minutes by default) to minimize the impact of token leakage. Refresh tokens have a longer lifetime but can be revoked if compromised.

### Token Revocation
Tokens can be revoked by administrators in case of security incidents. Implement regular token validation to ensure tokens haven't been revoked.

### Rate Limiting
Authentication endpoints have rate limiting to prevent brute force attacks. Implement exponential backoff in your client code when encountering rate limit errors.

### Principle of Least Privilege
Request only the permissions your application needs. This minimizes the potential impact if tokens are compromised.

### Multi-Factor Authentication
Enable MFA for sensitive operations or user accounts with elevated privileges to provide an additional layer of security.