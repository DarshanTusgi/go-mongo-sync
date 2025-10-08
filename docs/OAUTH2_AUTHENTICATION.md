# OAuth2 Authentication Guide

This guide explains how to set up and use OAuth2 authentication instead of the legacy license-based authentication system.

## Overview

The OAuth2 authentication system provides enterprise-grade security with:

- **Client Credentials Flow**: Secure machine-to-machine authentication
- **JWT Tokens**: Industry-standard tokens with configurable expiration
- **Automatic Token Refresh**: VM-sync automatically refreshes tokens before expiration
- **Backward Compatibility**: License-based authentication still works as fallback
- **Admin Management**: REST API for managing client credentials

## Authentication Flow

```
1. Admin creates client credentials via REST API
   └─► POST /api/auth/admin/clients

2. VM-sync stores credentials in local database
   └─► Local MongoDB: vm_oauth2_auth.stored_credentials

3. VM-sync authenticates to get JWT token
   └─► POST /api/auth/token

4. VM-sync uses token for WebSocket connection
   └─► WebSocket message: {"type": "oauth2_auth", "token": "..."}

5. VM-sync automatically refreshes token before expiration
   └─► Background process handles token renewal
```

## Setup Instructions

### 1. Configure Cloud-Sync (Server)

The cloud-sync service automatically initializes OAuth2 authentication. No additional configuration needed.

**Default settings:**
- Database: `oauth2_auth`
- Collection: `clients`
- JWT Secret: Configurable (TODO: move to config)
- Token Expiry: 24 hours

### 2. Create Client Credentials (Admin)

Use the admin API to create OAuth2 client credentials:

```bash
# Create new client credentials
curl -X POST http://localhost:8080/api/auth/admin/clients \
  -H "Content-Type: application/json" \
  -H "X-Admin-API-Key: admin-api-key-placeholder" \
  -d '{
    "app_id": "vm-sync-production",
    "name": "Production VM-Sync Instance",
    "description": "Production data synchronization client",
    "scopes": ["vm-sync", "data:read", "data:write", "stream:read", "stream:write"],
    "vm_metadata": {
      "hostname": "prod-vm-01",
      "environment": "production",
      "location": "us-east-1"
    }
  }'
```

**Response:**
```json
{
  "client_id": "oauth2_client_1234567890abcdef",
  "client_secret": "secret_abcdef1234567890fedcba0987654321",
  "app_id": "vm-sync-production",
  "name": "Production VM-Sync Instance",
  "scopes": ["vm-sync", "data:read", "data:write", "stream:read", "stream:write"],
  "created_at": "2024-01-01T12:00:00Z"
}
```

⚠️ **Important**: Save the `client_secret` - it's only shown once!

### 3. Configure VM-Sync (Client)

#### Option A: Configuration File

Update your `vm-config.yaml`:

```yaml
cloud_sync:
  http_url: "http://localhost:8080"
  ws_url: "ws://localhost:8080/ws"
  
  # Enable OAuth2 authentication
  oauth2:
    enabled: true
    client_id: "oauth2_client_1234567890abcdef"
    client_secret: "secret_abcdef1234567890fedcba0987654321"
    token_url: "http://localhost:8080/api/auth/token"
```

#### Option B: Admin Registration

1. Enable OAuth2 without credentials:
```yaml
oauth2:
  enabled: true
  # Leave client_id and client_secret empty
```

2. Register credentials via admin API:
```bash
# Store credentials in VM local database
curl -X POST http://vm-sync-host:8081/api/auth/store-credentials \
  -H "Content-Type: application/json" \
  -d '{
    "app_id": "vm-sync-production",
    "client_id": "oauth2_client_1234567890abcdef",
    "client_secret": "secret_abcdef1234567890fedcba0987654321"
  }'
```

### 4. Start Services

Start cloud-sync and vm-sync. You should see OAuth2 authentication logs:

**Cloud-sync logs:**
```
OAuth2 authentication service initialized successfully
OAuth2 authentication routes registered
vm-sync client OAuth2 authentication successful: client_id=oauth2_client_1234567890abcdef, app_id=vm-sync-production
```

**VM-sync logs:**
```
OAuth2 token manager initialized successfully
OAuth2 authentication sent to cloud-sync
OAuth2 authentication successful
```

## API Reference

### Admin Endpoints

All admin endpoints require the `X-Admin-API-Key` header.

#### Create Client Credentials
```
POST /api/auth/admin/clients
Content-Type: application/json
X-Admin-API-Key: admin-api-key-placeholder

{
  "app_id": "unique-app-identifier",
  "name": "Human-readable name",
  "description": "Optional description",
  "scopes": ["vm-sync", "data:read", "data:write"],
  "expires_in": 86400,  // Optional: seconds until expiration
  "vm_metadata": {      // Optional: VM-specific metadata
    "hostname": "vm-host",
    "environment": "production"
  }
}
```

#### List All Clients
```
GET /api/auth/admin/clients
X-Admin-API-Key: admin-api-key-placeholder
```

#### Revoke Client
```
DELETE /api/auth/admin/clients/{client_id}
X-Admin-API-Key: admin-api-key-placeholder
```

### Public Endpoints

#### Get Token (OAuth2 Client Credentials Flow)
```
POST /api/auth/token
Content-Type: application/json

{
  "grant_type": "client_credentials",
  "client_id": "oauth2_client_1234567890abcdef",
  "client_secret": "secret_abcdef1234567890fedcba0987654321",
  "scope": "vm-sync data:read data:write"  // Optional
}
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "Bearer",
  "expires_in": 86400,
  "scope": "vm-sync data:read data:write stream:read stream:write"
}
```

#### Validate Token
```
POST /api/auth/validate
Content-Type: application/json

{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

## Token Management

### Automatic Refresh

VM-sync automatically refreshes tokens:
- **Refresh Trigger**: 5 minutes before token expiration
- **Background Process**: Non-blocking token renewal
- **Fallback**: Uses cached token if refresh fails temporarily

### Token Storage

**Cloud-sync (Server):**
- Database: `oauth2_auth.clients`
- Stores: Client credentials, metadata, status

**VM-sync (Client):**
- Database: `vm_oauth2_auth.stored_credentials`
- Database: `vm_oauth2_auth.vm_token_cache`
- Stores: Client credentials and cached tokens

### Security Features

- **Hashed Secrets**: Client secrets are bcrypt-hashed in database
- **Token Expiration**: Configurable token lifetime (default: 24 hours)
- **Scope-based Authorization**: Fine-grained permission control
- **Admin API Keys**: Protected admin endpoints

## Backward Compatibility

The system maintains full backward compatibility:

1. **License Authentication Still Works**: Existing VM-sync instances continue working
2. **Fallback Mechanism**: If OAuth2 fails, VM-sync falls back to license auth
3. **Mixed Environments**: Some VMs can use OAuth2 while others use licenses

## Troubleshooting

### Common Issues

#### 1. OAuth2 Authentication Failed
```
Error: OAuth2 token validation failed: invalid token
```
**Solutions:**
- Check client_id and client_secret are correct
- Verify token hasn't expired
- Ensure cloud-sync OAuth2 service is running

#### 2. No Authentication Method Available
```
Error: no authentication method available: OAuth2 failed and no license found
```
**Solutions:**
- Configure OAuth2 credentials OR set VM_SYNC_LICENSE environment variable
- Check network connectivity between VM-sync and cloud-sync

#### 3. Token Refresh Failed
```
Warning: failed to refresh token: connection refused
```
**Solutions:**
- Check cloud-sync is accessible
- Verify client credentials haven't been revoked
- Check admin hasn't deleted the client

### Debug Logs

Enable debug logging to troubleshoot:

**VM-sync:**
```bash
LOG_LEVEL=debug ./bin/vm-sync -config oauth2-vm-config.yaml
```

**Cloud-sync:**
```bash
LOG_LEVEL=debug ./bin/cloud-sync -config config.yaml
```

## Migration from License Authentication

### Step 1: Run Both Systems in Parallel
1. Keep existing license authentication
2. Add OAuth2 configuration to cloud-sync and vm-sync
3. Test OAuth2 authentication with one VM instance

### Step 2: Gradual Migration
1. Create OAuth2 credentials for each VM instance
2. Update VM configurations one by one
3. Monitor logs to ensure successful authentication

### Step 3: Disable License Authentication (Optional)
1. Remove license validation from cloud-sync
2. Remove VM_SYNC_LICENSE environment variables
3. Update documentation and procedures

## Best Practices

### Security
- **Rotate Secrets**: Regularly create new client credentials and revoke old ones
- **Scope Limitation**: Use minimal required scopes for each client
- **Environment Separation**: Use different client credentials for dev/staging/prod
- **Secret Storage**: Store client secrets securely (HashiCorp Vault, AWS Secrets Manager, etc.)

### Monitoring
- **Token Expiration**: Monitor token refresh logs
- **Failed Authentication**: Alert on authentication failures
- **Client Management**: Track active clients and their last activity

### Operations
- **Backup Credentials**: Backup OAuth2 client database
- **Client Inventory**: Maintain inventory of all client credentials
- **Access Review**: Regularly review and clean up unused clients

## Production Deployment

### Environment Variables
```bash
# Cloud-sync
export OAUTH2_JWT_SECRET="your-production-jwt-secret-256-bits"
export OAUTH2_ADMIN_API_KEY="your-admin-api-key"

# VM-sync (if not using config file)
export OAUTH2_CLIENT_ID="oauth2_client_prod_123"
export OAUTH2_CLIENT_SECRET="secret_prod_456"
```

### Docker Compose
```yaml
version: '3.8'
services:
  cloud-sync:
    image: cloud-sync:latest
    environment:
      - OAUTH2_JWT_SECRET=${OAUTH2_JWT_SECRET}
      - OAUTH2_ADMIN_API_KEY=${OAUTH2_ADMIN_API_KEY}
    
  vm-sync:
    image: vm-sync:latest
    environment:
      - OAUTH2_CLIENT_ID=${OAUTH2_CLIENT_ID}
      - OAUTH2_CLIENT_SECRET=${OAUTH2_CLIENT_SECRET}
```

### Load Balancer Configuration
```nginx
# nginx configuration for OAuth2 endpoints
location /api/auth/ {
    proxy_pass http://cloud-sync-backend;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    
    # Increase timeout for token operations
    proxy_read_timeout 60s;
    proxy_connect_timeout 60s;
}
```

This completes the OAuth2 authentication system implementation for enterprise-grade security in your data synchronization platform.