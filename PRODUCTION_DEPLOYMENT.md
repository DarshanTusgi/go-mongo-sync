# Production Deployment Guide

## Overview
This guide provides step-by-step instructions for deploying the Go Data Sync system in a production environment.

## Prerequisites

### System Requirements
- Go 1.19 or later
- MongoDB 4.4 or later
- SSL/TLS certificates for secure connections
- Network connectivity between cloud-sync and vm-sync instances

### Security Requirements
- Unique UUID licenses for each environment
- Strong encryption keys (32-byte, base64 encoded)
- Secure MongoDB credentials
- Firewall rules restricting access
- SSL/TLS enabled for all connections

## Step 1: Environment Setup

### 1.1 Generate Production Licenses
```bash
# Generate unique UUIDs for production
uuidgen  # Use this for both CLOUD_SYNC_LICENSE and VM_SYNC_LICENSE
```

### 1.2 Generate Encryption Keys
```bash
# Generate a secure 32-byte encryption key
openssl rand -base64 32
```

### 1.3 Configure Environment Variables
Copy the production template:
```bash
cp .env.production .env
```

Edit `.env` with your production values:
- Set unique UUIDs for licenses
- Configure MongoDB connection strings
- Set encryption keys and key IDs
- Configure server URLs

## Step 2: Configuration Files

### 2.1 Cloud-Sync Configuration
Use `examples/cloud-config-production.yaml` as your base configuration:
- Update MongoDB URI via environment variable
- Configure field and document filters as needed
- Set appropriate server ports and timeouts
- Enable encryption with your production keys

### 2.2 VM-Sync Configuration
Use `examples/vm-config-production.yaml` as your base configuration:
- Update cloud-sync server URLs
- Configure collections to sync
- Set encryption settings matching cloud-sync
- Configure sync intervals and batch sizes

## Step 3: Build and Deploy

### 3.1 Build Binaries
```bash
# Build cloud-sync
go build -o bin/cloud-sync cmd/cloud-sync/main.go

# Build vm-sync
go build -o bin/vm-sync cmd/vm-sync/main.go
```

### 3.2 Deploy Cloud-Sync Server
```bash
# Start cloud-sync with production config
CLOUD_SYNC_LICENSE="your-production-uuid" ./bin/cloud-sync -config examples/cloud-config-production.yaml
```

### 3.3 Deploy VM-Sync Clients
```bash
# Start vm-sync with production config
VM_SYNC_LICENSE="your-production-uuid" ./bin/vm-sync -config examples/vm-config-production.yaml
```

## Step 4: Security Checklist

### 4.1 License Validation
- [ ] Unique UUIDs generated for production
- [ ] Same UUID used for both cloud-sync and vm-sync
- [ ] License validation tested and working
- [ ] Invalid licenses properly rejected

### 4.2 Encryption
- [ ] Strong encryption keys generated
- [ ] Same encryption key used across all instances
- [ ] Key rotation plan in place
- [ ] Keys stored securely (not in code)

### 4.3 Network Security
- [ ] SSL/TLS enabled for all connections
- [ ] Firewall rules configured
- [ ] MongoDB authentication enabled
- [ ] Network access restricted to authorized IPs

### 4.4 Data Protection
- [ ] Document-level filtering configured
- [ ] Field-level filtering for sensitive data
- [ ] Access controls tested
- [ ] Data encryption at rest enabled

## Step 5: Monitoring and Maintenance

### 5.1 Health Checks
- Monitor WebSocket connections
- Check sync status and document counts
- Monitor error logs for authentication failures
- Verify checkpoint progression

### 5.2 Log Monitoring
Key log messages to monitor:
- License validation failures
- WebSocket connection errors
- MongoDB connection issues
- Sync progress and errors

### 5.3 Performance Monitoring
- Document transfer rates
- Memory and CPU usage
- Network bandwidth utilization
- MongoDB performance metrics

## Step 6: Troubleshooting

### 6.1 Common Issues

**License Validation Failures:**
- Verify UUID format is correct
- Ensure same UUID used for cloud-sync and vm-sync
- Check environment variables are set

**Connection Issues:**
- Verify network connectivity
- Check firewall rules
- Validate SSL/TLS certificates
- Confirm MongoDB accessibility

**Sync Issues:**
- Check document and field filters
- Verify collection permissions
- Monitor checkpoint progression
- Review error logs for specific failures

### 6.2 Error Handling
The system includes robust error handling for:
- Invalid license formats (exits with code 1)
- Missing environment variables (exits with code 1)
- Configuration file errors (exits with code 1)
- Network connectivity issues (automatic retry)
- MongoDB connection failures (automatic retry)

## Step 7: Backup and Recovery

### 7.1 Configuration Backup
- Backup all configuration files
- Securely store encryption keys
- Document license UUIDs
- Maintain environment variable records

### 7.2 Data Backup
- Regular MongoDB backups
- Checkpoint data backup
- Sync state preservation
- Recovery procedures documented

## Security Best Practices

1. **Never commit secrets to version control**
2. **Rotate encryption keys regularly**
3. **Use unique licenses per environment**
4. **Enable comprehensive logging**
5. **Monitor authentication attempts**
6. **Implement network segmentation**
7. **Regular security audits**
8. **Keep dependencies updated**

## Support and Maintenance

- Regular updates and patches
- Security vulnerability monitoring
- Performance optimization
- Capacity planning
- Disaster recovery testing