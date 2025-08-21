#!/bin/bash

# Cloud-sync startup script
# This script starts the cloud-sync service with the test configuration

echo "🚀 Starting Cloud-sync Service..."
echo "Configuration: test-cloud-config.yaml"
echo "Port: 8080"
echo "WebSocket: ws://localhost:8080/ws"
echo "API: http://localhost:8080/api/data"
echo "Dashboard: http://localhost:8080"
echo ""

# Set the license key
export CLOUD_SYNC_LICENSE=123e4567-e89b-12d3-a456-426614174000

# Start cloud-sync
./bin/cloud-sync -config test-cloud-config.yaml