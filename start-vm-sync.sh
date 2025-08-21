#!/bin/bash

# VM-sync startup script
# This script starts the vm-sync service with the example configuration

echo "🚀 Starting VM-sync Service..."
echo "Configuration: examples/vm-config.yaml"
echo "Port: 8081"
echo "Connecting to Cloud-sync: ws://localhost:8080/ws"
echo ""

# Set the license key
export VM_SYNC_LICENSE=123e4567-e89b-12d3-a456-426614174000

# Start vm-sync
./bin/vm-sync -config examples/vm-config.yaml