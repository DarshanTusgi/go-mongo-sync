#!/bin/bash

# VM Sync Startup Script  
# Enhanced with filtering integration

echo "Starting VM Sync with Enhanced Filtering..."
echo "Configuration: config/vm-sync-config.yaml"
echo "Features:"
echo "  - Real-time sync with change streams & resume tokens"
echo "  - Document and field filtering"
echo "  - MongoDB checkpoint persistence"
echo ""

# Ensure config exists
if [ ! -f "config/vm-sync-config.yaml" ]; then
    echo "ERROR: config/vm-sync-config.yaml not found!"
    echo "Please ensure configuration file exists before starting."
    exit 1
fi

# Check if executable exists
if [ ! -f "./vm-sync" ]; then
    echo "ERROR: vm-sync executable not found!"
    echo "Please run 'go build -o vm-sync ./cmd/vm-sync' first."
    exit 1
fi

# Make executable if needed
chmod +x ./vm-sync

echo "Starting vm-sync process..."
echo "Press Ctrl+C to stop"
echo "----------------------------------------"

# Start the vm sync process
./vm-sync -config=config/vm-sync-config.yaml