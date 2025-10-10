#!/bin/bash

# Cloud Sync Startup Script
# Enhanced with filtering integration across initial dump and real-time sync

echo "Starting Cloud Sync with Enhanced Filtering..."
echo "Configuration: config/cloud-sync-config.yaml"
echo "Features:"
echo "  - Initial dump with document & field filtering"
echo "  - Real-time sync with change streams & resume tokens"
echo "  - TCP transport for efficient data transfer"
echo "  - Consistent filtering engine across all modes"
echo ""

# Ensure config exists
if [ ! -f "config/cloud-sync-config.yaml" ]; then
    echo "ERROR: config/cloud-sync-config.yaml not found!"
    echo "Please ensure configuration file exists before starting."
    exit 1
fi

# Check if executable exists
if [ ! -f "./cloud-sync" ]; then
    echo "ERROR: cloud-sync executable not found!"
    echo "Please run 'go build -o cloud-sync ./cmd/cloud-sync' first."
    exit 1
fi

# Make executable if needed
chmod +x ./cloud-sync

echo "Starting cloud-sync process..."
echo "Press Ctrl+C to stop"
echo "----------------------------------------"

# Start the cloud sync process
./cloud-sync -config=config/cloud-sync-config.yaml