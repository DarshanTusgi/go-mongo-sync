#!/bin/bash
echo "🚀 Starting Cloud Sync..."
cd "$(dirname "$0")"
./cloud-sync -config cloud-config.yaml
