#!/bin/bash
echo "🚀 Starting VM Sync..."
cd "$(dirname "$0")"
./vm-sync -config vm-config.yaml
