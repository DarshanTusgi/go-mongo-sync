# 🚀 Runnables Build System Guide

## Overview

The `make runnables` command creates a complete deployment package in the `runnables/` directory, following your project specifications for rebuilt binaries, configurations, and executable start scripts.

## 📁 Directory Structure

After running `make runnables`, you'll get this structure:

```
runnables/
├── cloud-sync              # Built binary (no license dependency)
├── vm-sync                 # Built binary (no license dependency)  
├── configs/
│   ├── cloud-config.yaml  # Cloud-sync configuration
│   ├── vm-config.yaml     # VM-sync configuration
│   └── oauth2-vm-config.yaml # OAuth2-based VM config
└── scripts/
    ├── start-cloud-sync.sh    # Cloud-sync start script
    ├── start-vm-sync.sh       # VM-sync start script
    └── deploy.sh              # Deployment helper script
```

## 🛠️ Build Commands

### Main Commands
```bash
# Build everything (runnables directory + binaries + configs + scripts)
make runnables

# Build individual components
make cloud-sync      # Build only cloud-sync binary
make vm-sync         # Build only vm-sync binary

# Clean and rebuild (removes entire runnables directory)
make clean           # Remove runnables directory
make runnables       # Build everything fresh
```

### Additional Commands
```bash
make help           # Show available commands
make test           # Run all tests
make lint           # Run code linting
make benchmark      # Build benchmark tool
```

## 🚀 Quick Start

### 1. Build Runnables
```bash
make runnables
```

### 2. Start Services

**Option 1: Start individually**
```bash
./runnables/scripts/start-cloud-sync.sh    # Terminal 1
./runnables/scripts/start-vm-sync.sh       # Terminal 2
```

**Option 2: Use deployment helper**
```bash
./runnables/scripts/deploy.sh start        # Starts both in background
./runnables/scripts/deploy.sh status       # Check status
./runnables/scripts/deploy.sh logs         # View logs
./runnables/scripts/deploy.sh stop         # Stop all services
```

## 🔧 Key Features

### ✅ License Dependency Removed
- **No `CLOUD_SYNC_LICENSE` environment variable needed**
- **No `VM_SYNC_LICENSE` environment variable needed**
- Uses OAuth2 authentication instead

### ✅ Configuration Compliance
- **Cloud-sync starts with `-config` flag** (per your memory requirement)
- **Real-time WebSocket synchronization enabled**
- **HTTP telemetry replaces WebSocket telemetry**

### ✅ Build Process
- **Complete rebuild**: `runnables/` directory is removed and recreated
- **Fresh binaries**: Always builds latest code
- **Updated scripts**: Auto-generated start scripts with current paths

### ✅ Smart Prerequisites Checking
- **Binary existence**: Scripts check if binaries exist before starting
- **Configuration files**: Validates config files are present
- **Connectivity checks**: VM-sync checks cloud-sync availability
- **Colored output**: Clear visual feedback for all operations

## 🌐 Access Points

After starting services:

- **Cloud-sync Dashboard**: http://localhost:8080/dashboard
- **Cloud-sync Health**: http://localhost:8080/health  
- **VM-sync Health**: http://localhost:8081/health
- **WebSocket**: ws://localhost:8080/ws
- **TCP Transport**: localhost:9000

## 📋 Build Specifications Compliance

### ✅ Memory Requirements Met
- **Runnables directory**: Stores built binaries, configs, and scripts ✅
- **Complete rebuild**: Directory removed and recreated each time ✅  
- **Start scripts**: Auto-generated and placed in runnables/ ✅
- **Configuration verification**: Always uses correct config files ✅

### ✅ Runtime Requirements Met
- **Cloud-sync `-config` flag**: Required for real-time sync ✅
- **OAuth2 authentication**: JWT-based, no license dependency ✅
- **Port configuration**: Cloud-sync:8080, VM-sync:8081, TCP:9000 ✅
- **Shared license removal**: Eliminated legacy UUID system ✅

## 🔄 Development Workflow

### Standard Workflow
```bash
# 1. Make code changes
# 2. Rebuild everything
make runnables

# 3. Test services
./runnables/scripts/deploy.sh start
./runnables/scripts/deploy.sh status

# 4. View logs if needed
./runnables/scripts/deploy.sh logs

# 5. Stop when done
./runnables/scripts/deploy.sh stop
```

### Debug Workflow
```bash
# Build and start cloud-sync only
make cloud-sync
./runnables/scripts/start-cloud-sync.sh

# In another terminal, build and start vm-sync
make vm-sync  
./runnables/scripts/start-vm-sync.sh
```

## 🎯 Advanced Usage

### Custom Configuration
```bash
# Use different config files
BINARY_DIR=runnables
$BINARY_DIR/cloud-sync -config custom-config.yaml
$BINARY_DIR/vm-sync -config custom-vm-config.yaml
```

### Background Services
```bash
# Start services in background with custom logs
nohup ./runnables/scripts/start-cloud-sync.sh > my-cloud.log 2>&1 &
nohup ./runnables/scripts/start-vm-sync.sh > my-vm.log 2>&1 &
```

### Health Monitoring
```bash
# Check if services are responding
curl http://localhost:8080/health    # Cloud-sync
curl http://localhost:8081/health    # VM-sync
```

## 🎉 Summary

The `make runnables` command provides:

1. **Complete deployment package** - All binaries, configs, and scripts
2. **OAuth2 authentication** - No legacy license dependency  
3. **Smart start scripts** - Prerequisite checking and colored output
4. **Development-friendly** - Easy rebuild and restart workflow
5. **Production-ready** - Background deployment with process management

This follows your project specifications exactly, ensuring the runnables directory is completely rebuilt each time with fresh binaries and up-to-date configurations.