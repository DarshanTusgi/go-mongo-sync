# Go Data Sync HTTP - Ultra-Stable Build Automation
# Cross-platform build system with production-ready configurations

# Build variables
BINARY_DIR = bin
RUNNABLES_DIR = runnables
CONFIG_DIR = configs
GO_CMD = go
GO_BUILD = $(GO_CMD) build
GO_CLEAN = $(GO_CMD) clean
GO_TEST = $(GO_CMD) test
GO_GET = $(GO_CMD) get

# Binary names
CLOUD_SYNC_BINARY = cloud-sync
VM_SYNC_BINARY = vm-sync

# Source directories
CLOUD_SYNC_SRC = cmd/cloud-sync
VM_SYNC_SRC = cmd/vm-sync

# Build flags for optimized production binaries
BUILD_FLAGS = -ldflags="-s -w -X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"
BUILD_TIME = $(shell date +%Y%m%d-%H%M%S)
VERSION = $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev-$(BUILD_TIME)")

# Detect OS and architecture
GOOS = $(shell go env GOOS)
GOARCH = $(shell go env GOARCH)

# Configuration files
CLOUD_CONFIG = config.yaml
VM_CONFIG = test-vm-config.yaml
CLOUD_EXAMPLE_CONFIG = examples/cloud-config.yaml
VM_EXAMPLE_CONFIG = examples/vm-config.yaml

.PHONY: all build clean test deps runnables cloud-sync vm-sync help run-cloud-sync run-vm-sync stop setup-configs

# Default target
all: clean deps build runnables
	@echo ""
	@echo "🎉 Build completed successfully!"
	@echo "   📁 Binaries: $(BINARY_DIR)/"
	@echo "   📁 Runnables: $(RUNNABLES_DIR)/"
	@echo "   📁 Configs: $(CONFIG_DIR)/"
	@echo ""
	@echo "🚀 Quick start:"
ifeq ($(GOOS),windows)
	@echo "   Cloud-sync: .\\$(RUNNABLES_DIR)\\start-cloud-sync.bat"
	@echo "   VM-sync:    .\\$(RUNNABLES_DIR)\\start-vm-sync.bat"
else
	@echo "   Cloud-sync: ./$(RUNNABLES_DIR)/start-cloud-sync.sh"
	@echo "   VM-sync:    ./$(RUNNABLES_DIR)/start-vm-sync.sh"
endif

# Help target
help:
	@echo "Go Data Sync HTTP - Ultra-Stable Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  all           - Clean, install deps, build binaries, and create runnables"
	@echo "  build         - Build both cloud-sync and vm-sync binaries"
	@echo "  cloud-sync    - Build only cloud-sync binary"
	@echo "  vm-sync       - Build only vm-sync binary"
	@echo "  runnables     - Create runnables directory with ultra-stable configs"
	@echo "  setup-configs - Copy ultra-stable configs to project root"
	@echo "  run-cloud-sync - Run cloud-sync with ultra-stable configuration"
	@echo "  run-vm-sync   - Run vm-sync with ultra-stable configuration"
	@echo "  stop          - Stop all running instances"
	@echo "  clean         - Remove built binaries and temporary files"
	@echo "  test          - Run all tests"
	@echo "  deps          - Install/update dependencies"
	@echo "  help          - Show this help message"
	@echo ""
	@echo "Build info:"
	@echo "  OS/Arch: $(GOOS)/$(GOARCH)"
	@echo "  Version: $(VERSION)"
	@echo "  TCP Transport: Ultra-Stable (200s timeouts, 1MB buffers, 64MB batches)"
	@echo "  Sync Mode: Scheduler-based (30m intervals)"

# Install dependencies
deps:
	@echo "📦 Installing dependencies..."
	$(GO_GET) ./...
	@echo "✅ Dependencies installed"

# Build both binaries with optimized flags
build: cloud-sync vm-sync
	@echo ""
	@echo "🚀 Build completed successfully!"
	@echo "   cloud-sync: $(BINARY_DIR)/$(CLOUD_SYNC_BINARY)"
	@echo "   vm-sync: $(BINARY_DIR)/$(VM_SYNC_BINARY)"
	@echo "   Build flags: $(BUILD_FLAGS)"

# Build cloud-sync binary with production optimizations
cloud-sync:
	@echo "🔨 Building cloud-sync (ultra-stable)..."
	@mkdir -p $(BINARY_DIR)
	$(GO_BUILD) $(BUILD_FLAGS) -o $(BINARY_DIR)/$(CLOUD_SYNC_BINARY) ./$(CLOUD_SYNC_SRC)
	@echo "✅ cloud-sync built: $(BINARY_DIR)/$(CLOUD_SYNC_BINARY)"

# Build vm-sync binary with production optimizations
vm-sync:
	@echo "🔨 Building vm-sync (ultra-stable)..."
	@mkdir -p $(BINARY_DIR)
	$(GO_BUILD) $(BUILD_FLAGS) -o $(BINARY_DIR)/$(VM_SYNC_BINARY) ./$(VM_SYNC_SRC)
	@echo "✅ vm-sync built: $(BINARY_DIR)/$(VM_SYNC_BINARY)"

# Setup ultra-stable configurations in project root
setup-configs:
	@echo "📋 Setting up ultra-stable configurations..."
	@mkdir -p $(CONFIG_DIR)
	@cp $(CLOUD_CONFIG) $(CONFIG_DIR)/cloud-production.yaml 2>/dev/null || echo "Note: $(CLOUD_CONFIG) not found"
	@cp $(VM_CONFIG) $(CONFIG_DIR)/vm-production.yaml 2>/dev/null || echo "Note: $(VM_CONFIG) not found"
	@cp $(CLOUD_EXAMPLE_CONFIG) $(CONFIG_DIR)/cloud-example.yaml
	@cp $(VM_EXAMPLE_CONFIG) $(CONFIG_DIR)/vm-example.yaml
	@echo "✅ Configuration files copied to $(CONFIG_DIR)/"
	@echo "   Production configs: cloud-production.yaml, vm-production.yaml"
	@echo "   Example configs: cloud-example.yaml, vm-example.yaml"

# Create runnables directory with ultra-stable configurations and start scripts
runnables: build setup-configs
	@echo "📁 Creating ultra-stable runnables directory..."
	@mkdir -p $(RUNNABLES_DIR)
	
	# Copy binaries to runnables
	@cp $(BINARY_DIR)/$(CLOUD_SYNC_BINARY) $(RUNNABLES_DIR)/
	@cp $(BINARY_DIR)/$(VM_SYNC_BINARY) $(RUNNABLES_DIR)/
	
	# Copy ultra-stable configs
	@cp $(CLOUD_CONFIG) $(RUNNABLES_DIR)/cloud-config.yaml 2>/dev/null || cp $(CLOUD_EXAMPLE_CONFIG) $(RUNNABLES_DIR)/cloud-config.yaml
	@cp $(VM_CONFIG) $(RUNNABLES_DIR)/vm-config.yaml 2>/dev/null || cp $(VM_EXAMPLE_CONFIG) $(RUNNABLES_DIR)/vm-config.yaml
	
	# Copy example configs as backup
	@cp $(CLOUD_EXAMPLE_CONFIG) $(RUNNABLES_DIR)/cloud-config-example.yaml
	@cp $(VM_EXAMPLE_CONFIG) $(RUNNABLES_DIR)/vm-config-example.yaml
	
	# Create start scripts based on OS
ifeq ($(GOOS),windows)
	@echo "Creating Windows ultra-stable start scripts..."
	@echo '@echo off' > $(RUNNABLES_DIR)/start-cloud-sync.bat
	@echo 'echo 🚀 Starting Ultra-Stable Cloud Sync...' >> $(RUNNABLES_DIR)/start-cloud-sync.bat
	@echo 'echo   - TCP Transport: Ultra-stable (180s timeouts, 1MB buffers)' >> $(RUNNABLES_DIR)/start-cloud-sync.bat
	@echo 'echo   - Sync Mode: Scheduler-based (30m intervals)' >> $(RUNNABLES_DIR)/start-cloud-sync.bat
	@echo 'echo   - Buffer Size: 1MB, Max Batch: 64MB' >> $(RUNNABLES_DIR)/start-cloud-sync.bat
	@echo 'echo   - Retries: 8, Backoff: 3s' >> $(RUNNABLES_DIR)/start-cloud-sync.bat
	@echo './cloud-sync.exe -config cloud-config.yaml' >> $(RUNNABLES_DIR)/start-cloud-sync.bat
	
	@echo '@echo off' > $(RUNNABLES_DIR)/start-vm-sync.bat
	@echo 'echo 🚀 Starting Ultra-Stable VM Sync...' >> $(RUNNABLES_DIR)/start-vm-sync.bat
	@echo 'echo   - TCP Transport: Ultra-stable (200s read timeout, 120s write timeout)' >> $(RUNNABLES_DIR)/start-vm-sync.bat
	@echo 'echo   - Sync Mode: Scheduler-based (30m intervals)' >> $(RUNNABLES_DIR)/start-vm-sync.bat
	@echo 'echo   - Buffer Size: 1MB, Max Batch: 64MB' >> $(RUNNABLES_DIR)/start-vm-sync.bat
	@echo 'echo   - Heartbeat: 90s, Connections: 20' >> $(RUNNABLES_DIR)/start-vm-sync.bat
	@echo './vm-sync.exe -config vm-config.yaml' >> $(RUNNABLES_DIR)/start-vm-sync.bat
	
	@echo 'taskkill /F /IM cloud-sync.exe 2>nul || echo No cloud-sync process found' > $(RUNNABLES_DIR)/stop-all.bat
	@echo 'taskkill /F /IM vm-sync.exe 2>nul || echo No vm-sync process found' >> $(RUNNABLES_DIR)/stop-all.bat
	@echo 'echo ✅ All processes stopped' >> $(RUNNABLES_DIR)/stop-all.bat
	
	@echo "✅ Windows ultra-stable runnables created in $(RUNNABLES_DIR)/"
else
	@echo "Creating Unix ultra-stable start scripts..."
	@echo '#!/bin/bash' > $(RUNNABLES_DIR)/start-cloud-sync.sh
	@echo 'echo "🚀 Starting Ultra-Stable Cloud Sync..."' >> $(RUNNABLES_DIR)/start-cloud-sync.sh
	@echo 'echo "   - TCP Transport: Ultra-stable (180s timeouts, 1MB buffers)"' >> $(RUNNABLES_DIR)/start-cloud-sync.sh
	@echo 'echo "   - Sync Mode: Scheduler-based (30m intervals)"' >> $(RUNNABLES_DIR)/start-cloud-sync.sh
	@echo 'echo "   - Buffer Size: 1MB, Max Batch: 64MB"' >> $(RUNNABLES_DIR)/start-cloud-sync.sh
	@echo 'echo "   - Retries: 8, Backoff: 3s"' >> $(RUNNABLES_DIR)/start-cloud-sync.sh
	@echo 'echo ""' >> $(RUNNABLES_DIR)/start-cloud-sync.sh
	@echo 'cd "$$(dirname "$$0")"' >> $(RUNNABLES_DIR)/start-cloud-sync.sh
	@echo './$(CLOUD_SYNC_BINARY) -config cloud-config.yaml' >> $(RUNNABLES_DIR)/start-cloud-sync.sh
	@chmod +x $(RUNNABLES_DIR)/start-cloud-sync.sh
	
	@echo '#!/bin/bash' > $(RUNNABLES_DIR)/start-vm-sync.sh
	@echo 'echo "🚀 Starting Ultra-Stable VM Sync..."' >> $(RUNNABLES_DIR)/start-vm-sync.sh
	@echo 'echo "   - TCP Transport: Ultra-stable (200s read, 120s write timeouts)"' >> $(RUNNABLES_DIR)/start-vm-sync.sh
	@echo 'echo "   - Sync Mode: Scheduler-based (30m intervals)"' >> $(RUNNABLES_DIR)/start-vm-sync.sh
	@echo 'echo "   - Buffer Size: 1MB, Max Batch: 64MB"' >> $(RUNNABLES_DIR)/start-vm-sync.sh
	@echo 'echo "   - Heartbeat: 90s, Connections: 20"' >> $(RUNNABLES_DIR)/start-vm-sync.sh
	@echo 'echo ""' >> $(RUNNABLES_DIR)/start-vm-sync.sh
	@echo 'cd "$$(dirname "$$0")"' >> $(RUNNABLES_DIR)/start-vm-sync.sh
	@echo './$(VM_SYNC_BINARY) -config vm-config.yaml' >> $(RUNNABLES_DIR)/start-vm-sync.sh
	@chmod +x $(RUNNABLES_DIR)/start-vm-sync.sh
	
	@echo '#!/bin/bash' > $(RUNNABLES_DIR)/stop-all.sh
	@echo 'echo "🛑 Stopping all sync processes..."' >> $(RUNNABLES_DIR)/stop-all.sh
	@echo 'pkill -f "$(CLOUD_SYNC_BINARY)" 2>/dev/null || echo "No cloud-sync process found"' >> $(RUNNABLES_DIR)/stop-all.sh
	@echo 'pkill -f "$(VM_SYNC_BINARY)" 2>/dev/null || echo "No vm-sync process found"' >> $(RUNNABLES_DIR)/stop-all.sh
	@echo 'echo "✅ All processes stopped"' >> $(RUNNABLES_DIR)/stop-all.sh
	@chmod +x $(RUNNABLES_DIR)/stop-all.sh
	
	@echo "✅ Unix ultra-stable runnables created in $(RUNNABLES_DIR)/"
endif
	
	@echo ""
	@echo "📋 Ultra-Stable Runnables Summary:"
	@echo "   Directory: $(RUNNABLES_DIR)/"
	@echo "   Binaries: $(CLOUD_SYNC_BINARY), $(VM_SYNC_BINARY)"
	@echo "   Configs: cloud-config.yaml, vm-config.yaml"
	@echo "   Examples: cloud-config-example.yaml, vm-config-example.yaml"
ifeq ($(GOOS),windows)
	@echo "   Scripts: start-cloud-sync.bat, start-vm-sync.bat, stop-all.bat"
else
	@echo "   Scripts: start-cloud-sync.sh, start-vm-sync.sh, stop-all.sh"
endif
	@echo ""
	@echo "🚀 Ultra-Stable Features:"
	@echo "   - TCP timeouts: 180-200s for maximum stability"
	@echo "   - Buffer sizes: 1MB for high throughput"
	@echo "   - Batch sizes: 64MB for efficiency"
	@echo "   - Retry logic: 8 attempts with 3s backoff"
	@echo "   - Scheduler sync: 30m intervals (production)"
	@echo "   - Error handling: Progressive timeout adaptation"

# Direct run targets using ultra-stable configurations
run-cloud-sync: cloud-sync
	@echo "🔧 Starting cloud-sync with ultra-stable configuration..."
	@echo "   Config: $(CLOUD_CONFIG) (fallback: $(CLOUD_EXAMPLE_CONFIG))"
	./$(BINARY_DIR)/$(CLOUD_SYNC_BINARY) -config $(CLOUD_CONFIG) 2>/dev/null || ./$(BINARY_DIR)/$(CLOUD_SYNC_BINARY) -config $(CLOUD_EXAMPLE_CONFIG)

run-vm-sync: vm-sync
	@echo "🔧 Starting vm-sync with ultra-stable configuration..."
	@echo "   Config: $(VM_CONFIG) (fallback: $(VM_EXAMPLE_CONFIG))"
	./$(BINARY_DIR)/$(VM_SYNC_BINARY) -config $(VM_CONFIG) 2>/dev/null || ./$(BINARY_DIR)/$(VM_SYNC_BINARY) -config $(VM_EXAMPLE_CONFIG)

# Stop all running processes
stop:
	@echo "🛑 Stopping all sync processes..."
ifeq ($(GOOS),windows)
	taskkill /F /IM $(CLOUD_SYNC_BINARY).exe 2>nul || echo "No cloud-sync process found"
	taskkill /F /IM $(VM_SYNC_BINARY).exe 2>nul || echo "No vm-sync process found"
else
	pkill -f "$(CLOUD_SYNC_BINARY)" 2>/dev/null || echo "No cloud-sync process found"
	pkill -f "$(VM_SYNC_BINARY)" 2>/dev/null || echo "No vm-sync process found"
endif
	@echo "✅ All processes stopped"

# Run tests
test:
	@echo "🧪 Running tests..."
	$(GO_TEST) -v ./...

# Clean build artifacts and temporary files
clean:
	@echo "🧹 Cleaning build artifacts..."
	@rm -rf $(BINARY_DIR)
	@rm -rf $(RUNNABLES_DIR)
	@rm -rf $(CONFIG_DIR)
	@rm -f *.log
	@rm -f *-test.yaml
	@rm -f test_*.sh
	@rm -f /tmp/vm-sync-tcp-checkpoints/*.json 2>/dev/null || true
	$(GO_CLEAN)
	@echo "✅ Cleanup completed"

# Development targets with ultra-stable configs
dev-cloud-sync: cloud-sync
	@echo "🔧 Starting cloud-sync in development mode (ultra-stable)..."
	./$(BINARY_DIR)/$(CLOUD_SYNC_BINARY) -config $(CLOUD_EXAMPLE_CONFIG)

dev-vm-sync: vm-sync
	@echo "🔧 Starting vm-sync in development mode (ultra-stable)..."
	./$(BINARY_DIR)/$(VM_SYNC_BINARY) -config $(VM_EXAMPLE_CONFIG)

# Production deployment target
deploy: all
	@echo "🚀 Preparing production deployment..."
	@echo "   ✅ Binaries built with optimizations"
	@echo "   ✅ Ultra-stable configurations ready"
	@echo "   ✅ Start scripts created"
	@echo "   ✅ Stop scripts included"
	@echo ""
	@echo "📝 Deployment checklist:"
	@echo "   1. Copy $(RUNNABLES_DIR)/ to target server"
	@echo "   2. Update configs with production credentials"
	@echo "   3. Set environment variables if needed"
	@echo "   4. Run start scripts to begin sync"
	@echo ""
	@echo "🔒 Security reminders:"
	@echo "   - Update MongoDB URIs for production"
	@echo "   - Set proper encryption keys"
	@echo "   - Configure OAuth2 credentials"
	@echo "   - Use TLS for production networks"