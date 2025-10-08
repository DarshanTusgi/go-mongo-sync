# Go Data Sync HTTP - Build Automation
# Cross-platform build system for cloud-sync and vm-sync

# Build variables
BINARY_DIR = bin
RUNNABLES_DIR = runnables
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

# Build flags
BUILD_FLAGS = -ldflags="-s -w"
BUILD_TIME = $(shell date +%Y%m%d-%H%M%S)
VERSION = $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev-$(BUILD_TIME)")

# Detect OS and architecture
GOOS = $(shell go env GOOS)
GOARCH = $(shell go env GOARCH)

.PHONY: all build clean test deps runnables cloud-sync vm-sync help

# Default target
all: clean deps build runnables

# Help target
help:
	@echo "Go Data Sync HTTP - Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  all        - Clean, install deps, build binaries, and create runnables"
	@echo "  build      - Build both cloud-sync and vm-sync binaries"
	@echo "  cloud-sync - Build only cloud-sync binary"
	@echo "  vm-sync    - Build only vm-sync binary"
	@echo "  runnables  - Create runnables directory with start scripts"
	@echo "  clean      - Remove built binaries and temporary files"
	@echo "  test       - Run all tests"
	@echo "  deps       - Install/update dependencies"
	@echo "  help       - Show this help message"
	@echo ""
	@echo "Build info:"
	@echo "  OS/Arch: $(GOOS)/$(GOARCH)"
	@echo "  Version: $(VERSION)"

# Install dependencies
deps:
	@echo "📦 Installing dependencies..."
	$(GO_GET) ./...
	@echo "✅ Dependencies installed"

# Build both binaries
build: cloud-sync vm-sync
	@echo "🚀 Build completed successfully!"
	@echo "   cloud-sync: $(BINARY_DIR)/$(CLOUD_SYNC_BINARY)"
	@echo "   vm-sync: $(BINARY_DIR)/$(VM_SYNC_BINARY)"

# Build cloud-sync binary
cloud-sync:
	@echo "🔨 Building cloud-sync..."
	@mkdir -p $(BINARY_DIR)
	$(GO_BUILD) $(BUILD_FLAGS) -o $(BINARY_DIR)/$(CLOUD_SYNC_BINARY) ./$(CLOUD_SYNC_SRC)
	@echo "✅ cloud-sync built: $(BINARY_DIR)/$(CLOUD_SYNC_BINARY)"

# Build vm-sync binary
vm-sync:
	@echo "🔨 Building vm-sync..."
	@mkdir -p $(BINARY_DIR)
	$(GO_BUILD) $(BUILD_FLAGS) -o $(BINARY_DIR)/$(VM_SYNC_BINARY) ./$(VM_SYNC_SRC)
	@echo "✅ vm-sync built: $(BINARY_DIR)/$(VM_SYNC_BINARY)"

# Create runnables directory with start scripts
runnables: build
	@echo "📁 Creating runnables directory..."
	@mkdir -p $(RUNNABLES_DIR)
	
	# Copy binaries to runnables
	@cp $(BINARY_DIR)/$(CLOUD_SYNC_BINARY) $(RUNNABLES_DIR)/
	@cp $(BINARY_DIR)/$(VM_SYNC_BINARY) $(RUNNABLES_DIR)/
	
	# Copy example configs
	@cp examples/cloud-config.yaml $(RUNNABLES_DIR)/
	@cp examples/vm-config.yaml $(RUNNABLES_DIR)/
	
	# Create start scripts based on OS
ifeq ($(GOOS),windows)
	@echo "Creating Windows start scripts..."
	@echo '@echo off' > $(RUNNABLES_DIR)/start-cloud-sync.bat
	@echo 'echo Starting Cloud Sync...' >> $(RUNNABLES_DIR)/start-cloud-sync.bat
	@echo './cloud-sync.exe -config cloud-config.yaml' >> $(RUNNABLES_DIR)/start-cloud-sync.bat
	
	@echo '@echo off' > $(RUNNABLES_DIR)/start-vm-sync.bat
	@echo 'echo Starting VM Sync...' >> $(RUNNABLES_DIR)/start-vm-sync.bat
	@echo './vm-sync.exe -config vm-config.yaml' >> $(RUNNABLES_DIR)/start-vm-sync.bat
	
	@echo "✅ Windows runnables created in $(RUNNABLES_DIR)/"
else
	@echo "Creating Unix start scripts..."
	@echo '#!/bin/bash' > $(RUNNABLES_DIR)/start-cloud-sync.sh
	@echo 'echo "🚀 Starting Cloud Sync..."' >> $(RUNNABLES_DIR)/start-cloud-sync.sh
	@echo 'cd "$$(dirname "$$0")"' >> $(RUNNABLES_DIR)/start-cloud-sync.sh
	@echo './$(CLOUD_SYNC_BINARY) -config cloud-config.yaml' >> $(RUNNABLES_DIR)/start-cloud-sync.sh
	@chmod +x $(RUNNABLES_DIR)/start-cloud-sync.sh
	
	@echo '#!/bin/bash' > $(RUNNABLES_DIR)/start-vm-sync.sh
	@echo 'echo "🚀 Starting VM Sync..."' >> $(RUNNABLES_DIR)/start-vm-sync.sh
	@echo 'cd "$$(dirname "$$0")"' >> $(RUNNABLES_DIR)/start-vm-sync.sh
	@echo './$(VM_SYNC_BINARY) -config vm-config.yaml' >> $(RUNNABLES_DIR)/start-vm-sync.sh
	@chmod +x $(RUNNABLES_DIR)/start-vm-sync.sh
	
	@echo "✅ Unix runnables created in $(RUNNABLES_DIR)/"
endif
	
	@echo ""
	@echo "📋 Runnables Summary:"
	@echo "   Directory: $(RUNNABLES_DIR)/"
	@echo "   Binaries: $(CLOUD_SYNC_BINARY), $(VM_SYNC_BINARY)"
	@echo "   Configs: cloud-config.yaml, vm-config.yaml"
ifeq ($(GOOS),windows)
	@echo "   Scripts: start-cloud-sync.bat, start-vm-sync.bat"
else
	@echo "   Scripts: start-cloud-sync.sh, start-vm-sync.sh"
endif

# Run tests
test:
	@echo "🧪 Running tests..."
	$(GO_TEST) -v ./...

# Clean build artifacts
clean:
	@echo "🧹 Cleaning build artifacts..."
	@rm -rf $(BINARY_DIR)
	@rm -rf $(RUNNABLES_DIR)
	@rm -f *.log
	@rm -f *-test.yaml
	@rm -f test_*.sh
	$(GO_CLEAN)
	@echo "✅ Cleanup completed"

# Development targets
dev-cloud-sync: cloud-sync
	@echo "🔧 Starting cloud-sync in development mode..."
	./$(BINARY_DIR)/$(CLOUD_SYNC_BINARY) -config examples/cloud-config.yaml

dev-vm-sync: vm-sync
	@echo "🔧 Starting vm-sync in development mode..."
	./$(BINARY_DIR)/$(VM_SYNC_BINARY) -config examples/vm-config.yaml