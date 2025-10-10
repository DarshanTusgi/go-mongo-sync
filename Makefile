# Go Data Sync HTTP - Makefile
# Builds binaries, configurations, and start scripts in runnables/ directory

.PHONY: all clean build runnables cloud-sync vm-sync test lint help create-dirs build-cloud-sync build-vm-sync copy-configs generate-scripts

# Variables
BINARY_DIR := runnables
CONFIG_DIR := $(BINARY_DIR)/configs
SCRIPTS_DIR := $(BINARY_DIR)/scripts
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
VERSION := v1.0.0

# Build flags
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME) -X main.gitCommit=$(GIT_COMMIT)"

# Default target
all: runnables

# Help target
help:
	@echo "Go Data Sync HTTP - Build System"
	@echo ""
	@echo "Usage:"
	@echo "  make runnables    Build all binaries, configs, and start scripts"
	@echo "  make cloud-sync   Build only cloud-sync binary"
	@echo "  make vm-sync      Build only vm-sync binary"
	@echo "  make clean        Remove runnables directory"
	@echo "  make test         Run all tests"
	@echo "  make lint         Run code linting"
	@echo "  make help         Show this help message"
	@echo ""

# Clean target - completely remove runnables directory
clean:
	@echo "🧹 Cleaning runnables directory..."
	@rm -rf $(BINARY_DIR)
	@echo "✅ Runnables directory cleaned"

# Create directory structure
create-dirs:
	@echo "📁 Creating runnables directory structure..."
	@mkdir -p $(BINARY_DIR)
	@mkdir -p $(CONFIG_DIR)
	@mkdir -p $(SCRIPTS_DIR)

# Build cloud-sync binary
build-cloud-sync: create-dirs
	@echo "🏗️ Building cloud-sync binary..."
	@go build $(LDFLAGS) -o $(BINARY_DIR)/cloud-sync ./cmd/cloud-sync
	@chmod +x $(BINARY_DIR)/cloud-sync
	@echo "✅ cloud-sync binary built"

# Build vm-sync binary
build-vm-sync: create-dirs
	@echo "🏗️ Building vm-sync binary..."
	@go build $(LDFLAGS) -o $(BINARY_DIR)/vm-sync ./cmd/vm-sync
	@chmod +x $(BINARY_DIR)/vm-sync
	@echo "✅ vm-sync binary built"

# Copy configuration files
copy-configs: create-dirs
	@echo "📋 Copying configuration files..."
	@cp configs/cloud-example.yaml $(CONFIG_DIR)/cloud-example.yaml
	@cp configs/cloud-production.yaml $(CONFIG_DIR)/cloud-production.yaml
	@cp configs/vm-example.yaml $(CONFIG_DIR)/vm-example.yaml
	@cp configs/vm-production.yaml $(CONFIG_DIR)/vm-production.yaml
	@echo "✅ Configuration files copied"

# Generate start scripts
generate-scripts: create-dirs
	@echo "📜 Generating start scripts..."
	@./scripts/generate-start-scripts.sh $(SCRIPTS_DIR)
	@chmod +x $(SCRIPTS_DIR)/*.sh
	@echo "✅ Start scripts generated"

# Individual build targets
cloud-sync: build-cloud-sync

vm-sync: build-vm-sync

# Main runnables target - builds everything
runnables: clean build-cloud-sync build-vm-sync copy-configs generate-scripts
	@echo ""
	@echo "🎉 Runnables build completed successfully!"
	@echo ""
	@echo "📁 Directory Structure:"
	@find $(BINARY_DIR) -type f -exec echo "  {}" \;
	@echo ""
	@echo "🚀 Quick Start:"
	@echo "  ./$(SCRIPTS_DIR)/start-cloud-sync.sh    # Start cloud-sync"
	@echo "  ./$(SCRIPTS_DIR)/start-vm-sync.sh       # Start vm-sync"
	@echo "  ./$(SCRIPTS_DIR)/deploy.sh start        # Start both services"
	@echo ""
	@echo "📁 Configuration Files:"
	@echo "  $(CONFIG_DIR)/cloud-example.yaml        # Cloud-sync example config"
	@echo "  $(CONFIG_DIR)/cloud-production.yaml     # Cloud-sync production config"
	@echo "  $(CONFIG_DIR)/vm-example.yaml           # VM-sync example config"
	@echo "  $(CONFIG_DIR)/vm-production.yaml        # VM-sync production config"
	@echo ""
	@echo "🌐 Access Points:"
	@echo "  Cloud-sync Dashboard: http://localhost:8080/dashboard"
	@echo "  VM-sync Health: http://localhost:8081/health"
	@echo ""

# Test target
test:
	@echo "🧪 Running tests..."
	@go test ./... -v

# Lint target
lint:
	@echo "🔍 Running linter..."
	@golangci-lint run ./... || echo "Install golangci-lint for linting support"

# Build benchmark
benchmark:
	@echo "📊 Building benchmark..."
	@go build -o examples/benchmark examples/benchmark_analysis.go
	@echo "✅ Run with: ./examples/benchmark"