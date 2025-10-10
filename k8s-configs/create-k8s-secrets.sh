#!/bin/bash
# Create Kubernetes secrets for cloud-sync and vm-sync
# OAuth2-based authentication - NO LICENSE DEPENDENCY
# Usage: ./create-k8s-secrets.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🔐 Creating Kubernetes Secrets for Domain-Based Deployment${NC}"
echo -e "${GREEN}✅ OAuth2 Authentication - License Dependency Removed${NC}"

# Create namespace if it doesn't exist
kubectl create namespace data-sync --dry-run=client -o yaml | kubectl apply -f -

# Cloud-Sync Secrets
echo -e "${YELLOW}Creating cloud-sync secrets...${NC}"

# Prompt for MongoDB URI
read -p "Enter MongoDB URI for cloud-sync: " CLOUD_MONGODB_URI

# Generate encryption key
ENCRYPTION_KEY=$(openssl rand -base64 32)
echo -e "${GREEN}Generated encryption key: ${ENCRYPTION_KEY}${NC}"

kubectl create secret generic cloud-sync-secrets \
  --namespace=data-sync \
  --from-literal=mongodb-uri="${CLOUD_MONGODB_URI}" \
  --from-literal=encryption-key="${ENCRYPTION_KEY}" \
  --dry-run=client -o yaml | kubectl apply -f -

# VM-Sync Secrets  
echo -e "${YELLOW}Creating vm-sync secrets...${NC}"

# Prompt for VM MongoDB URI
read -p "Enter MongoDB URI for vm-sync: " VM_MONGODB_URI

# Generate OAuth2 credentials for HTTP telemetry
VM_CLIENT_ID="vm_sync_$(openssl rand -hex 8)"
VM_CLIENT_SECRET=$(openssl rand -hex 32)

echo -e "${GREEN}Generated OAuth2 credentials:${NC}"
echo -e "Client ID: ${VM_CLIENT_ID}"
echo -e "Client Secret: ${VM_CLIENT_SECRET}"

kubectl create secret generic vm-sync-secrets \
  --namespace=data-sync \
  --from-literal=mongodb-uri="${VM_MONGODB_URI}" \
  --from-literal=encryption-key="${ENCRYPTION_KEY}" \
  --from-literal=client-id="${VM_CLIENT_ID}" \
  --from-literal=client-secret="${VM_CLIENT_SECRET}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo -e "${GREEN}✅ Secrets created successfully!${NC}"
echo -e "${GREEN}🚫 License environment variables NO LONGER NEEDED!${NC}"

# Create OAuth2 client registration in cloud-sync
echo -e "${YELLOW}📝 OAuth2 Client Registration Command:${NC}"
echo -e "Execute this after cloud-sync is running:"
echo ""
echo "curl -X POST https://darshan.com/api/auth/clients \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{"
echo "    \"client_id\": \"${VM_CLIENT_ID}\","
echo "    \"client_secret\": \"${VM_CLIENT_SECRET}\","
echo "    \"app_id\": \"vm-sync-xyz-com\","
echo "    \"scopes\": [\"telemetry\", \"sync\"]"
echo "  }'"

echo ""
echo -e "${GREEN}🚀 Ready to deploy! Run: kubectl apply -f k8s-configs/${NC}"
echo -e "${GREEN}✅ Modern OAuth2 authentication - no legacy license system${NC}"