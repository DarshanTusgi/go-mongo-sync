#!/bin/bash
# Deploy cloud-sync and vm-sync to Kubernetes with domain-based routing
# Usage: ./deploy-k8s.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Deploying Domain-Based Data Sync to Kubernetes${NC}"
echo -e "${BLUE}   darshan.com → Cloud-Sync${NC}"
echo -e "${BLUE}   xyz.com → VM-Sync${NC}"
echo -e ""

# Check prerequisites
echo -e "${YELLOW}🔍 Checking prerequisites...${NC}"

# Check kubectl
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}❌ kubectl not found. Please install kubectl.${NC}"
    exit 1
fi

# Check cluster connection
if ! kubectl cluster-info &> /dev/null; then
    echo -e "${RED}❌ Cannot connect to Kubernetes cluster. Please check your kubeconfig.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Prerequisites met${NC}"

# Create namespace
echo -e "${YELLOW}📁 Creating namespace...${NC}"
kubectl create namespace data-sync --dry-run=client -o yaml | kubectl apply -f -

# Deploy secrets if they don't exist
if ! kubectl get secret cloud-sync-secrets -n data-sync &> /dev/null; then
    echo -e "${YELLOW}🔐 Secrets not found. Please run ./create-k8s-secrets.sh first${NC}"
    read -p "Do you want to create secrets now? (y/n): " CREATE_SECRETS
    if [[ $CREATE_SECRETS == "y" ]]; then
        ./create-k8s-secrets.sh
    else
        echo -e "${RED}❌ Secrets required for deployment. Exiting.${NC}"
        exit 1
    fi
fi

# Update ConfigMaps with configuration files
echo -e "${YELLOW}📝 Updating ConfigMaps...${NC}"

# Update cloud-sync config
kubectl create configmap cloud-sync-config \
    --namespace=data-sync \
    --from-file=config.yaml=cloud-sync-domain.yaml \
    --dry-run=client -o yaml | kubectl apply -f -

# Update vm-sync config  
kubectl create configmap vm-sync-config \
    --namespace=data-sync \
    --from-file=config.yaml=vm-sync-domain.yaml \
    --dry-run=client -o yaml | kubectl apply -f -

echo -e "${GREEN}✅ ConfigMaps updated${NC}"

# Deploy cloud-sync
echo -e "${YELLOW}☁️  Deploying cloud-sync...${NC}"
kubectl apply -f cloud-sync-deployment.yaml

# Wait for cloud-sync to be ready
echo -e "${YELLOW}⏳ Waiting for cloud-sync to be ready...${NC}"
kubectl wait --for=condition=available --timeout=300s deployment/cloud-sync -n data-sync

# Deploy vm-sync
echo -e "${YELLOW}🖥️  Deploying vm-sync...${NC}"
kubectl apply -f vm-sync-deployment.yaml

# Wait for vm-sync to be ready
echo -e "${YELLOW}⏳ Waiting for vm-sync to be ready...${NC}"
kubectl wait --for=condition=available --timeout=300s deployment/vm-sync -n data-sync

echo -e "${GREEN}✅ Deployment completed successfully!${NC}"

# Display status
echo -e "${BLUE}📊 Deployment Status:${NC}"
kubectl get pods -n data-sync
echo ""
kubectl get services -n data-sync
echo ""
kubectl get ingress -n data-sync

# Display access information
echo -e "${GREEN}🌐 Access Information:${NC}"
echo -e "Cloud-Sync Dashboard: https://darshan.com/dashboard"
echo -e "Cloud-Sync API: https://darshan.com/api"
echo -e "Cloud-Sync Health: https://darshan.com/health"
echo ""
echo -e "VM-Sync Health: https://xyz.com/health"
echo -e "VM-Sync TCP Receiver: xyz.com:9000"

# Display telemetry configuration info
echo -e "${YELLOW}📡 HTTP Telemetry Configuration:${NC}"
echo -e "VM-Sync sends HTTP telemetry to: https://darshan.com/api/telemetry"
echo -e "WebSocket dependency eliminated for telemetry ✅"
echo ""

# Display next steps
echo -e "${BLUE}🎯 Next Steps:${NC}"
echo -e "1. Update DNS records:"
echo -e "   darshan.com → Cloud-Sync LoadBalancer IP"
echo -e "   xyz.com → VM-Sync LoadBalancer IP"
echo -e ""
echo -e "2. Configure TLS certificates (if using cert-manager)"
echo -e ""
echo -e "3. Register OAuth2 client for VM-Sync telemetry:"
echo -e "   Check the output from create-k8s-secrets.sh for the curl command"
echo -e ""
echo -e "4. Monitor deployment:"
echo -e "   kubectl logs -f deployment/cloud-sync -n data-sync"
echo -e "   kubectl logs -f deployment/vm-sync -n data-sync"

echo -e "${GREEN}🎉 Domain-based deployment completed!${NC}"