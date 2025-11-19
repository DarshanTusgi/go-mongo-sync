#!/bin/bash

# TCP Diagnosis Script for K8s/K3s Deployments
# Usage: ./diagnose-tcp-k8s.sh [cloud-pod-name] [vm-pod-name] [vm-service-name]

set -e

CLOUD_POD="${1:-cloud-sync}"
VM_POD="${2:-vm-sync}"
VM_SERVICE="${3:-vm-sync-tcp}"

echo "════════════════════════════════════════════════════════════"
echo "🔍 TCP TRANSPORT DIAGNOSIS - K8s/K3s Cross-Cluster"
echo "════════════════════════════════════════════════════════════"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
success() { echo -e "${GREEN}✅ $1${NC}"; }
error() { echo -e "${RED}❌ $1${NC}"; }
warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
info() { echo -e "${BLUE}ℹ️  $1${NC}"; }

echo "Target Pods:"
info "  Cloud-sync: $CLOUD_POD"
info "  VM-sync: $VM_POD"
info "  VM Service: $VM_SERVICE"
echo ""

# ═══════════════════════════════════════════════════════════
# 1. VM-SYNC RECEIVER CHECK
# ═══════════════════════════════════════════════════════════
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1️⃣  VM-SYNC TCP RECEIVER STATUS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check if port 9000 is listening
echo ""
info "Checking if port 9000 is listening in vm-sync pod..."
if kubectl exec -it "$VM_POD" -- netstat -tuln 2>/dev/null | grep ':9000' > /dev/null 2>&1; then
    success "Port 9000 is LISTENING"
    kubectl exec -it "$VM_POD" -- netstat -tuln | grep ':9000'
else
    error "Port 9000 is NOT listening!"
    warning "VM-sync TCP receiver may not have started"
fi

# Check vm-sync logs for receiver startup
echo ""
info "Checking vm-sync logs for TCP receiver startup..."
if kubectl logs "$VM_POD" --tail=100 | grep "TCP RECEIVER LISTENING" > /dev/null 2>&1; then
    success "TCP receiver started successfully"
    kubectl logs "$VM_POD" --tail=100 | grep "TCP RECEIVER"
else
    error "TCP receiver startup not confirmed in logs"
    warning "Check logs: kubectl logs $VM_POD | grep TCP"
fi

# Check batch handler registration
echo ""
info "Checking batch handler registration..."
if kubectl logs "$VM_POD" --tail=100 | grep "TCP BATCH HANDLER: Registered" > /dev/null 2>&1; then
    success "Batch handler registered"
else
    error "Batch handler not registered!"
fi

# ═══════════════════════════════════════════════════════════
# 2. KUBERNETES SERVICE CHECK
# ═══════════════════════════════════════════════════════════
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "2️⃣  KUBERNETES SERVICE CONFIGURATION"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
info "Checking if $VM_SERVICE service exists..."
if kubectl get svc "$VM_SERVICE" > /dev/null 2>&1; then
    success "Service $VM_SERVICE exists"
    kubectl get svc "$VM_SERVICE" -o wide
    
    # Check if service has endpoints
    echo ""
    info "Checking service endpoints..."
    ENDPOINTS=$(kubectl get endpoints "$VM_SERVICE" -o jsonpath='{.subsets[*].addresses[*].ip}')
    if [ -n "$ENDPOINTS" ]; then
        success "Service has endpoints: $ENDPOINTS"
    else
        error "Service has NO endpoints!"
        warning "Pod selector may not match"
    fi
else
    error "Service $VM_SERVICE does NOT exist!"
    warning "You need to create a Service for vm-sync:"
    echo ""
    echo "---"
    echo "apiVersion: v1"
    echo "kind: Service"
    echo "metadata:"
    echo "  name: $VM_SERVICE"
    echo "spec:"
    echo "  selector:"
    echo "    app: vm-sync  # Match your pod label"
    echo "  ports:"
    echo "  - name: tcp-transport"
    echo "    port: 9000"
    echo "    targetPort: 9000"
    echo "    protocol: TCP"
    echo "  type: ClusterIP"
    echo "---"
fi

# ═══════════════════════════════════════════════════════════
# 3. NETWORK CONNECTIVITY TEST
# ═══════════════════════════════════════════════════════════
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "3️⃣  NETWORK CONNECTIVITY (Cloud → VM)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
info "Testing TCP connection from cloud-sync to vm-sync..."
if kubectl exec -it "$CLOUD_POD" -- nc -zv "$VM_SERVICE" 9000 2>&1 | grep -i "succeeded\|open" > /dev/null; then
    success "Cloud-sync CAN reach vm-sync:9000"
else
    error "Cloud-sync CANNOT reach vm-sync:9000"
    warning "Possible causes:"
    echo "  - Service not created"
    echo "  - Network policy blocking"
    echo "  - Cross-cluster networking issue (K8s ↔ K3s)"
    echo "  - Firewall rules"
fi

# Test DNS resolution
echo ""
info "Testing DNS resolution..."
if kubectl exec -it "$CLOUD_POD" -- nslookup "$VM_SERVICE" > /dev/null 2>&1; then
    success "DNS resolves $VM_SERVICE"
    kubectl exec -it "$CLOUD_POD" -- nslookup "$VM_SERVICE" 2>&1 | tail -5
else
    error "DNS cannot resolve $VM_SERVICE"
fi

# ═══════════════════════════════════════════════════════════
# 4. CLOUD-SYNC SENDER CHECK
# ═══════════════════════════════════════════════════════════
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "4️⃣  CLOUD-SYNC TCP SENDER STATUS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
info "Checking cloud-sync TCP initialization..."
if kubectl logs "$CLOUD_POD" --tail=200 | grep "TCP TRANSPORT OPTIMIZED" > /dev/null 2>&1; then
    success "TCP sender initialized"
    kubectl logs "$CLOUD_POD" --tail=200 | grep "TCP TRANSPORT OPTIMIZED"
else
    error "TCP sender not initialized"
fi

# Check for TCP send attempts
echo ""
info "Checking for TCP send attempts..."
if kubectl logs "$CLOUD_POD" --tail=200 | grep "TCP SENDING" > /dev/null 2>&1; then
    success "Cloud-sync is attempting TCP sends"
    kubectl logs "$CLOUD_POD" --tail=200 | grep "TCP SENDING" | tail -5
else
    warning "No TCP send attempts found in logs"
fi

# Check for ACK received
echo ""
info "Checking for ACK confirmations..."
if kubectl logs "$CLOUD_POD" --tail=200 | grep "ACK RECEIVED" > /dev/null 2>&1; then
    success "Cloud-sync is receiving ACKs from vm-sync"
    kubectl logs "$CLOUD_POD" --tail=200 | grep "ACK RECEIVED" | tail -5
else
    error "No ACKs received - vm-sync not acknowledging batches"
fi

# ═══════════════════════════════════════════════════════════
# 5. VM-SYNC BATCH RECEPTION CHECK
# ═══════════════════════════════════════════════════════════
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "5️⃣  VM-SYNC BATCH RECEPTION STATUS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
info "Checking if vm-sync is receiving TCP connections..."
if kubectl logs "$VM_POD" --tail=200 | grep "TCP CONNECTION ACCEPTED" > /dev/null 2>&1; then
    success "VM-sync is accepting TCP connections"
    kubectl logs "$VM_POD" --tail=200 | grep "TCP CONNECTION ACCEPTED" | tail -3
else
    error "VM-sync has NOT accepted any TCP connections"
fi

echo ""
info "Checking if vm-sync is receiving TCP frames..."
if kubectl logs "$VM_POD" --tail=200 | grep "TCP FRAME RECEIVED" > /dev/null 2>&1; then
    success "VM-sync is receiving TCP frames"
    kubectl logs "$VM_POD" --tail=200 | grep "TCP FRAME RECEIVED" | tail -5
else
    error "VM-sync is NOT receiving TCP frames"
fi

echo ""
info "Checking if batch handler is being called..."
if kubectl logs "$VM_POD" --tail=200 | grep "TCP BATCH HANDLER CALLED" > /dev/null 2>&1; then
    success "Batch handler is being invoked"
    kubectl logs "$VM_POD" --tail=200 | grep "TCP BATCH HANDLER CALLED" | tail -5
else
    error "Batch handler is NOT being called"
fi

echo ""
info "Checking for batch processing success..."
if kubectl logs "$VM_POD" --tail=200 | grep "TCP BATCH SUCCESS" > /dev/null 2>&1; then
    success "Batches are being processed successfully"
    kubectl logs "$VM_POD" --tail=200 | grep "TCP BATCH SUCCESS" | tail -5
else
    error "No successful batch processing found"
fi

# Check for batch handler errors
echo ""
info "Checking for batch handler errors..."
if kubectl logs "$VM_POD" --tail=200 | grep "BATCH HANDLER FAILED" > /dev/null 2>&1; then
    error "Batch handler is failing!"
    kubectl logs "$VM_POD" --tail=200 | grep "BATCH HANDLER FAILED" | tail -5
else
    success "No batch handler failures detected"
fi

# ═══════════════════════════════════════════════════════════
# 6. SUMMARY & RECOMMENDATIONS
# ═══════════════════════════════════════════════════════════
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "6️⃣  DIAGNOSIS SUMMARY"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo ""
echo "📋 RECOMMENDED NEXT STEPS:"
echo ""

# Check most common issues
ISSUES=()

# Issue 1: Port not listening
if ! kubectl exec -it "$VM_POD" -- netstat -tuln 2>/dev/null | grep ':9000' > /dev/null 2>&1; then
    ISSUES+=("1. VM-sync port 9000 is not listening - check if TCP receiver started")
fi

# Issue 2: Service missing
if ! kubectl get svc "$VM_SERVICE" > /dev/null 2>&1; then
    ISSUES+=("2. Kubernetes Service '$VM_SERVICE' does not exist - create it")
fi

# Issue 3: No connectivity
if ! kubectl exec -it "$CLOUD_POD" -- nc -zv "$VM_SERVICE" 9000 2>&1 | grep -i "succeeded\|open" > /dev/null; then
    ISSUES+=("3. Network connectivity issue - check firewall/network policies")
fi

# Issue 4: No TCP connections
if ! kubectl logs "$VM_POD" --tail=200 | grep "TCP CONNECTION ACCEPTED" > /dev/null 2>&1; then
    ISSUES+=("4. No TCP connections accepted - check if cloud-sync is connecting to correct address")
fi

# Issue 5: No frames received
if ! kubectl logs "$VM_POD" --tail=200 | grep "TCP FRAME RECEIVED" > /dev/null 2>&1; then
    ISSUES+=("5. No TCP frames received - check if data is being sent")
fi

if [ ${#ISSUES[@]} -eq 0 ]; then
    success "No issues detected! TCP transport appears to be working correctly."
else
    error "Found ${#ISSUES[@]} issue(s):"
    echo ""
    for issue in "${ISSUES[@]}"; do
        warning "$issue"
    done
fi

echo ""
echo "════════════════════════════════════════════════════════════"
echo "✅ DIAGNOSIS COMPLETE"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "📝 For detailed logs, run:"
echo "   kubectl logs $CLOUD_POD --tail=500 | grep TCP"
echo "   kubectl logs $VM_POD --tail=500 | grep TCP"
echo ""
