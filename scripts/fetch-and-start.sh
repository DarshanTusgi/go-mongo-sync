#!/bin/bash
set -e  # Exit on any error

echo "🚀 STARTUP WRAPPER: Starting cloud-sync with tenant fetch..."
echo "   TENANT_DNS: ${TENANT_DNS}"
echo "   TENANT_ID: ${TENANT_ID}"
echo "   TENANT_NAME: ${TENANT_NAME}"
echo "   COMMUNITY_ID: ${COMMUNITY_ID}"
echo "   COMMUNITY_NAME: ${COMMUNITY_NAME}"

# Check if we need to fetch tenant info
if [ -z "$TENANT_ID" ] || [ -z "$COMMUNITY_ID" ]; then
    echo "🔍 TENANT INFO: Fetching from API..."
    
    # Validate required vars
    if [ -z "$TENANT_DNS" ]; then
        echo "❌ FATAL: TENANT_DNS must be set"
        exit 1
    fi
    if [ -z "$COMMUNITY_NAME" ]; then
        echo "❌ FATAL: COMMUNITY_NAME must be set"
        exit 1
    fi
    
    # STEP 1: Fetch tenant/community info - CORRECT REQUEST FORMAT
    echo "📡 HTTP CALL 1/3: Fetching tenant info..."
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "https://${TENANT_DNS}/api/r1/system/community_info/fetch" \
        -H "Content-Type: application/json" \
        -d "{\"dns\":\"${TENANT_DNS}\",\"communityName\":\"${COMMUNITY_NAME}\"}" \
        --max-time 30)
    
    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    RESPONSE_BODY=$(echo "$RESPONSE" | sed '$d')
    
    if [ "$HTTP_CODE" != "200" ]; then
        echo "❌ FATAL: Failed to fetch tenant info (HTTP $HTTP_CODE)"
        echo "Response: $RESPONSE_BODY"
        exit 1
    fi
    
    echo "Response received: $RESPONSE_BODY"
    
    # Parse JSON - use Python if available (most reliable), otherwise jq, otherwise grep
    if command -v python3 &> /dev/null; then
        export TENANT_ID=$(echo "$RESPONSE_BODY" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('tenant',{}).get('id',''))")
        export TENANT_NAME_FETCHED=$(echo "$RESPONSE_BODY" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('tenant',{}).get('name',''))")
        export COMMUNITY_ID=$(echo "$RESPONSE_BODY" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('community',{}).get('id',''))")
    elif command -v jq &> /dev/null; then
        export TENANT_ID=$(echo "$RESPONSE_BODY" | jq -r '.tenant.id // empty')
        export TENANT_NAME_FETCHED=$(echo "$RESPONSE_BODY" | jq -r '.tenant.name // empty')
        export COMMUNITY_ID=$(echo "$RESPONSE_BODY" | jq -r '.community.id // empty')
    else
        # Fallback to grep/sed
        export TENANT_ID=$(echo "$RESPONSE_BODY" | grep -o '"tenant"[[:space:]]*:[[:space:]]*{[^}]*"id"[[:space:]]*:[[:space:]]*"[^"]*"' | grep -o '"id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)".*/\1/')
        export TENANT_NAME_FETCHED=$(echo "$RESPONSE_BODY" | grep -o '"tenant"[[:space:]]*:[[:space:]]*{[^}]*"name"[[:space:]]*:[[:space:]]*"[^"]*"' | grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)".*/\1/')
        export COMMUNITY_ID=$(echo "$RESPONSE_BODY" | grep -o '"community"[[:space:]]*:[[:space:]]*{[^}]*"id"[[:space:]]*:[[:space:]]*"[^"]*"' | grep -o '"id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)".*/\1/')
    fi
    
    # Set TENANT_NAME if not already set
    if [ -z "$TENANT_NAME" ]; then
        export TENANT_NAME="$TENANT_NAME_FETCHED"
    fi
    
    echo "✅ HTTP CALL 1/3 SUCCESS: TENANT_ID=$TENANT_ID, COMMUNITY_ID=$COMMUNITY_ID"
    
    # STEP 2: Fetch service discovery
    echo "📡 HTTP CALL 2/3: Fetching service discovery..."
    SD_RESPONSE=$(curl -s -w "\n%{http_code}" "https://${TENANT_DNS}/caas/sd" --max-time 30)
    
    SD_HTTP_CODE=$(echo "$SD_RESPONSE" | tail -1)
    SD_BODY=$(echo "$SD_RESPONSE" | sed '$d')
    
    if [ "$SD_HTTP_CODE" != "200" ]; then
        echo "❌ FATAL: Failed to fetch service discovery (HTTP $SD_HTTP_CODE)"
        exit 1
    fi
    
    if command -v python3 &> /dev/null; then
        GLOBAL_CAAS=$(echo "$SD_BODY" | python3 -c "import sys, json; print(json.load(sys.stdin).get('global_caas',''))")
    elif command -v jq &> /dev/null; then
        GLOBAL_CAAS=$(echo "$SD_BODY" | jq -r '.global_caas // empty')
    else
        GLOBAL_CAAS=$(echo "$SD_BODY" | grep -o '"global_caas"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)".*/\1/')
    fi
    
    echo "✅ HTTP CALL 2/3 SUCCESS: global_caas=$GLOBAL_CAAS"
    
    # STEP 3: Extract domain and fetch root tenant
    GLOBAL_CAAS_DOMAIN=$(echo "$GLOBAL_CAAS" | sed -e 's|https\?://||' -e 's|/.*||')
    
    echo "📡 HTTP CALL 3/3: Fetching root tenant from $GLOBAL_CAAS_DOMAIN..."
    ROOT_RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "https://${GLOBAL_CAAS_DOMAIN}/api/r1/system/community_info/fetch" \
        -H "Content-Type: application/json" \
        -d "{\"dns\":\"${GLOBAL_CAAS_DOMAIN}\",\"communityName\":\"default\"}" \
        --max-time 30)
    
    ROOT_HTTP_CODE=$(echo "$ROOT_RESPONSE" | tail -1)
    ROOT_BODY=$(echo "$ROOT_RESPONSE" | sed '$d')
    
    if [ "$ROOT_HTTP_CODE" != "200" ]; then
        echo "❌ FATAL: Failed to fetch root tenant info (HTTP $ROOT_HTTP_CODE)"
        echo "Response: $ROOT_BODY"
        exit 1
    fi
    
    if command -v python3 &> /dev/null; then
        export ROOT_TENANT_NAME=$(echo "$ROOT_BODY" | python3 -c "import sys, json; print(json.load(sys.stdin).get('tenant',{}).get('name',''))")
    elif command -v jq &> /dev/null; then
        export ROOT_TENANT_NAME=$(echo "$ROOT_BODY" | jq -r '.tenant.name // empty')
    else
        export ROOT_TENANT_NAME=$(echo "$ROOT_BODY" | grep -o '"tenant"[[:space:]]*:[[:space:]]*{[^}]*"name"[[:space:]]*:[[:space:]]*"[^"]*"' | grep -o '"name"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"\([^"]*\)".*/\1/')
    fi
    
    echo "✅ HTTP CALL 3/3 SUCCESS: ROOT_TENANT_NAME=$ROOT_TENANT_NAME"
    
    # Verify all required vars are set
    if [ -z "$TENANT_ID" ] || [ -z "$COMMUNITY_ID" ] || [ -z "$ROOT_TENANT_NAME" ]; then
        echo "❌ FATAL: Failed to fetch required tenant info"
        echo "   TENANT_ID: $TENANT_ID"
        echo "   COMMUNITY_ID: $COMMUNITY_ID"
        echo "   ROOT_TENANT_NAME: $ROOT_TENANT_NAME"
        exit 1
    fi
    
    echo "✅ TENANT INFO: All environment variables set successfully"
    echo "   TENANT_ID: $TENANT_ID"
    echo "   TENANT_NAME: $TENANT_NAME"
    echo "   COMMUNITY_ID: $COMMUNITY_ID"
    echo "   COMMUNITY_NAME: $COMMUNITY_NAME"
    echo "   ROOT_TENANT_NAME: $ROOT_TENANT_NAME"
else
    echo "✅ TENANT INFO: Using provided environment variables (no fetch needed)"
fi

# Give a moment for any background processes to settle
sleep 1

# Execute cloud-sync with all arguments passed to this script
# Check if running in Docker (binary at ./cloud-sync) or local (binary at ./runnables/cloud-sync)
if [ -f "./cloud-sync" ]; then
    echo "🚀 STARTING CLOUD-SYNC: exec ./cloud-sync $@"
    exec ./cloud-sync "$@"
elif [ -f "./runnables/cloud-sync" ]; then
    echo "🚀 STARTING CLOUD-SYNC: exec ./runnables/cloud-sync $@"
    exec ./runnables/cloud-sync "$@"
else
    echo "❌ FATAL: cloud-sync binary not found"
    exit 1
fi
