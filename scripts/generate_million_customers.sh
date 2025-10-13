#!/bin/bash

# Script to generate 1 million customer records
# This will insert 1M customers into the real_transfer_test.customers collection

MONGO_URI="mongodb+srv://admin:IdZcKnNvmWqea13k@proptuity-dev.mgzig.mongodb.net/real_transfer_test"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JS_SCRIPT="$SCRIPT_DIR/generate_million_customers.js"

echo "🚀 Generating 1 Million Customer Records..."
echo "📍 Database: real_transfer_test"
echo "📦 Collection: customers"
echo ""

# Check if mongosh is installed
if ! command -v mongosh &> /dev/null; then
    echo "❌ Error: mongosh is not installed"
    echo "Please install MongoDB Shell: https://www.mongodb.com/try/download/shell"
    exit 1
fi

# Run the generation script
mongosh "$MONGO_URI" --file "$JS_SCRIPT"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Successfully generated 1 million customer records!"
    echo "🎯 You can now test the initial sync performance"
else
    echo ""
    echo "❌ Failed to generate customer records"
    exit 1
fi
