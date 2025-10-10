#!/bin/bash

# Check sync results after initial sync
echo "🔍 CHECKING SYNC RESULTS"
echo "======================="

collections=("products" "customers" "orders" "inventory")

echo "📊 Document Counts:"
echo "=================="

total_docs=0
total_indexes=0

for collection in "${collections[@]}"; do
    echo ""
    echo "📋 Collection: $collection"
    echo "------------------------"
    
    if command -v mongosh &> /dev/null; then
        # Count documents
        count=$(mongosh --quiet --eval "use real_transfer_test; db.${collection}.countDocuments({})")
        
        # Count indexes
        index_count=$(mongosh --quiet --eval "use real_transfer_test; db.${collection}.getIndexes().length")
        
        # Show sample document
        echo "  📊 Documents: $count"
        echo "  🔍 Indexes: $index_count"
        
        if [ "$count" -gt 0 ]; then
            echo "  📄 Sample document:"
            mongosh --quiet --eval "use real_transfer_test; db.${collection}.findOne({}, {_id: 1, status: 1, price: 1, name: 1, product_id: 1, customer_id: 1, order_id: 1})" | head -1
            
            echo "  🏷️  Index names:"
            mongosh --quiet --eval "use real_transfer_test; db.${collection}.getIndexes().forEach(idx => print('    - ' + idx.name))"
        fi
        
    elif command -v mongo &> /dev/null; then
        # Count documents
        count=$(mongo --quiet --eval "use real_transfer_test; db.${collection}.countDocuments({})")
        
        # Count indexes
        index_count=$(mongo --quiet --eval "use real_transfer_test; db.${collection}.getIndexes().length")
        
        echo "  📊 Documents: $count"
        echo "  🔍 Indexes: $index_count"
        
        if [ "$count" -gt 0 ]; then
            echo "  📄 Sample document:"
            mongo --quiet --eval "use real_transfer_test; printjson(db.${collection}.findOne({}, {_id: 1, status: 1, price: 1, name: 1, product_id: 1, customer_id: 1, order_id: 1}))"
            
            echo "  🏷️  Index names:"
            mongo --quiet --eval "use real_transfer_test; db.${collection}.getIndexes().forEach(function(idx) { print('    - ' + idx.name); })"
        fi
    else
        echo "  ❌ MongoDB client not found"
        continue
    fi
    
    total_docs=$((total_docs + count))
    total_indexes=$((total_indexes + index_count))
done

echo ""
echo "📊 SUMMARY"
echo "=========="
echo "  📋 Total Documents: $total_docs"
echo "  🔍 Total Indexes: $total_indexes"
echo ""

if [ "$total_docs" -gt 0 ]; then
    echo "✅ SUCCESS: Data was transferred!"
    
    if [ "$total_indexes" -gt 4 ]; then  # Each collection should have at least _id index
        echo "✅ SUCCESS: Indexes were created!"
    else
        echo "⚠️  WARNING: Index count seems low ($total_indexes)"
    fi
    
    echo ""
    echo "🔍 FILTER VERIFICATION:"
    echo "======================"
    echo "Checking if filters were applied correctly..."
    
    # Check products filter (status=active, price>0)
    if command -v mongosh &> /dev/null; then
        echo "📋 Products filter check:"
        mongosh --quiet --eval "
            use real_transfer_test; 
            var activeCount = db.products.countDocuments({status: 'active'});
            var pricedCount = db.products.countDocuments({price: {\$gt: 0}});
            var totalCount = db.products.countDocuments({});
            print('  - Total products: ' + totalCount);
            print('  - Active products: ' + activeCount);
            print('  - Products with price > 0: ' + pricedCount);
            if (activeCount === totalCount && pricedCount === totalCount) {
                print('  ✅ Product filters applied correctly');
            } else {
                print('  ⚠️  Product filters may not be working');
            }
        "
        
        echo "📋 Customers filter check:"
        mongosh --quiet --eval "
            use real_transfer_test;
            var verifiedCount = db.customers.countDocuments({verified: true});
            var activeCount = db.customers.countDocuments({status: 'active'});
            var totalCount = db.customers.countDocuments({});
            print('  - Total customers: ' + totalCount);
            print('  - Verified customers: ' + verifiedCount);
            print('  - Active customers: ' + activeCount);
            if (verifiedCount === totalCount && activeCount === totalCount) {
                print('  ✅ Customer filters applied correctly');
            } else {
                print('  ⚠️  Customer filters may not be working');
            }
        "
    fi
    
else
    echo "❌ FAILURE: No data was transferred"
    echo ""
    echo "🔍 TROUBLESHOOTING:"
    echo "=================="
    echo "1. Check if both services are running:"
    echo "   curl http://localhost:8080/health"
    echo "   curl http://localhost:8081/health"
    echo ""
    echo "2. Check logs for errors:"
    echo "   tail -f cloud-sync.log"
    echo "   tail -f vm-sync.log"
    echo ""
    echo "3. Verify WebSocket connection in logs"
    echo ""
    echo "4. Try triggering sync again:"
    echo "   curl -X POST http://localhost:8080/api/sync/initial -H \"Content-Type: application/json\" -d '{\"force\": true}'"
fi

echo ""
echo "🎯 Test completed!"