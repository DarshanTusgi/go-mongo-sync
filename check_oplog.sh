#!/bin/bash
# Check MongoDB oplog status and retention

MONGO_URI="mongodb+srv://admin:IdZcKnNvmWqea13k@proptuity-dev.mgzig.mongodb.net"

echo "🔍 Checking MongoDB Oplog Configuration"
echo "=========================================="
echo ""

# Check oplog size and retention
mongosh "$MONGO_URI/local" --quiet --eval '
var stats = db.oplog.rs.stats();
var first = db.oplog.rs.find().sort({$natural: 1}).limit(1).toArray()[0];
var last = db.oplog.rs.find().sort({$natural: -1}).limit(1).toArray()[0];

if (first && last) {
    var firstTime = first.ts.getTime();
    var lastTime = last.ts.getTime();
    var retentionHours = (lastTime - firstTime) / (1000 * 3600);
    
    print("📊 OPLOG STATISTICS:");
    print("-------------------");
    print("Size: " + (stats.size / (1024*1024*1024)).toFixed(2) + " GB");
    print("Max Size: " + (stats.maxSize / (1024*1024*1024)).toFixed(2) + " GB");
    print("Document Count: " + stats.count.toLocaleString());
    print("");
    print("⏰ TIME WINDOW:");
    print("-------------------");
    print("Oldest Entry: " + new Date(firstTime));
    print("Newest Entry: " + new Date(lastTime));
    print("Retention: " + retentionHours.toFixed(2) + " hours");
    print("");
    
    if (retentionHours < 1) {
        print("❌ WARNING: Oplog retention < 1 hour (very risky!)");
    } else if (retentionHours < 6) {
        print("⚠️  CAUTION: Oplog retention < 6 hours (may cause issues)");
    } else {
        print("✅ GOOD: Oplog retention > 6 hours");
    }
    
    print("");
    print("💡 RECOMMENDATION:");
    if (retentionHours < 6) {
        print("   - Reduce scheduler_interval (currently 30s)");
        print("   - Upgrade to larger Atlas tier for bigger oplog");
        print("   - Reduce write load on database");
    }
} else {
    print("❌ ERROR: Cannot access oplog");
    print("This might be a permission issue!");
}
' 2>&1

echo ""
echo "🔍 Checking User Permissions"
echo "=========================================="
mongosh "$MONGO_URI/admin" --quiet --eval '
var user = db.runCommand({connectionStatus: 1});
print("Current User: " + JSON.stringify(user.authInfo.authenticatedUsers, null, 2));
print("");
print("Roles:");
user.authInfo.authenticatedUserRoles.forEach(function(role) {
    print("  - " + role.role + " on " + role.db);
});
' 2>&1

echo ""
echo "✅ Diagnostic complete!"
