#!/bin/bash

echo "==============================================="
echo "Distributed Data Processing Platform"
echo "Local Verification Script"
echo "==============================================="
echo ""

echo "[1/5] Checking Docker services status..."
docker-compose ps
echo ""

echo "[2/5] Checking Kafka topics..."
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:29092 2>/dev/null
echo ""

echo "[3/5] Checking MongoDB collections..."
docker exec -it mongodb mongosh -u admin -p password123 --quiet --eval "
use analytics;
print('Collections in analytics database:');
db.getCollectionNames().forEach(function(coll) {
    var count = db[coll].countDocuments();
    print('  - ' + coll + ': ' + count + ' documents');
});
" 2>/dev/null
echo ""

echo "[4/5] Sample data from event_counts..."
docker exec -it mongodb mongosh -u admin -p password123 --quiet --eval "
use analytics;
db.event_counts.find().limit(3).forEach(printjson);
" 2>/dev/null
echo ""

echo "[5/5] Sample data from purchase_analytics..."
docker exec -it mongodb mongosh -u admin -p password123 --quiet --eval "
use analytics;
db.purchase_analytics.find().limit(2).forEach(printjson);
" 2>/dev/null
echo ""

echo "==============================================="
echo "Platform Status Summary"
echo "==============================================="
RUNNING=$(docker-compose ps --filter "status=running" | grep -c "Up")
echo "Services running: $RUNNING/8"
echo ""
echo "Access Spark UI: http://localhost:8080"
echo ""
echo "Note: AWS S3 integration is disabled for local testing"
echo "==============================================="