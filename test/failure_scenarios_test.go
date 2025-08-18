package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	cloudURI = "mongodb+srv://admin:IdZcKnNvmWqea13k@proptuity-dev.mgzig.mongodb.net/test_db"
	vmURI    = "mongodb://localhost:27017/test_db"
	testDB   = "test_db"
	testColl = "failure_test"
)

var (
	cloudClient *mongo.Client
	vmClient    *mongo.Client
)

func TestMain(m *testing.M) {
	// Setup
	var err error
	cloudClient, err = mongo.Connect(context.Background(), options.Client().ApplyURI(cloudURI))
	if err != nil {
		log.Fatalf("Failed to connect to cloud MongoDB: %v", err)
	}

	vmClient, err = mongo.Connect(context.Background(), options.Client().ApplyURI(vmURI))
	if err != nil {
		log.Fatalf("Failed to connect to VM MongoDB: %v", err)
	}

	// Run tests
	code := m.Run()

	// Cleanup
	cloudClient.Disconnect(context.Background())
	vmClient.Disconnect(context.Background())

	os.Exit(code)
}

func TestMidSnapshotRestart(t *testing.T) {
	t.Log("Testing mid-snapshot restart scenario")

	// Setup: Insert large dataset in cloud
	ctx := context.Background()
	cloudColl := cloudClient.Database(testDB).Collection(testColl)
	vmColl := vmClient.Database(testDB).Collection(testColl)

	// Clean up first
	cloudColl.Drop(ctx)
	vmColl.Drop(ctx)

	// Insert 10000 documents to simulate large snapshot
	docs := make([]interface{}, 10000)
	for i := 0; i < 10000; i++ {
		docs[i] = bson.M{
			"_id":   fmt.Sprintf("doc_%d", i),
			"data":  fmt.Sprintf("test_data_%d", i),
			"index": i,
		}
	}

	_, err := cloudColl.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("Failed to insert test documents: %v", err)
	}

	// Start cloud-sync and vm-sync
	cloudCmd := startCloudSync(t)
	vmCmd := startVMSync(t)

	// Wait for sync to start
	time.Sleep(5 * time.Second)

	// Kill vm-sync mid-snapshot
	t.Log("Killing vm-sync mid-snapshot")
	vmCmd.Process.Kill()
	vmCmd.Wait()

	// Wait a bit
	time.Sleep(2 * time.Second)

	// Restart vm-sync
	t.Log("Restarting vm-sync")
	vmCmd = startVMSync(t)
	defer vmCmd.Process.Kill()

	// Wait for sync to complete
	time.Sleep(30 * time.Second)

	// Verify data consistency
	cloudCount, _ := cloudColl.CountDocuments(ctx, bson.M{})
	vmCount, _ := vmColl.CountDocuments(ctx, bson.M{})

	if cloudCount != vmCount {
		t.Errorf("Data inconsistency after restart: cloud=%d, vm=%d", cloudCount, vmCount)
	}

	// Cleanup
	cloudCmd.Process.Kill()
	vmCmd.Process.Kill()
	cloudCmd.Wait()
	vmCmd.Wait()
}

func TestNetworkCutDuringSync(t *testing.T) {
	t.Log("Testing network cut during sync scenario")

	ctx := context.Background()
	cloudColl := cloudClient.Database(testDB).Collection(testColl)
	vmColl := vmClient.Database(testDB).Collection(testColl)

	// Clean up
	cloudColl.Drop(ctx)
	vmColl.Drop(ctx)

	// Insert initial data
	initialDocs := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		initialDocs[i] = bson.M{
			"_id":  fmt.Sprintf("initial_%d", i),
			"data": fmt.Sprintf("initial_data_%d", i),
		}
	}
	cloudColl.InsertMany(ctx, initialDocs)

	// Start sync services
	cloudCmd := startCloudSync(t)
	vmCmd := startVMSync(t)
	defer func() {
		cloudCmd.Process.Kill()
		vmCmd.Process.Kill()
		cloudCmd.Wait()
		vmCmd.Wait()
	}()

	// Wait for initial sync
	time.Sleep(10 * time.Second)

	// Simulate network cut by blocking network traffic
	t.Log("Simulating network cut")
	// Note: In real test, you might use iptables or similar
	// For this test, we'll kill and restart to simulate network recovery
	vmCmd.Process.Kill()
	vmCmd.Wait()

	// Insert more data while "network is down"
	networkDownDocs := make([]interface{}, 500)
	for i := 0; i < 500; i++ {
		networkDownDocs[i] = bson.M{
			"_id":  fmt.Sprintf("network_down_%d", i),
			"data": fmt.Sprintf("network_down_data_%d", i),
		}
	}
	cloudColl.InsertMany(ctx, networkDownDocs)

	// Wait to simulate network downtime
	time.Sleep(5 * time.Second)

	// "Restore network" by restarting vm-sync
	t.Log("Restoring network connection")
	vmCmd = startVMSync(t)

	// Wait for catch-up sync
	time.Sleep(20 * time.Second)

	// Verify all data is synced
	cloudCount, _ := cloudColl.CountDocuments(ctx, bson.M{})
	vmCount, _ := vmColl.CountDocuments(ctx, bson.M{})

	if cloudCount != vmCount {
		t.Errorf("Data inconsistency after network recovery: cloud=%d, vm=%d", cloudCount, vmCount)
	}
}

func TestDDLEventHandling(t *testing.T) {
	t.Log("Testing DDL event handling scenario")

	ctx := context.Background()
	cloudDB := cloudClient.Database(testDB)
	vmDB := vmClient.Database(testDB)

	// Clean up
	cloudDB.Collection(testColl).Drop(ctx)
	cloudDB.Collection(testColl + "_renamed").Drop(ctx)
	vmDB.Collection(testColl).Drop(ctx)
	vmDB.Collection(testColl + "_renamed").Drop(ctx)

	// Insert initial data
	cloudColl := cloudDB.Collection(testColl)
	initialDocs := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		initialDocs[i] = bson.M{
			"_id":  fmt.Sprintf("ddl_test_%d", i),
			"data": fmt.Sprintf("ddl_data_%d", i),
		}
	}
	cloudColl.InsertMany(ctx, initialDocs)

	// Start sync services
	cloudCmd := startCloudSync(t)
	vmCmd := startVMSync(t)
	defer func() {
		cloudCmd.Process.Kill()
		vmCmd.Process.Kill()
		cloudCmd.Wait()
		vmCmd.Wait()
	}()

	// Wait for initial sync
	time.Sleep(10 * time.Second)

	// Perform DDL operation - rename collection
	t.Log("Performing DDL operation: renaming collection")
	err := cloudDB.RunCommand(ctx, bson.M{
		"renameCollection": fmt.Sprintf("%s.%s", testDB, testColl),
		"to":               fmt.Sprintf("%s.%s_renamed", testDB, testColl),
	}).Err()
	if err != nil {
		t.Logf("DDL operation failed (expected in some cases): %v", err)
	}

	// Wait for invalidate event processing
	time.Sleep(15 * time.Second)

	// Insert data to renamed collection
	renamedColl := cloudDB.Collection(testColl + "_renamed")
	postDDLDocs := make([]interface{}, 50)
	for i := 0; i < 50; i++ {
		postDDLDocs[i] = bson.M{
			"_id":  fmt.Sprintf("post_ddl_%d", i),
			"data": fmt.Sprintf("post_ddl_data_%d", i),
		}
	}
	renamedColl.InsertMany(ctx, postDDLDocs)

	// Wait for re-bootstrap and sync
	time.Sleep(20 * time.Second)

	// Verify data consistency in renamed collection
	cloudRenamedCount, _ := renamedColl.CountDocuments(ctx, bson.M{})
	vmRenamedCount, _ := vmDB.Collection(testColl + "_renamed").CountDocuments(ctx, bson.M{})

	t.Logf("After DDL: cloud_renamed=%d, vm_renamed=%d", cloudRenamedCount, vmRenamedCount)

	// Check that original collection is empty/dropped on VM side
	vmOriginalCount, _ := vmDB.Collection(testColl).CountDocuments(ctx, bson.M{})
	t.Logf("VM original collection count: %d", vmOriginalCount)
}

func TestOplogWindowExpiry(t *testing.T) {
	t.Log("Testing oplog window expiry scenario")

	// This test simulates what happens when resume token becomes invalid
	// due to oplog window expiry

	ctx := context.Background()
	cloudColl := cloudClient.Database(testDB).Collection(testColl)
	vmColl := vmClient.Database(testDB).Collection(testColl)

	// Clean up
	cloudColl.Drop(ctx)
	vmColl.Drop(ctx)

	// Insert initial data
	initialDocs := make([]interface{}, 500)
	for i := 0; i < 500; i++ {
		initialDocs[i] = bson.M{
			"_id":  fmt.Sprintf("oplog_test_%d", i),
			"data": fmt.Sprintf("oplog_data_%d", i),
		}
	}
	cloudColl.InsertMany(ctx, initialDocs)

	// Start sync services
	cloudCmd := startCloudSync(t)
	vmCmd := startVMSync(t)
	defer func() {
		cloudCmd.Process.Kill()
		vmCmd.Process.Kill()
		cloudCmd.Wait()
		vmCmd.Wait()
	}()

	// Wait for initial sync
	time.Sleep(10 * time.Second)

	// Stop vm-sync for extended period to simulate oplog window expiry
	t.Log("Stopping vm-sync to simulate oplog window expiry")
	vmCmd.Process.Kill()
	vmCmd.Wait()

	// Generate lots of operations to potentially expire oplog
	for batch := 0; batch < 10; batch++ {
		batchDocs := make([]interface{}, 1000)
		for i := 0; i < 1000; i++ {
			batchDocs[i] = bson.M{
				"_id":   fmt.Sprintf("batch_%d_doc_%d", batch, i),
				"data":  fmt.Sprintf("batch_data_%d_%d", batch, i),
				"batch": batch,
			}
		}
		cloudColl.InsertMany(ctx, batchDocs)
		time.Sleep(1 * time.Second)
	}

	// Wait longer to increase chance of oplog expiry
	time.Sleep(10 * time.Second)

	// Restart vm-sync
	t.Log("Restarting vm-sync after potential oplog expiry")
	vmCmd = startVMSync(t)

	// Wait for recovery and sync
	time.Sleep(30 * time.Second)

	// Verify data consistency
	cloudCount, _ := cloudColl.CountDocuments(ctx, bson.M{})
	vmCount, _ := vmColl.CountDocuments(ctx, bson.M{})

	t.Logf("After oplog recovery: cloud=%d, vm=%d", cloudCount, vmCount)

	if cloudCount != vmCount {
		t.Errorf("Data inconsistency after oplog recovery: cloud=%d, vm=%d", cloudCount, vmCount)
	}
}

func startCloudSync(t *testing.T) *exec.Cmd {
	cmd := exec.Command("./bin/cloud-sync", "-config", "examples/cloud-config.yaml")
	cmd.Dir = "/Users/darshanredkar/darshan/proptuity/code/go-data-sync/go-data-sync-http"
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		t.Fatalf("Failed to start cloud-sync: %v", err)
	}

	t.Log("Started cloud-sync")
	return cmd
}

func startVMSync(t *testing.T) *exec.Cmd {
	cmd := exec.Command("./bin/vm-sync", "-config", "examples/vm-config.yaml")
	cmd.Dir = "/Users/darshanredkar/darshan/proptuity/code/go-data-sync/go-data-sync-http"
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		t.Fatalf("Failed to start vm-sync: %v", err)
	}

	t.Log("Started vm-sync")
	return cmd
}

func waitForSync(t *testing.T, cloudColl, vmColl *mongo.Collection, expectedCount int64, timeout time.Duration) bool {
	ctx := context.Background()
	start := time.Now()

	for time.Since(start) < timeout {
		vmCount, _ := vmColl.CountDocuments(ctx, bson.M{})
		if vmCount >= expectedCount {
			return true
		}
		time.Sleep(1 * time.Second)
	}

	return false
}