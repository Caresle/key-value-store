package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/caresle/kvstore"
)

const dataDir = "./example-data"

var keepData = flag.Bool("keep", false, "keep example-data directory after demo")

// Helper functions for formatted output
func printSection(title string) {
	fmt.Printf("\n[%s]\n", title)
}

func printSuccess(msg string) {
	fmt.Printf("✓ %s\n", msg)
}

func printInfo(msg string) {
	fmt.Printf("→ %s\n", msg)
}

func printError(msg string) {
	fmt.Printf("✗ %s\n", msg)
}

func cleanup() {
	if *keepData {
		printInfo(fmt.Sprintf("Keeping %s directory for inspection", dataDir))
		printInfo("(data will persist on next run)")
	} else {
		if err := os.RemoveAll(dataDir); err != nil {
			printError(fmt.Sprintf("Failed to remove %s: %v", dataDir, err))
		} else {
			printInfo(fmt.Sprintf("Removed %s directory", dataDir))
			printInfo("(use -keep flag to preserve data for inspection)")
		}
	}
}

func main() {
	flag.Parse()

	fmt.Println("=== KVStore Demo Application ===")
	printInfo(fmt.Sprintf("Data directory: %s", dataDir))

	// Clean up old data if it exists (start fresh)
	os.RemoveAll(dataDir)

	// Ensure cleanup happens at the end
	defer cleanup()

	// ============================================================
	// Section 1: Opening Store
	// ============================================================
	printSection("1] Opening Store")

	// Opens store - will load snapshot.dat if it exists, then replay WAL
	store, err := kvstore.Open(dataDir)
	if err != nil {
		log.Fatalf("✗ Failed to open store: %v", err)
	}
	printSuccess("Store opened successfully")
	printInfo(fmt.Sprintf("Files: %s/wal.log, %s/snapshot.dat", dataDir, dataDir))

	// ============================================================
	// Section 2: Basic Operations
	// ============================================================
	printSection("2] Basic Operations")

	// Set operations with mixed data types
	// Values are []byte - store strings, JSON, binary data, anything
	if err := store.Set("user:1", []byte(`{"name":"Alice","role":"admin"}`)); err != nil {
		log.Fatalf("✗ Set failed: %v", err)
	}
	printSuccess(`Set: user:1 = {"name":"Alice","role":"admin"}`)

	if err := store.Set("user:2", []byte(`{"name":"Bob","role":"user"}`)); err != nil {
		log.Fatalf("✗ Set failed: %v", err)
	}
	printSuccess(`Set: user:2 = {"name":"Bob","role":"user"}`)

	if err := store.Set("config:theme", []byte("dark")); err != nil {
		log.Fatalf("✗ Set failed: %v", err)
	}
	printSuccess("Set: config:theme = dark")

	if err := store.Set("config:lang", []byte("en")); err != nil {
		log.Fatalf("✗ Set failed: %v", err)
	}
	printSuccess("Set: config:lang = en")

	// Get operation
	value, exists := store.Get("user:1")
	if !exists {
		printError("Get: user:1 not found (unexpected!)")
	} else {
		printSuccess(fmt.Sprintf("Get: user:1 = %s", value))
	}

	// Demonstrate Get on non-existent key (expected behavior)
	_, exists = store.Get("nonexistent")
	if !exists {
		printInfo("Get: nonexistent = (not found)")
	}

	// Delete operation
	if err := store.Delete("user:2"); err != nil {
		log.Fatalf("✗ Delete failed: %v", err)
	}
	printSuccess("Delete: user:2")

	// Show current state
	keys := store.Keys()
	printInfo(fmt.Sprintf("Store now has %d keys: %v", store.Len(), keys))

	// ============================================================
	// Section 3: Persistence Demo - Session 1
	// ============================================================
	printSection("3] Persistence Demo - Session 1")

	printInfo("Closing store to create snapshot...")
	// Close() writes snapshot and truncates WAL (fail-safe: preserves WAL on error)
	if err := store.Close(); err != nil {
		log.Fatalf("✗ Close failed: %v", err)
	}
	printSuccess(fmt.Sprintf("Snapshot written (%s/snapshot.dat)", dataDir))
	printSuccess("WAL truncated (clean shutdown)")

	// ============================================================
	// Section 4: Persistence Demo - Session 2
	// ============================================================
	printSection("4] Persistence Demo - Session 2")

	printInfo("Reopening store to load from snapshot...")
	// Second Open() loads from snapshot - instant recovery!
	// WAL is empty because previous Close() truncated it
	store, err = kvstore.Open(dataDir)
	if err != nil {
		log.Fatalf("✗ Failed to reopen store: %v", err)
	}
	printSuccess("Data recovered from snapshot! (instant recovery)")

	// Verify data persisted
	value, exists = store.Get("user:1")
	if !exists {
		printError("Data lost! user:1 not found after recovery")
	} else {
		printSuccess(fmt.Sprintf("Verified: user:1 = %s", value))
	}

	printInfo("Adding new session data...")
	if err := store.Set("session:id", []byte("abc123")); err != nil {
		log.Fatalf("✗ Set failed: %v", err)
	}
	printSuccess("Set: session:id = abc123")

	if err := store.Set("session:active", []byte("true")); err != nil {
		log.Fatalf("✗ Set failed: %v", err)
	}
	printSuccess("Set: session:active = true")

	// ============================================================
	// Section 5: Advanced Features
	// ============================================================
	printSection("5] Advanced Features")

	// Show all keys
	keys = store.Keys()
	printInfo(fmt.Sprintf("All keys: %v", keys))
	printInfo(fmt.Sprintf("Total entries: %d", store.Len()))

	// Binary data storage example
	type Config struct {
		MaxConns int    `json:"max_conns"`
		Timeout  int    `json:"timeout"`
		Host     string `json:"host"`
	}
	config := Config{MaxConns: 100, Timeout: 30, Host: "localhost"}
	configJSON, _ := json.Marshal(config)
	if err := store.Set("binary:config", configJSON); err != nil {
		log.Fatalf("✗ Set failed: %v", err)
	}
	printSuccess(fmt.Sprintf("Binary data: Stored serialized struct (%d bytes)", len(configJSON)))

	// Empty value handling
	if err := store.Set("empty:key", []byte{}); err != nil {
		log.Fatalf("✗ Set failed: %v", err)
	}
	emptyVal, exists := store.Get("empty:key")
	if exists && len(emptyVal) == 0 {
		printSuccess("Empty value: Stored key with 0-byte value")
	}

	// ============================================================
	// Section 6: Cleanup & Summary
	// ============================================================
	printSection("6] Cleanup & Summary")

	// Always defer Close() to ensure snapshot is written
	if err := store.Close(); err != nil {
		log.Fatalf("✗ Close failed: %v", err)
	}
	printSuccess("Store closed successfully")

	fmt.Println("\nDemo complete! Features demonstrated:")
	fmt.Println("  ✓ Basic operations (Set, Get, Delete)")
	fmt.Println("  ✓ Data persistence via snapshots")
	fmt.Println("  ✓ Clean shutdown and recovery")
	fmt.Println("  ✓ Error handling")
	fmt.Println("  ✓ Binary data storage")

	fmt.Printf("\nFiles created in %s/:\n", dataDir)
	fmt.Println("  - snapshot.dat (snapshot file with all data)")
	fmt.Println("  - wal.log (write-ahead log, empty after clean shutdown)")

	fmt.Println("\nRun again with: go run cmd/example/main.go")
	fmt.Println("Keep data with: go run cmd/example/main.go -keep")
}
