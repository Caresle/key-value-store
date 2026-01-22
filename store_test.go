package kvstore

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestOpenStore(t *testing.T) {
	store, err := Open("./testing-data")

	if err != nil {
		t.Fatalf("Error opening the store path")
	}

	defer store.Close()
}

func TestSetGetValues(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     []byte
		wantValue []byte
	}{
		{
			name:      "simple string value",
			key:       "k1",
			value:     []byte("k1 value"),
			wantValue: []byte("k1 value"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := Open("./testing-data")

			if err != nil {
				t.Fatalf("Error opening the store path")
			}

			defer store.Close()

			err = store.Set(test.key, test.value)

			if err != nil {
				t.Fatalf("Error saving key %s of type string", test.key)
			}

			val, exists := store.Get(test.key)

			if !exists {
				t.Fatalf("Error returning key %s", test.key)
			}

			if !bytes.Equal(val, test.wantValue) {
				t.Fatalf("Value doesn't match")
			}
		})
	}
}

func TestStoreConcurrentReadWrite(t *testing.T) {
	store, err := Open("./test-data")

	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}

	defer store.Close()

	var wg sync.WaitGroup

	for i := range 50 {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", n)
			value := fmt.Appendf(nil, "value-%d", n)
			if err := store.Set(key, value); err != nil {
				t.Errorf("Error setting key %s", key)
			}
		}(i)
	}

	for i := range 50 {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			key := fmt.Sprintf("key-%d", n%10)
			store.Get(key)
		}(i)
	}

	wg.Wait()
}

func TestStoreConcurrrentWritesSameKey(t *testing.T) {
	store, err := Open("./test-data")

	if err != nil {
		t.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	var wg sync.WaitGroup

	for i := range 100 {
		wg.Add(1)

		go func(n int) {
			defer wg.Done()

			value := fmt.Appendf(nil, "value-%d", n)
			store.Set("shared-key", value)
		}(i)
	}

	wg.Wait()

	value, exists := store.Get("shared-key")

	if !exists {
		t.Errorf("key should exists after concurrent writes")
	}

	if len(value) == 0 {
		t.Fatalf("value should not be empty")
	}
}

// TestStoreRecoveryAfterCleanShutdown tests recovery after clean shutdown (WAL truncated)
func TestStoreRecoveryAfterCleanShutdown(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	// First session: write data and close cleanly
	store1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if err := store1.Set("key1", []byte("value1")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := store1.Set("key2", []byte("value2")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Close cleanly (truncates WAL)
	if err := store1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Second session: reopen
	store2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (second) failed: %v", err)
	}
	defer store2.Close()

	// Data should be recovered from snapshot
	if store2.Len() != 2 {
		t.Errorf("Expected 2 keys after clean shutdown, got %d", store2.Len())
	}

	// Verify data integrity
	value1, exists1 := store2.Get("key1")
	if !exists1 {
		t.Error("key1 not found after clean shutdown recovery")
	} else if string(value1) != "value1" {
		t.Errorf("key1 value mismatch: got %q, want 'value1'", value1)
	}

	value2, exists2 := store2.Get("key2")
	if !exists2 {
		t.Error("key2 not found after clean shutdown recovery")
	} else if string(value2) != "value2" {
		t.Errorf("key2 value mismatch: got %q, want 'value2'", value2)
	}
}

// TestStoreRecoveryAfterCrash tests recovery after crash (WAL not truncated)
func TestStoreRecoveryAfterCrash(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	// First session: write data WITHOUT closing (simulate crash)
	store1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	testData := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	for key, value := range testData {
		if err := store1.Set(key, value); err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// DON'T call Close() - simulate crash
	// store1.Close()

	// Second session: recover from crash
	store2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (after crash) failed: %v", err)
	}
	defer store2.Close()

	// Verify all data is recovered from WAL
	if store2.Len() != len(testData) {
		t.Errorf("Expected %d keys after crash recovery, got %d", len(testData), store2.Len())
	}

	for key, expectedValue := range testData {
		value, exists := store2.Get(key)
		if !exists {
			t.Errorf("Key %q not found after crash recovery", key)
			continue
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Key %q: value mismatch: got %v, want %v", key, value, expectedValue)
		}
	}
}

// TestStoreMultipleOperations tests complex operation sequences with crash recovery
func TestStoreMultipleOperations(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	// First session: complex operations
	store1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Set key1
	if err := store1.Set("key1", []byte("value1")); err != nil {
		t.Fatalf("Set key1 failed: %v", err)
	}

	// Set key2
	if err := store1.Set("key2", []byte("value2")); err != nil {
		t.Fatalf("Set key2 failed: %v", err)
	}

	// Delete key1
	if err := store1.Delete("key1"); err != nil {
		t.Fatalf("Delete key1 failed: %v", err)
	}

	// Set key1 again with different value
	if err := store1.Set("key1", []byte("value3")); err != nil {
		t.Fatalf("Set key1 (second) failed: %v", err)
	}

	// Crash (no Close)
	// store1.Close()

	// Second session: verify final state
	store2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (after crash) failed: %v", err)
	}
	defer store2.Close()

	// key1 should have value3 (last set)
	value1, exists := store2.Get("key1")
	if !exists {
		t.Error("key1 not found after crash recovery")
	} else if !bytes.Equal(value1, []byte("value3")) {
		t.Errorf("key1: got %q, want %q", value1, "value3")
	}

	// key2 should have value2
	value2, exists := store2.Get("key2")
	if !exists {
		t.Error("key2 not found after crash recovery")
	} else if !bytes.Equal(value2, []byte("value2")) {
		t.Errorf("key2: got %q, want %q", value2, "value2")
	}

	// Should have exactly 2 keys
	if store2.Len() != 2 {
		t.Errorf("Expected 2 keys, got %d", store2.Len())
	}
}

// TestStoreWALFailure tests behavior when WAL write fails
func TestStoreWALFailure(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	// Create store
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Set initial value
	if err := store.Set("key1", []byte("value1")); err != nil {
		t.Fatalf("Set key1 failed: %v", err)
	}

	// Close the WAL file to simulate failure
	store.wal.Close()

	// Try to set - should fail
	err = store.Set("key2", []byte("value2"))
	if err == nil {
		t.Error("Expected error when WAL is closed, got nil")
	}

	// Verify in-memory data was NOT modified
	_, exists := store.Get("key2")
	if exists {
		t.Error("key2 should not exist after failed Set")
	}

	// key1 should still exist
	value1, exists := store.Get("key1")
	if !exists {
		t.Error("key1 should still exist")
	} else if !bytes.Equal(value1, []byte("value1")) {
		t.Errorf("key1 value changed unexpectedly")
	}
}

// TestStoreConcurrentWritesWithRecovery tests concurrent writes and crash recovery
func TestStoreConcurrentWritesWithRecovery(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	// First session: concurrent writes
	store1, err := Open(dir)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	const numGoroutines = 10
	const keysPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < keysPerGoroutine; i++ {
				key := fmt.Sprintf("key-%d-%d", id, i)
				value := []byte(fmt.Sprintf("value-%d-%d", id, i))
				if err := store1.Set(key, value); err != nil {
					t.Errorf("Set failed: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()

	expectedCount := numGoroutines * keysPerGoroutine
	if store1.Len() != expectedCount {
		t.Errorf("Expected %d keys after concurrent writes, got %d", expectedCount, store1.Len())
	}

	// Crash (no Close)
	// store1.Close()

	// Second session: verify all keys recovered
	store2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open (after crash) failed: %v", err)
	}
	defer store2.Close()

	if store2.Len() != expectedCount {
		t.Errorf("Expected %d keys after crash recovery, got %d", expectedCount, store2.Len())
	}

	// Verify a few random keys
	testKeys := []string{"key-0-0", "key-5-5", "key-9-9"}
	for _, key := range testKeys {
		if _, exists := store2.Get(key); !exists {
			t.Errorf("Key %q not found after crash recovery", key)
		}
	}
}
