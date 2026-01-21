package kvstore

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// Helper function to create a temporary directory for testing
func createTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

// Helper function to clean up test directory
func cleanupDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Errorf("Failed to cleanup temp dir: %v", err)
	}
}

// TestWALCreate tests WAL file creation
func TestWALCreate(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	wal, err := NewWAL(dir, true)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	// Verify WAL file exists
	walPath := filepath.Join(dir, "wal.log")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file does not exist: %v", err)
	}

	// Verify file is empty initially
	if info.Size() != 0 {
		t.Errorf("Expected empty WAL file, got size: %d", info.Size())
	}
}

// TestWALAppendSingle tests appending a single entry
func TestWALAppendSingle(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	wal, err := NewWAL(dir, true)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// Append an entry
	entry := NewSetEntry("key1", []byte("value1"))
	if err := wal.Append(entry); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify file size increased
	walPath := filepath.Join(dir, "wal.log")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("WAL file is empty after append")
	}

	// Close and reopen to verify persistence
	if err := wal.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and replay
	wal2, err := NewWAL(dir, true)
	if err != nil {
		t.Fatalf("NewWAL (reopen) failed: %v", err)
	}
	defer wal2.Close()

	count := 0
	err = wal2.Replay(func(e *Entry) error {
		count++
		if e.Key != entry.Key {
			t.Errorf("Key mismatch: got %q, want %q", e.Key, entry.Key)
		}
		if !bytes.Equal(e.Value, entry.Value) {
			t.Errorf("Value mismatch: got %v, want %v", e.Value, entry.Value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 entry in replay, got %d", count)
	}
}

// TestWALAppendMultiple tests appending multiple entries
func TestWALAppendMultiple(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	wal, err := NewWAL(dir, true)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// Append 100 entries
	entries := make([]*Entry, 100)
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			entries[i] = NewSetEntry("key"+string(rune(i)), []byte("value"+string(rune(i))))
		} else {
			entries[i] = NewDeleteEntry("key" + string(rune(i)))
		}
		if err := wal.Append(entries[i]); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and replay
	wal2, err := NewWAL(dir, true)
	if err != nil {
		t.Fatalf("NewWAL (reopen) failed: %v", err)
	}
	defer wal2.Close()

	count := 0
	err = wal2.Replay(func(e *Entry) error {
		if count >= len(entries) {
			t.Errorf("More entries than expected: count=%d", count)
			return nil
		}
		if e.Key != entries[count].Key {
			t.Errorf("Entry %d: Key mismatch: got %q, want %q", count, e.Key, entries[count].Key)
		}
		if e.Operation != entries[count].Operation {
			t.Errorf("Entry %d: Operation mismatch: got %d, want %d", count, e.Operation, entries[count].Operation)
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if count != 100 {
		t.Errorf("Expected 100 entries in replay, got %d", count)
	}
}

// TestWALReplayEmpty tests replaying an empty WAL
func TestWALReplayEmpty(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	wal, err := NewWAL(dir, true)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	count := 0
	err = wal.Replay(func(e *Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 entries in replay, got %d", count)
	}
}

// TestWALCorruptionDetection tests that corrupted entries are detected
func TestWALCorruptionDetection(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	wal, err := NewWAL(dir, true)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// Append valid entries
	for i := 0; i < 5; i++ {
		entry := NewSetEntry("key", []byte("value"))
		if err := wal.Append(entry); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Manually corrupt the file (flip some bits in the middle)
	walPath := filepath.Join(dir, "wal.log")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	// Corrupt a byte in the middle
	if len(data) > 100 {
		data[len(data)/2] ^= 0xFF
	}

	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatalf("Failed to write corrupted WAL: %v", err)
	}

	// Reopen and replay - should stop at corruption
	wal2, err := NewWAL(dir, true)
	if err != nil {
		t.Fatalf("NewWAL (reopen) failed: %v", err)
	}
	defer wal2.Close()

	count := 0
	err = wal2.Replay(func(e *Entry) error {
		count++
		return nil
	})

	// Replay should succeed but stop at corruption
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	// Should have recovered some entries before corruption
	if count >= 5 {
		t.Errorf("Expected less than 5 entries (stopped at corruption), got %d", count)
	}
}

// TestWALTruncate tests WAL truncation
func TestWALTruncate(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	wal, err := NewWAL(dir, true)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	// Append entries
	for i := 0; i < 10; i++ {
		entry := NewSetEntry("key", []byte("value"))
		if err := wal.Append(entry); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Truncate
	if err := wal.Truncate(); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify file size is 0
	walPath := filepath.Join(dir, "wal.log")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("Failed to stat WAL file: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("Expected empty WAL after truncate, got size: %d", info.Size())
	}

	// Verify replay returns no entries
	count := 0
	err = wal.Replay(func(e *Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay after truncate failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 entries after truncate, got %d", count)
	}

	// Verify can still append after truncate
	entry := NewSetEntry("new-key", []byte("new-value"))
	if err := wal.Append(entry); err != nil {
		t.Fatalf("Append after truncate failed: %v", err)
	}
}

// TestWALSyncMode tests both sync modes
func TestWALSyncMode(t *testing.T) {
	testCases := []struct {
		name     string
		syncMode bool
	}{
		{"sync enabled", true},
		{"sync disabled", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := createTempDir(t)
			defer cleanupDir(t, dir)

			wal, err := NewWAL(dir, tc.syncMode)
			if err != nil {
				t.Fatalf("NewWAL failed: %v", err)
			}
			defer wal.Close()

			// Append entries
			entry := NewSetEntry("key", []byte("value"))
			if err := wal.Append(entry); err != nil {
				t.Fatalf("Append failed: %v", err)
			}

			// Both modes should work correctly
			count := 0
			err = wal.Replay(func(e *Entry) error {
				count++
				return nil
			})
			if err != nil {
				t.Fatalf("Replay failed: %v", err)
			}
			if count != 1 {
				t.Errorf("Expected 1 entry, got %d", count)
			}
		})
	}
}

// TestWALConcurrentAppend tests concurrent appends
func TestWALConcurrentAppend(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	wal, err := NewWAL(dir, false) // Disable sync for speed
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	// Launch 10 goroutines, each appending 100 entries
	const numGoroutines = 10
	const entriesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				entry := NewSetEntry("key", []byte("value"))
				if err := wal.Append(entry); err != nil {
					t.Errorf("Goroutine %d: Append failed: %v", id, err)
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify all entries are present
	count := 0
	err = wal.Replay(func(e *Entry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	expectedCount := numGoroutines * entriesPerGoroutine
	if count != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, count)
	}
}

// TestWALRecoveryAfterCrash simulates crash recovery
func TestWALRecoveryAfterCrash(t *testing.T) {
	dir := createTempDir(t)
	defer cleanupDir(t, dir)

	// First session: write data without closing properly (simulate crash)
	wal1, err := NewWAL(dir, true)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	entries := []*Entry{
		NewSetEntry("key1", []byte("value1")),
		NewSetEntry("key2", []byte("value2")),
		NewDeleteEntry("key3"),
	}

	for _, entry := range entries {
		if err := wal1.Append(entry); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// DON'T call Close() - simulate crash
	// wal1.Close()

	// Second session: recover from crash
	wal2, err := NewWAL(dir, true)
	if err != nil {
		t.Fatalf("NewWAL (after crash) failed: %v", err)
	}
	defer wal2.Close()

	count := 0
	err = wal2.Replay(func(e *Entry) error {
		if count >= len(entries) {
			t.Errorf("More entries than expected")
			return nil
		}
		if e.Key != entries[count].Key {
			t.Errorf("Entry %d: Key mismatch: got %q, want %q", count, e.Key, entries[count].Key)
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Replay after crash failed: %v", err)
	}

	if count != len(entries) {
		t.Errorf("Expected %d entries after crash recovery, got %d", len(entries), count)
	}
}
