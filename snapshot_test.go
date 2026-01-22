package kvstore

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotWriteAndLoad(t *testing.T) {
	tempDir := t.TempDir()

	// Create test data
	data := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
		"key3": []byte("value3"),
	}

	// Write snapshot
	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	// Load snapshot
	loaded, err := loadSnapshot(tempDir)
	if err != nil {
		t.Fatalf("loadSnapshot failed: %v", err)
	}

	// Verify all keys and values
	if len(loaded) != len(data) {
		t.Errorf("Size mismatch: got %d, want %d", len(loaded), len(data))
	}

	for key, expectedValue := range data {
		actualValue, exists := loaded[key]
		if !exists {
			t.Errorf("Key %q not found in loaded snapshot", key)
			continue
		}
		if !bytes.Equal(actualValue, expectedValue) {
			t.Errorf("Value mismatch for key %q: got %q, want %q", key, actualValue, expectedValue)
		}
	}
}

func TestSnapshotEmptyMap(t *testing.T) {
	tempDir := t.TempDir()

	// Write empty snapshot
	data := make(map[string][]byte)
	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	// Load snapshot
	loaded, err := loadSnapshot(tempDir)
	if err != nil {
		t.Fatalf("loadSnapshot failed: %v", err)
	}

	// Verify empty
	if len(loaded) != 0 {
		t.Errorf("Expected empty map, got %d entries", len(loaded))
	}
}

func TestSnapshotSingleEntry(t *testing.T) {
	tempDir := t.TempDir()

	data := map[string][]byte{
		"only-key": []byte("only-value"),
	}

	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	loaded, err := loadSnapshot(tempDir)
	if err != nil {
		t.Fatalf("loadSnapshot failed: %v", err)
	}

	if len(loaded) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(loaded))
	}

	value, exists := loaded["only-key"]
	if !exists {
		t.Fatal("Key 'only-key' not found")
	}
	if !bytes.Equal(value, []byte("only-value")) {
		t.Errorf("Value mismatch: got %q, want %q", value, "only-value")
	}
}

func TestSnapshotNoFile(t *testing.T) {
	tempDir := t.TempDir()

	// Load from directory with no snapshot
	loaded, err := loadSnapshot(tempDir)
	if err != nil {
		t.Fatalf("loadSnapshot should not error on missing file: %v", err)
	}

	// Should return empty map
	if len(loaded) != 0 {
		t.Errorf("Expected empty map for missing snapshot, got %d entries", len(loaded))
	}
}

func TestSnapshotBinaryFormat(t *testing.T) {
	tempDir := t.TempDir()

	data := map[string][]byte{
		"test": []byte("value"),
	}

	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	// Read raw file and verify format
	snapshotPath := filepath.Join(tempDir, snapshotFilename)
	content, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("Failed to read snapshot file: %v", err)
	}

	buf := bytes.NewBuffer(content)

	// Verify magic number
	var magic uint32
	if err := binary.Read(buf, binary.BigEndian, &magic); err != nil {
		t.Fatalf("Failed to read magic: %v", err)
	}
	if magic != SnapshotMagic {
		t.Errorf("Magic mismatch: got 0x%X, want 0x%X", magic, SnapshotMagic)
	}

	// Verify timestamp exists and is reasonable
	var timestamp int64
	if err := binary.Read(buf, binary.BigEndian, &timestamp); err != nil {
		t.Fatalf("Failed to read timestamp: %v", err)
	}
	if timestamp <= 0 {
		t.Errorf("Invalid timestamp: %d", timestamp)
	}

	// Verify count
	var count uint32
	if err := binary.Read(buf, binary.BigEndian, &count); err != nil {
		t.Fatalf("Failed to read count: %v", err)
	}
	if count != 1 {
		t.Errorf("Count mismatch: got %d, want 1", count)
	}

	// Header checksum should exist (just verify it's present)
	var headerChecksum uint32
	if err := binary.Read(buf, binary.BigEndian, &headerChecksum); err != nil {
		t.Fatalf("Failed to read header checksum: %v", err)
	}
}

func TestSnapshotCorruptedMagic(t *testing.T) {
	tempDir := t.TempDir()

	// Create a file with wrong magic
	snapshotPath := filepath.Join(tempDir, snapshotFilename)
	file, err := os.Create(snapshotPath)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	wrongMagic := uint32(0x12345678)
	binary.Write(file, binary.BigEndian, wrongMagic)
	binary.Write(file, binary.BigEndian, int64(12345))
	binary.Write(file, binary.BigEndian, uint32(0))
	binary.Write(file, binary.BigEndian, uint32(0)) // checksum
	file.Close()

	// Try to load
	_, err = loadSnapshot(tempDir)
	if err == nil {
		t.Fatal("Expected error for corrupted magic, got nil")
	}
	if !strings.Contains(err.Error(), "magic") {
		t.Errorf("Expected magic error, got: %v", err)
	}
}

func TestSnapshotCorruptedChecksum(t *testing.T) {
	tempDir := t.TempDir()

	data := map[string][]byte{
		"test": []byte("value"),
	}

	// Write valid snapshot
	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	// Corrupt the header checksum
	snapshotPath := filepath.Join(tempDir, snapshotFilename)
	content, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("Failed to read snapshot: %v", err)
	}

	// Header checksum is at bytes 16-20 (after magic, timestamp, count)
	// Flip some bits in the checksum
	if len(content) > 19 {
		content[16] ^= 0xFF
	}

	if err := os.WriteFile(snapshotPath, content, 0644); err != nil {
		t.Fatalf("Failed to write corrupted snapshot: %v", err)
	}

	// Try to load
	_, err = loadSnapshot(tempDir)
	if err == nil {
		t.Fatal("Expected error for corrupted checksum, got nil")
	}
	if !strings.Contains(err.Error(), "checksum") {
		t.Errorf("Expected checksum error, got: %v", err)
	}
}

func TestSnapshotTruncatedFile(t *testing.T) {
	tempDir := t.TempDir()

	data := map[string][]byte{
		"test": []byte("value"),
	}

	// Write valid snapshot
	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	// Truncate the file
	snapshotPath := filepath.Join(tempDir, snapshotFilename)
	content, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("Failed to read snapshot: %v", err)
	}

	// Write only first 10 bytes (incomplete)
	if err := os.WriteFile(snapshotPath, content[:10], 0644); err != nil {
		t.Fatalf("Failed to write truncated snapshot: %v", err)
	}

	// Try to load
	_, err = loadSnapshot(tempDir)
	if err == nil {
		t.Fatal("Expected error for truncated file, got nil")
	}
}

func TestSnapshotAtomicity(t *testing.T) {
	tempDir := t.TempDir()

	data := map[string][]byte{
		"key": []byte("value"),
	}

	// Write snapshot
	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	// Verify temp file was cleaned up
	tempPath := filepath.Join(tempDir, snapshotTempFilename)
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Error("Temp file should be cleaned up after successful write")
	}

	// Verify final snapshot exists
	snapshotPath := filepath.Join(tempDir, snapshotFilename)
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Errorf("Snapshot file should exist: %v", err)
	}
}

func TestSnapshotOverwrite(t *testing.T) {
	tempDir := t.TempDir()

	// Write first snapshot
	data1 := map[string][]byte{
		"old": []byte("data"),
	}
	if err := writeSnapshot(tempDir, data1); err != nil {
		t.Fatalf("First writeSnapshot failed: %v", err)
	}

	// Write second snapshot (should overwrite)
	data2 := map[string][]byte{
		"new": []byte("data"),
	}
	if err := writeSnapshot(tempDir, data2); err != nil {
		t.Fatalf("Second writeSnapshot failed: %v", err)
	}

	// Load and verify only new data exists
	loaded, err := loadSnapshot(tempDir)
	if err != nil {
		t.Fatalf("loadSnapshot failed: %v", err)
	}

	if len(loaded) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(loaded))
	}

	if _, exists := loaded["old"]; exists {
		t.Error("Old data should be overwritten")
	}

	if _, exists := loaded["new"]; !exists {
		t.Error("New data should exist")
	}
}

func TestSnapshotLargeDataset(t *testing.T) {
	tempDir := t.TempDir()

	// Create large dataset
	data := make(map[string][]byte)
	for i := 0; i < 1000; i++ {
		key := string(rune('a'+(i%26))) + string(rune('0'+(i/26)%10)) + string(rune('A'+(i/260)%26))
		value := bytes.Repeat([]byte{byte(i % 256)}, 100)
		data[key] = value
	}

	// Write snapshot
	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	// Load snapshot
	loaded, err := loadSnapshot(tempDir)
	if err != nil {
		t.Fatalf("loadSnapshot failed: %v", err)
	}

	// Verify size
	if len(loaded) != len(data) {
		t.Errorf("Size mismatch: got %d, want %d", len(loaded), len(data))
	}

	// Spot check some entries
	for key, expectedValue := range data {
		actualValue, exists := loaded[key]
		if !exists {
			t.Errorf("Key %q not found in loaded snapshot", key)
			continue
		}
		if !bytes.Equal(actualValue, expectedValue) {
			t.Errorf("Value mismatch for key %q", key)
			break // Don't spam errors
		}
	}
}

func TestSnapshotUnicode(t *testing.T) {
	tempDir := t.TempDir()

	data := map[string][]byte{
		"emoji-😀":      []byte("happy face"),
		"中文":           []byte("Chinese"),
		"العربية":      []byte("Arabic"),
		"🔑":            []byte("key emoji"),
		"mixed-日本語-en": []byte("mixed languages"),
	}

	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	loaded, err := loadSnapshot(tempDir)
	if err != nil {
		t.Fatalf("loadSnapshot failed: %v", err)
	}

	if len(loaded) != len(data) {
		t.Errorf("Size mismatch: got %d, want %d", len(loaded), len(data))
	}

	for key, expectedValue := range data {
		actualValue, exists := loaded[key]
		if !exists {
			t.Errorf("Key %q not found", key)
			continue
		}
		if !bytes.Equal(actualValue, expectedValue) {
			t.Errorf("Value mismatch for key %q: got %q, want %q", key, actualValue, expectedValue)
		}
	}
}

func TestSnapshotBinaryValues(t *testing.T) {
	tempDir := t.TempDir()

	data := map[string][]byte{
		"nulls":  {0x00, 0x00, 0x00},
		"binary": {0xFF, 0xFE, 0xFD, 0xFC},
		"mixed":  {0x00, 0x41, 0x00, 0x42}, // null + ASCII
	}

	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	loaded, err := loadSnapshot(tempDir)
	if err != nil {
		t.Fatalf("loadSnapshot failed: %v", err)
	}

	for key, expectedValue := range data {
		actualValue, exists := loaded[key]
		if !exists {
			t.Errorf("Key %q not found", key)
			continue
		}
		if !bytes.Equal(actualValue, expectedValue) {
			t.Errorf("Value mismatch for key %q: got %v, want %v", key, actualValue, expectedValue)
		}
	}
}

func TestSnapshotEmptyValue(t *testing.T) {
	tempDir := t.TempDir()

	data := map[string][]byte{
		"empty":    {},
		"nonempty": []byte("value"),
	}

	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	loaded, err := loadSnapshot(tempDir)
	if err != nil {
		t.Fatalf("loadSnapshot failed: %v", err)
	}

	emptyValue, exists := loaded["empty"]
	if !exists {
		t.Error("Key 'empty' not found")
	}
	if len(emptyValue) != 0 {
		t.Errorf("Expected empty value, got %d bytes", len(emptyValue))
	}

	nonEmptyValue, exists := loaded["nonempty"]
	if !exists {
		t.Error("Key 'nonempty' not found")
	}
	if !bytes.Equal(nonEmptyValue, []byte("value")) {
		t.Errorf("Value mismatch: got %q, want 'value'", nonEmptyValue)
	}
}

func TestSnapshotExists(t *testing.T) {
	tempDir := t.TempDir()

	// Initially no snapshot
	if snapshotExists(tempDir) {
		t.Error("snapshotExists should return false for non-existent snapshot")
	}

	// Write snapshot
	data := map[string][]byte{"key": []byte("value")}
	if err := writeSnapshot(tempDir, data); err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	// Now should exist
	if !snapshotExists(tempDir) {
		t.Error("snapshotExists should return true after writing snapshot")
	}
}
