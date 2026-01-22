package kvstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

const SnapshotMagic uint32 = 0x4B565350 // "KVSP" - KV SnaPshot

const snapshotFilename = "snapshot.dat"
const snapshotTempFilename = "snapshot.dat.tmp"

// writeSnapshot serializes the entire map to a snapshot file
// Format:
//
//	Header: Magic(4) | Timestamp(8) | Count(4) | HeaderCRC32(4)
//	Each Entry: KeyLen(4) | Key(var) | ValueLen(4) | Value(var) | EntryCRC32(4)
//
// Uses atomic write (temp file + rename) to prevent corruption
// Returns error if write fails (caller should preserve WAL)
func writeSnapshot(dataDir string, data map[string][]byte) error {
	// Create temp file for atomic write
	tempPath := filepath.Join(dataDir, snapshotTempFilename)
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create snapshot temp file: %w", err)
	}

	// Ensure cleanup on error
	defer func() {
		if file != nil {
			file.Close()
			os.Remove(tempPath)
		}
	}()

	// Write header
	timestamp := time.Now().UnixNano()
	count := uint32(len(data))

	var headerBuf bytes.Buffer
	if err := binary.Write(&headerBuf, binary.BigEndian, SnapshotMagic); err != nil {
		return fmt.Errorf("failed to write magic: %w", err)
	}
	if err := binary.Write(&headerBuf, binary.BigEndian, timestamp); err != nil {
		return fmt.Errorf("failed to write timestamp: %w", err)
	}
	if err := binary.Write(&headerBuf, binary.BigEndian, count); err != nil {
		return fmt.Errorf("failed to write count: %w", err)
	}

	// Compute header checksum
	headerChecksum := crc32.ChecksumIEEE(headerBuf.Bytes())

	// Write header + checksum to file
	if _, err := file.Write(headerBuf.Bytes()); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}
	if err := binary.Write(file, binary.BigEndian, headerChecksum); err != nil {
		return fmt.Errorf("failed to write header checksum: %w", err)
	}

	// Write each entry
	for key, value := range data {
		var entryBuf bytes.Buffer

		keyBytes := []byte(key)
		keyLen := uint32(len(keyBytes))
		if err := binary.Write(&entryBuf, binary.BigEndian, keyLen); err != nil {
			return fmt.Errorf("failed to write key length: %w", err)
		}
		if _, err := entryBuf.Write(keyBytes); err != nil {
			return fmt.Errorf("failed to write key: %w", err)
		}

		valueLen := uint32(len(value))
		if err := binary.Write(&entryBuf, binary.BigEndian, valueLen); err != nil {
			return fmt.Errorf("failed to write value length: %w", err)
		}
		if valueLen > 0 {
			if _, err := entryBuf.Write(value); err != nil {
				return fmt.Errorf("failed to write value: %w", err)
			}
		}

		// Compute entry checksum
		entryChecksum := crc32.ChecksumIEEE(entryBuf.Bytes())

		// Write entry + checksum to file
		if _, err := file.Write(entryBuf.Bytes()); err != nil {
			return fmt.Errorf("failed to write entry: %w", err)
		}
		if err := binary.Write(file, binary.BigEndian, entryChecksum); err != nil {
			return fmt.Errorf("failed to write entry checksum: %w", err)
		}
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot: %w", err)
	}

	// Close file before rename
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot temp file: %w", err)
	}
	file = nil // Prevent defer cleanup

	// Atomic rename
	snapshotPath := filepath.Join(dataDir, snapshotFilename)
	if err := os.Rename(tempPath, snapshotPath); err != nil {
		return fmt.Errorf("failed to rename snapshot file: %w", err)
	}

	return nil
}

// loadSnapshot reads a snapshot file and returns the deserialized map
// Returns empty map + nil if snapshot doesn't exist (not an error)
// Returns error if snapshot exists but is corrupted
func loadSnapshot(dataDir string) (map[string][]byte, error) {
	snapshotPath := filepath.Join(dataDir, snapshotFilename)

	// Check if snapshot exists
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		// No snapshot = empty map (not an error)
		return make(map[string][]byte), nil
	}

	// Open snapshot file
	file, err := os.Open(snapshotPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open snapshot file: %w", err)
	}
	defer file.Close()

	// Read header
	var headerBuf bytes.Buffer
	var magic uint32
	if err := binary.Read(file, binary.BigEndian, &magic); err != nil {
		return nil, fmt.Errorf("failed to read magic: %w", err)
	}
	binary.Write(&headerBuf, binary.BigEndian, magic)

	if magic != SnapshotMagic {
		return nil, fmt.Errorf("invalid magic: expected 0x%X, got 0x%X", SnapshotMagic, magic)
	}

	var timestamp int64
	if err := binary.Read(file, binary.BigEndian, &timestamp); err != nil {
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}
	binary.Write(&headerBuf, binary.BigEndian, timestamp)

	var count uint32
	if err := binary.Read(file, binary.BigEndian, &count); err != nil {
		return nil, fmt.Errorf("failed to read count: %w", err)
	}
	binary.Write(&headerBuf, binary.BigEndian, count)

	// Verify header checksum
	var storedHeaderChecksum uint32
	if err := binary.Read(file, binary.BigEndian, &storedHeaderChecksum); err != nil {
		return nil, fmt.Errorf("failed to read header checksum: %w", err)
	}
	computedHeaderChecksum := crc32.ChecksumIEEE(headerBuf.Bytes())
	if computedHeaderChecksum != storedHeaderChecksum {
		return nil, fmt.Errorf("header checksum mismatch: expected 0x%X, got 0x%X (snapshot corrupted)", storedHeaderChecksum, computedHeaderChecksum)
	}

	// Read entries
	data := make(map[string][]byte, count)
	for i := uint32(0); i < count; i++ {
		var entryBuf bytes.Buffer

		var keyLen uint32
		if err := binary.Read(file, binary.BigEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to read key length for entry %d: %w", i, err)
		}
		binary.Write(&entryBuf, binary.BigEndian, keyLen)

		keyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key for entry %d: %w", i, err)
		}
		entryBuf.Write(keyBytes)

		var valueLen uint32
		if err := binary.Read(file, binary.BigEndian, &valueLen); err != nil {
			return nil, fmt.Errorf("failed to read value length for entry %d: %w", i, err)
		}
		binary.Write(&entryBuf, binary.BigEndian, valueLen)

		value := make([]byte, valueLen)
		if valueLen > 0 {
			if _, err := io.ReadFull(file, value); err != nil {
				return nil, fmt.Errorf("failed to read value for entry %d: %w", i, err)
			}
			entryBuf.Write(value)
		}

		// Verify entry checksum
		var storedEntryChecksum uint32
		if err := binary.Read(file, binary.BigEndian, &storedEntryChecksum); err != nil {
			return nil, fmt.Errorf("failed to read entry checksum for entry %d: %w", i, err)
		}
		computedEntryChecksum := crc32.ChecksumIEEE(entryBuf.Bytes())
		if computedEntryChecksum != storedEntryChecksum {
			return nil, fmt.Errorf("entry %d checksum mismatch: expected 0x%X, got 0x%X (snapshot corrupted)", i, storedEntryChecksum, computedEntryChecksum)
		}

		// Add to map
		key := string(keyBytes)
		data[key] = value
	}

	return data, nil
}

// snapshotExists checks if a snapshot file exists
func snapshotExists(dataDir string) bool {
	snapshotPath := filepath.Join(dataDir, snapshotFilename)
	_, err := os.Stat(snapshotPath)
	return err == nil
}
