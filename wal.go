package kvstore

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// WAL represents a Write-Ahead Log for durability
type WAL struct {
	file     *os.File
	mu       sync.Mutex
	dataDir  string
	syncMode bool
}

// NewWAL creates or opens a Write-Ahead Log in the specified directory
func NewWAL(dataDir string, syncMode bool) (*WAL, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Open or create WAL file
	walPath := filepath.Join(dataDir, "wal.log")
	file, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		file:     file,
		dataDir:  dataDir,
		syncMode: syncMode,
	}, nil
}

// Append writes an entry to the WAL
func (w *WAL) Append(entry *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Encode entry to in-memory buffer first (atomic write preparation)
	var buf bytes.Buffer
	if err := entry.Encode(&buf); err != nil {
		return fmt.Errorf("failed to encode entry: %w", err)
	}

	// Write buffer to file atomically
	if _, err := w.file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Sync to disk if configured
	if w.syncMode {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync WAL: %w", err)
		}
	}

	return nil
}

// Replay reads all entries from the WAL and calls the callback for each valid entry
// Stops at first corrupted entry (partial recovery)
// Skips unknown operation codes (forward compatibility)
func (w *WAL) Replay(callback func(*Entry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to beginning of file
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to start of WAL: %w", err)
	}

	for {
		entry, err := DecodeEntry(w.file)
		if err != nil {
			// EOF is normal - end of valid entries
			if err == io.EOF {
				break
			}

			// Checksum mismatch means corruption - stop replay (partial recovery)
			if strings.Contains(err.Error(), "checksum mismatch") {
				// Log warning but don't return error - allow partial recovery
				fmt.Fprintf(os.Stderr, "WAL replay: corruption detected, stopping at corrupted entry: %v\n", err)
				break
			}

			// Other errors (like truncated entry) also stop replay
			if strings.Contains(err.Error(), "failed to read") {
				fmt.Fprintf(os.Stderr, "WAL replay: incomplete entry detected, stopping: %v\n", err)
				break
			}

			// Unexpected error
			return fmt.Errorf("failed to decode WAL entry: %w", err)
		}

		// Skip unknown operations (forward compatibility)
		if entry.Operation != OpSet && entry.Operation != OpDelete {
			fmt.Fprintf(os.Stderr, "WAL replay: unknown operation code 0x%X, skipping entry\n", entry.Operation)
			continue
		}

		// Call callback with entry
		if err := callback(entry); err != nil {
			return fmt.Errorf("callback failed during replay: %w", err)
		}
	}

	// Seek to end of file for new appends
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to end of WAL: %w", err)
	}

	return nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Sync any remaining data to disk
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL on close: %w", err)
	}

	// Close file handle
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	return nil
}

// Truncate clears the WAL file (called after successful snapshot)
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Truncate file to 0 bytes
	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	// Seek to beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek after truncate: %w", err)
	}

	return nil
}
