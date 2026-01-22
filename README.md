# Key Value Store

A persistent, thread-safe key-value store implemented in Go with Write-Ahead Logging (WAL) and snapshot support for durability and fast recovery.

## Features

- **Persistent Storage**: All data is durable and survives restarts
- **Write-Ahead Logging (WAL)**: Ensures data integrity through crash recovery
- **Snapshots**: Fast recovery on clean shutdowns via binary snapshots
- **Thread-Safe**: Concurrent reads, exclusive writes using `sync.RWMutex`
- **Binary Format**: Efficient serialization with CRC32 checksums
- **Atomic Operations**: Snapshot writes are atomic using temp file + rename

## Installation

```bash
go get github.com/caresle/kvstore
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/caresle/kvstore"
)

func main() {
    // Open or create a store
    store, err := kvstore.Open("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()
    
    // Set a value
    if err := store.Set("username", []byte("alice")); err != nil {
        log.Fatal(err)
    }
    
    // Get a value
    value, exists := store.Get("username")
    if exists {
        fmt.Printf("username: %s\n", value)
    }
    
    // Delete a value
    if err := store.Delete("username"); err != nil {
        log.Fatal(err)
    }
}
```

## How It Works

### Data Persistence

The store uses two mechanisms for durability:

1. **Write-Ahead Log (WAL)**: Every write operation is first appended to the WAL before updating the in-memory map
   - File: `data/wal.log`
   - Used for crash recovery

2. **Snapshots**: On clean shutdown, the entire in-memory map is serialized to a snapshot file
   - File: `data/snapshot.dat`
   - Used for fast recovery on startup

### Recovery Behavior

**Clean Shutdown** (store.Close() called):
1. Snapshot is written to disk
2. WAL is truncated (cleared)
3. On next startup: load snapshot (instant recovery)

**Crash/Abrupt Shutdown** (process killed):
1. Snapshot remains from last clean shutdown
2. WAL contains operations since last snapshot
3. On next startup: load snapshot + replay WAL (full recovery)

### Binary Format

**Snapshot Format**:
```
Header (20 bytes):
  Magic:     4 bytes (0x4B565350 - "KVSP")
  Timestamp: 8 bytes (int64, nanoseconds)
  Count:     4 bytes (uint32, entry count)
  CRC32:     4 bytes (header checksum)

Each Entry (variable):
  KeyLen:    4 bytes (uint32)
  Key:       variable bytes
  ValueLen:  4 bytes (uint32)
  Value:     variable bytes
  CRC32:     4 bytes (entry checksum)
```

**WAL Format**:
```
Each Entry (variable):
  Magic:     4 bytes (0x4B564C47 - "KVLG")
  Operation: 1 byte (0x01=Set, 0x02=Delete)
  Timestamp: 8 bytes (int64, nanoseconds)
  KeyLen:    4 bytes (uint32)
  Key:       variable bytes
  ValueLen:  4 bytes (uint32)
  Value:     variable bytes
  CRC32:     4 bytes (checksum)
```

## API Reference

### Types

```go
type Store struct { /* ... */ }

type Config struct {
    DataDir    string  // Directory for data files (default: required)
    SyncWrites bool    // Fsync after each write (default: true)
}
```

### Functions

**`Open(dataDir string) (*Store, error)`**
Opens or creates a store with default configuration.

**`OpenWithConfig(config Config) (*Store, error)`**
Opens a store with custom configuration.

**`(s *Store) Set(key string, value []byte) error`**
Stores a key-value pair. Returns error on WAL write failure.

**`(s *Store) Get(key string) ([]byte, bool)`**
Retrieves a value by key. Returns (value, true) if found, (nil, false) otherwise.

**`(s *Store) Delete(key string) error`**
Removes a key-value pair. Returns error on WAL write failure.

**`(s *Store) Close() error`**
Closes the store gracefully:
- Writes snapshot to disk
- Truncates WAL (only if snapshot succeeds)
- Closes WAL file

Returns error if snapshot or close fails. If snapshot fails, WAL is preserved for recovery.

**`(s *Store) Len() int`**
Returns the number of key-value pairs in the store.

**`(s *Store) Keys() []string`**
Returns a slice of all keys in the store.

## Error Handling

### Fail-Safe Guarantees

- **Snapshot Write Failure**: If snapshot writing fails during `Close()`, the WAL is **NOT** truncated, preserving all data for recovery on next startup
- **Corrupted Snapshot**: Detected via CRC32 validation; returns error on load
- **Corrupted WAL**: Partial recovery - replays valid entries, stops at first corruption

### Example Error Handling

```go
store, err := kvstore.Open("./data")
if err != nil {
    log.Fatalf("Failed to open store: %v", err)
}

if err := store.Set("key", []byte("value")); err != nil {
    log.Printf("Write failed: %v", err)
    // Data NOT persisted, retry or handle gracefully
}

if err := store.Close(); err != nil {
    log.Printf("Close failed: %v", err)
    // Snapshot may have failed, but WAL is intact
}
```

## Testing

Run tests:
```bash
make test
```

Run tests with race detector:
```bash
make test_race
```

## Project Status

**Phase 5 Complete** ✅

- ✅ In-memory key-value store with thread safety
- ✅ Write-Ahead Logging (WAL) with binary encoding
- ✅ Crash recovery via WAL replay
- ✅ Snapshots for fast recovery on clean shutdown
- ✅ CRC32 validation for data integrity
- ✅ Comprehensive test suite with race detection

## License

See LICENSE file for details.
