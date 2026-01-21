# Key-Value Store Implementation Roadmap

## Design Decisions Summary

✅ **Module name**: `kvstore`  
✅ **Error handling**: Return errors (Option B)  
✅ **Snapshot strategy**: On clean shutdown only (Option 1)  
✅ **Binary format**: Efficient binary with magic numbers  
✅ **Value type**: `[]byte` for flexibility  
✅ **Concurrency**: `sync.RWMutex` - multiple readers, single writer  
✅ **File structure**: Flat package structure in root  

---

## Implementation Phases

### Phase 1: Foundation (Start Here)
- [x] Initialize Go module: `go mod init kvstore`
- [x] Create basic Store struct with in-memory hash map
- [x] Implement Get/Set/Delete without persistence (just `sync.RWMutex` + `map[string][]byte`)
- [x] Write basic tests to verify operations work and are thread-safe (`go test -race`)

**Goal**: Get comfortable with Go maps, mutexes, and testing before adding complexity.

---

### Phase 2: WAL Entry Encoding
- [x] Design the Entry struct (operation, timestamp, key, value)
- [x] Implement binary encoding/decoding functions (use `encoding/binary` package)
- [x] Write tests for encoding/decoding (ensure round-trip works)

**Goal**: Understand binary serialization before working with files.

---

### Phase 3: Write-Ahead Log
- [ ] Implement WAL struct with file handle
- [ ] Implement Append() method to write entries to disk
- [ ] Implement Read() method to replay entries from disk
- [ ] Write WAL tests (create, append, read back)

**Goal**: Get persistence working for individual operations.

---

### Phase 4: Integration
- [ ] Connect Store to WAL (call WAL.Append in Set/Delete)
- [ ] Implement recovery logic in Open() to replay WAL
- [ ] Test crash recovery (write data, close without snapshot, reopen)

**Goal**: Ensure durability - data survives restarts.

---

### Phase 5: Snapshots
- [ ] Implement snapshot writing (serialize entire map to file)
- [ ] Implement snapshot loading (deserialize on startup)
- [ ] Integrate with Close() method (snapshot before shutdown)
- [ ] Test clean shutdown recovery (should load snapshot, not replay WAL)

**Goal**: Fast recovery on clean shutdowns.

---

### Phase 6: Polish
- [ ] Add error handling throughout
- [ ] Add comprehensive tests (concurrent access, large datasets, edge cases)
- [ ] Create example program in cmd/example/main.go
- [ ] Write documentation (README with usage examples, design decisions)

---

## Project Structure

```
key-value-store/
├── README.md
├── go.mod
├── .gitignore
├── store.go              # Main Store implementation (public API)
├── store_test.go         # Store tests
├── wal.go                # Write-Ahead Log implementation
├── wal_test.go           # WAL tests
├── snapshot.go           # Snapshot functionality
├── snapshot_test.go      # Snapshot tests
├── entry.go              # WAL entry encoding/decoding
├── entry_test.go         # Entry serialization tests
└── cmd/
    └── example/
        └── main.go       # Example usage demo
```

---

## Public API (store.go)

```go
package kvstore

// Store represents a persistent key-value store
type Store struct {
    // private fields
}

// Config holds configuration for the store
type Config struct {
    DataDir    string  // Directory for WAL and snapshot files
    SyncWrites bool    // Whether to fsync after each write (default: true)
}

// Open opens or creates a key-value store at the given directory
func Open(dataDir string) (*Store, error)

// OpenWithConfig opens a store with custom configuration
func OpenWithConfig(config Config) (*Store, error)

// Set stores a key-value pair
func (s *Store) Set(key string, value []byte) error

// Get retrieves a value by key
// Returns (value, true) if found, (nil, false) if not found
func (s *Store) Get(key string) ([]byte, bool)

// Delete removes a key-value pair
func (s *Store) Delete(key string) error

// Close gracefully shuts down the store
func (s *Store) Close() error

// Len returns the number of keys in the store
func (s *Store) Len() int

// Keys returns all keys in the store (for debugging/testing)
func (s *Store) Keys() []string
```

---

## Binary Format Specifications

### WAL Entry Format
```
┌─────────────┬──────────┬───────────┬─────────┬─────────────┬───────────┬──────────┐
│  Magic (4B) │ Op (1B)  │TS (8B)    │KLen(4B) │ Key (var)   │VLen (4B)  │Val (var) │
└─────────────┴──────────┴───────────┴─────────┴─────────────┴───────────┴──────────┘
```

**Fields:**
- **Magic**: `0x4B564C47` ("KVLG" - KV LoG)
- **Operation**: `0x01` = SET, `0x02` = DELETE
- **Timestamp**: Unix nano (int64, big-endian)
- **KeyLen**: uint32, big-endian
- **Key**: UTF-8 string bytes
- **ValueLen**: uint32, big-endian (0 for DELETE)
- **Value**: Raw bytes (empty for DELETE)

### Snapshot Format
```
┌─────────────┬───────────┬───────────┬─────────────────────────────┐
│  Magic (4B) │ TS (8B)   │Count (4B) │  Entries (repeated)          │
└─────────────┴───────────┴───────────┴─────────────────────────────┘

Each entry:
┌───────────┬─────────┬─────────────┬───────────┐
│ KLen(4B)  │Key(var) │ VLen (4B)   │Val (var)  │
└───────────┴─────────┴─────────────┴───────────┘
```

**Fields:**
- **Magic**: `0x4B565350` ("KVSP" - KV SnaPshot)
- **Timestamp**: Unix nano (int64, big-endian)
- **Count**: Number of entries (uint32, big-endian)
- **Entries**: Repeated (KeyLen, Key, ValueLen, Value) tuples

---

## Recovery Mechanism

**Startup Sequence:**
1. Check if dataDir exists → Create if needed
2. Look for latest snapshot file → Load if found
3. Look for WAL files newer than snapshot → Replay if found
4. Create new WAL file for current session
5. Store is ready for operations

**Recovery Scenarios:**
- **Clean shutdown**: Load snapshot → instant recovery
- **Crash**: Load old snapshot + replay entire WAL → slower recovery

---

## Concurrency Model

**Data Structures:**
```go
type Store struct {
    mu       sync.RWMutex           // Protects data map
    data     map[string][]byte      // In-memory key-value pairs
    wal      *WAL                   // Write-Ahead Log
    config   Config                 // Configuration
    closed   bool                   // Whether store is closed
}
```

**Lock Strategy:**

**Read Operations (Get, Len, Keys):**
1. Acquire RLock (allows multiple concurrent readers)
2. Read from map
3. Release RLock
4. Return result

**Write Operations (Set, Delete):**
1. Acquire Lock (exclusive, blocks all readers/writers)
2. Append to WAL (with fsync if configured)
   - If WAL write fails → Release lock, return error
3. Modify in-memory map
4. Release Lock
5. Return success

**Close Operation:**
1. Acquire Lock (exclusive)
2. Set closed = true
3. Write snapshot to disk
4. Close WAL file
5. Release Lock

---

## Key Go Packages You'll Need

- `sync` - RWMutex for thread safety
- `encoding/binary` - Binary serialization (BigEndian)
- `os` - File I/O operations
- `io` - Reader/Writer interfaces
- `path/filepath` - Path manipulation
- `time` - Timestamps
- `testing` - Unit tests
- `errors` / `fmt` - Error handling

---

## Helpful Testing Commands

```bash
# Run all tests
go test ./...

# Run tests with race detector (IMPORTANT for concurrency)
go test -race ./...

# Run tests with verbose output
go test -v ./...

# Run specific test
go test -run TestStoreConcurrency

# Run benchmarks
go test -bench=.

# Check test coverage
go test -cover ./...
```

---

## Tips for Success

1. **Start simple**: Get the in-memory store working first before adding persistence
2. **Test early, test often**: Write tests as you go, especially for concurrency
3. **Use `go test -race`**: This will catch race conditions you might miss
4. **Read the encoding/binary docs**: Understanding BigEndian vs LittleEndian is key
5. **Handle file errors gracefully**: Disk I/O can fail in many ways
6. **Keep functions small**: Each function should do one thing well
7. **Don't optimize prematurely**: Get it working correctly first, then optimize

---

## When to Ask for Help

Feel free to reach out when you:
- 🤔 Don't understand how a Go package works
- 🐛 Hit a tricky bug you can't figure out
- 🔒 Need help with concurrency patterns or race conditions
- 📝 Want to review your design before implementing
- 🧪 Need help writing tests for a specific scenario
- 💡 Want to discuss tradeoffs between different approaches
- 📚 Need clarification on any of the design decisions above

---

## Example Usage

```go
package main

import (
    "log"
    "kvstore"
)

func main() {
    // Open/create store with persistence directory
    db, err := kvstore.Open("./data")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Set a value
    if err := db.Set("user:1", []byte("Alice")); err != nil {
        log.Fatal(err)
    }
    
    // Get a value
    if val, ok := db.Get("user:1"); ok {
        log.Printf("Found: %s", val)
    }
    
    // Delete a value
    if err := db.Delete("user:1"); err != nil {
        log.Fatal(err)
    }
}
```

---

## Additional Resources

- [Designing Data-Intensive Applications](https://dataintensive.net/) - Chapter on Storage Engines
- [Go sync.RWMutex docs](https://pkg.go.dev/sync#RWMutex)
- [Go encoding/binary docs](https://pkg.go.dev/encoding/binary)
- [Write-Ahead Logging (Wikipedia)](https://en.wikipedia.org/wiki/Write-ahead_logging)
- [LSM Trees and SSTables](https://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/) - For future enhancements
