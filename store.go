package kvstore

import (
	"fmt"
	"sync"
)

type Store struct {
	mu     sync.RWMutex
	data   map[string][]byte
	wal    *WAL
	config Config
}

type Config struct {
	DataDir    string
	SyncWrites bool
}

func Open(dataDir string) (*Store, error) {
	return OpenWithConfig(Config{
		DataDir:    dataDir,
		SyncWrites: true,
	})
}

func OpenWithConfig(config Config) (*Store, error) {
	// Create WAL
	wal, err := NewWAL(config.DataDir, config.SyncWrites)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	// Create store
	store := &Store{
		data:   make(map[string][]byte),
		wal:    wal,
		config: config,
	}

	// Replay WAL to recover state
	err = wal.Replay(func(entry *Entry) error {
		// No lock needed - single-threaded during recovery
		switch entry.Operation {
		case OpSet:
			store.data[entry.Key] = entry.Value
		case OpDelete:
			delete(store.data, entry.Key)
		}
		return nil
	})

	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	return store, nil
}

// Store methods
func (s *Store) Set(key string, value []byte) error {
	// Write to WAL FIRST (before modifying memory)
	entry := NewSetEntry(key, value)
	if err := s.wal.Append(entry); err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}

	// Then update in-memory (this can't fail)
	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()

	return nil
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.data[key]

	if !exists {
		return nil, false
	}

	return value, true
}

func (s *Store) Delete(key string) error {
	// Write to WAL FIRST
	entry := NewDeleteEntry(key)
	if err := s.wal.Append(entry); err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}

	// Then update in-memory
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()

	return nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Truncate WAL (clean shutdown = no need to replay on next open)
	if err := s.wal.Truncate(); err != nil {
		s.wal.Close() // Try to close anyway
		return fmt.Errorf("WAL truncate failed: %w", err)
	}

	// Close WAL
	if err := s.wal.Close(); err != nil {
		return fmt.Errorf("WAL close failed: %w", err)
	}

	return nil
}

func (s *Store) Len() int {
	return len(s.data)
}

func (s *Store) Keys() []string {
	keys := make([]string, 0, len(s.data))

	for key := range s.data {
		keys = append(keys, key)
	}

	return keys
}
