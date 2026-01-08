package kvstore

import "sync"

type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
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
	return &Store{
		data: make(map[string][]byte),
	}, nil
}

// Store methods
func (s *Store) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
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
	delete(s.data, key)
	return nil
}

func (s *Store) Close() error {
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
