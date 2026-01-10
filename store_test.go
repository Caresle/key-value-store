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
