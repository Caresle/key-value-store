package kvstore

import (
	"bytes"
	"testing"
)

func TestOpenStore(t *testing.T) {
	store, err := Open("./testing-data")

	if err != nil {
		t.Fatalf("Error opening the store path")
	}

	defer store.Close()
}

func TestSetKey(t *testing.T) {
	store, err := Open("./testing-data")

	if err != nil {
		t.Fatalf("Error opening the store path")
	}

	defer store.Close()

	err = store.Set("k1", []byte("1"))

	if err != nil {
		t.Fatalf("Error saving key k1 of type string")
	}
}

func TestSimpleSetGet(t *testing.T) {
	store, err := Open("./testing-data")
	if err != nil {
		t.Fatalf("Error opening the store path")
	}

	defer store.Close()

	err = store.Set("k1", []byte("1"))

	if err != nil {
		t.Fatalf("Error saving key k1 of type string")
	}

	val, exists := store.Get("k1")

	if !exists {
		t.Fatalf("Error returning key k1")
	}

	if !bytes.Equal(val, []byte("1")) {
		t.Fatalf("Value doesn't match")
	}
}
