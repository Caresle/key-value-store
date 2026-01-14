package kvstore

import (
	"bytes"
	"testing"
)

func TestEntryEncodeDecodeSet(t *testing.T) {
	original := NewSetEntry("test-key", []byte("test-value"))

	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeEntry(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Operation != original.Operation {
		t.Errorf("Operation mismatch: got %d, want %d", decoded.Operation, original.Operation)
	}

	if decoded.Timestamp != original.Timestamp {
		t.Errorf("Operation mismatch: got %d, want %d", decoded.Timestamp, original.Timestamp)
	}

	if decoded.Key != original.Key {
		t.Errorf("Operation mismatch: got %q, want %q", decoded.Key, original.Key)
	}

	if !bytes.Equal(decoded.Value, original.Value) {
		t.Errorf("value mismatch: got %q, want %q", decoded.Value, original.Value)
	}
}
