package kvstore

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
	"time"
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

func TestEntryInvalidMagic(t *testing.T) {
	var buf bytes.Buffer

	wrongMagic := uint(0x12345678)

	binary.Write(&buf, binary.BigEndian, wrongMagic)
	binary.Write(&buf, binary.BigEndian, byte(OpSet))
	binary.Write(&buf, binary.BigEndian, time.Now().UnixNano())
	binary.Write(&buf, binary.BigEndian, uint32(7))

	buf.Write([]byte("test-key"))
	binary.Write(&buf, binary.BigEndian, uint32(10)) // value length

	buf.Write([]byte("test-value"))
	binary.Write(&buf, binary.BigEndian, int64(123456789))

	_, err := DecodeEntry(&buf)

	if err == nil {
		t.Error("Expected error for invalid magic, got nil")
	}

	if !strings.Contains(err.Error(), "magic") {
		t.Errorf("Expected magic error, got: %v", err)
	}
}

func TestEntryEmptyKey(t *testing.T) {
	original := NewSetEntry("", []byte("test-value"))

	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeEntry(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Key != "" {
		t.Errorf("Expected empty key, got %q", decoded.Key)
	}

	if !bytes.Equal(decoded.Value, original.Value) {
		t.Errorf("value mismatch: got %q, want %q", decoded.Value, original.Value)
	}
}

func TestEntryEmptyValue(t *testing.T) {
	original := NewSetEntry("test-key", []byte(""))

	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeEntry(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Key != original.Key {
		t.Errorf("Key mismatch: got %q, want %q", decoded.Key, original.Key)
	}

	if len(decoded.Value) != 0 {
		t.Errorf("Expected empty value, got %q", decoded.Value)
	}
}

func TestEntryNilValue(t *testing.T) {
	original := NewSetEntry("test-key", nil)

	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeEntry(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Key != original.Key {
		t.Errorf("Key mismatch: got %q, want %q", decoded.Key, original.Key)
	}

	if len(decoded.Value) != 0 {
		t.Errorf("Expected nil/empty value, got %q", decoded.Value)
	}
}

func TestEntryDeleteOperation(t *testing.T) {
	original := NewDeleteEntry("test-key")

	if original.Operation != OpDelete {
		t.Errorf("Expected OpDelete, got %d", original.Operation)
	}

	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeEntry(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Operation != OpDelete {
		t.Errorf("Operation mismatch: got %d, want %d", decoded.Operation, OpDelete)
	}

	if decoded.Key != original.Key {
		t.Errorf("Key mismatch: got %q, want %q", decoded.Key, original.Key)
	}

	if decoded.Value != nil && len(decoded.Value) != 0 {
		t.Errorf("Expected nil/empty value for delete, got %q", decoded.Value)
	}
}

func TestEntryLargeKey(t *testing.T) {
	largeKey := strings.Repeat("k", 10000)
	original := NewSetEntry(largeKey, []byte("value"))

	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeEntry(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Key != largeKey {
		t.Errorf("Key length mismatch: got %d, want %d", len(decoded.Key), len(largeKey))
	}
}

func TestEntryLargeValue(t *testing.T) {
	largeValue := bytes.Repeat([]byte("v"), 100000)
	original := NewSetEntry("key", largeValue)

	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeEntry(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if !bytes.Equal(decoded.Value, largeValue) {
		t.Errorf("Value length mismatch: got %d, want %d", len(decoded.Value), len(largeValue))
	}
}

func TestEntryBinaryValue(t *testing.T) {
	binaryValue := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
	original := NewSetEntry("binary-key", binaryValue)

	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeEntry(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if !bytes.Equal(decoded.Value, binaryValue) {
		t.Errorf("Binary value mismatch: got %v, want %v", decoded.Value, binaryValue)
	}
}

func TestEntryUnicodeKey(t *testing.T) {
	unicodeKey := "测试-🔑-тест-مفتاح"
	original := NewSetEntry(unicodeKey, []byte("value"))

	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeEntry(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Key != unicodeKey {
		t.Errorf("Unicode key mismatch: got %q, want %q", decoded.Key, unicodeKey)
	}
}

func TestEntryCorruptedData(t *testing.T) {
	original := NewSetEntry("test-key", []byte("test-value"))

	var buf bytes.Buffer
	if err := original.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Truncate buffer to simulate corruption
	truncated := bytes.NewReader(buf.Bytes()[:10])

	_, err := DecodeEntry(truncated)
	if err == nil {
		t.Error("Expected error for corrupted data, got nil")
	}
}

func TestEntryEmptyBuffer(t *testing.T) {
	var buf bytes.Buffer

	_, err := DecodeEntry(&buf)
	if err == nil {
		t.Error("Expected error for empty buffer, got nil")
	}
}

func TestEntryMultipleEncodeDecode(t *testing.T) {
	entries := []*Entry{
		NewSetEntry("key1", []byte("value1")),
		NewSetEntry("key2", []byte("value2")),
		NewDeleteEntry("key3"),
		NewSetEntry("key4", nil),
	}

	var buf bytes.Buffer

	// Encode all entries
	for _, entry := range entries {
		if err := entry.Encode(&buf); err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
	}

	// Decode all entries
	for i := 0; i < len(entries); i++ {
		decoded, err := DecodeEntry(&buf)
		if err != nil {
			t.Fatalf("Decode entry %d failed: %v", i, err)
		}

		if decoded.Key != entries[i].Key {
			t.Errorf("Entry %d: Key mismatch: got %q, want %q", i, decoded.Key, entries[i].Key)
		}

		if decoded.Operation != entries[i].Operation {
			t.Errorf("Entry %d: Operation mismatch: got %d, want %d", i, decoded.Operation, entries[i].Operation)
		}
	}
}
