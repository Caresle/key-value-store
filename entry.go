package kvstore

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	OpSet    byte = 0x01
	OpDelete byte = 0x02
)

const EntryMagic uint32 = 0x4B564C47 // "KVLG"

type Entry struct {
	Operation byte
	Timestamp int64
	Key       string
	Value     []byte
}

func NewSetEntry(key string, value []byte) *Entry {
	return &Entry{
		Operation: OpSet,
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
	}
}

func NewDeleteEntry(key string) *Entry {
	return &Entry{
		Operation: OpDelete,
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     nil,
	}
}

func (e *Entry) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, EntryMagic); err != nil {
		return fmt.Errorf("failed to write magic: %w", err)
	}

	if err := binary.Write(w, binary.BigEndian, e.Operation); err != nil {
		return fmt.Errorf("failed to write operation: %w", err)
	}

	if err := binary.Write(w, binary.BigEndian, e.Timestamp); err != nil {
		return fmt.Errorf("failed to write timestamp: %w", err)
	}

	keyBytes := []byte(e.Key)
	keyLen := uint32(len(e.Key))
	if err := binary.Write(w, binary.BigEndian, keyLen); err != nil {
		return fmt.Errorf("failed to write key length: %w", err)
	}

	if _, err := w.Write(keyBytes); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}

	valueLen := uint32(len(e.Value))
	if err := binary.Write(w, binary.BigEndian, valueLen); err != nil {
		return fmt.Errorf("failed to write value length: %w", err)
	}

	if valueLen > 0 {
		if _, err := w.Write(e.Value); err != nil {
			return fmt.Errorf("failed to write value: %w", err)
		}
	}

	return nil
}

func DecodeEntry(r io.Reader) (*Entry, error) {
	var magic uint32

	if err := binary.Read(r, binary.BigEndian, &magic); err != nil {
		return nil, fmt.Errorf("failed to read magic: %w", err)
	}

	if magic != EntryMagic {
		return nil, fmt.Errorf("invalid magic")
	}

	var operation byte
	if err := binary.Read(r, binary.BigEndian, &operation); err != nil {
		return nil, fmt.Errorf("failed to read operation: %w", err)
	}

	var timestamp int64
	if err := binary.Read(r, binary.BigEndian, &timestamp); err != nil {
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}

	var keyLen uint32
	if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
		return nil, fmt.Errorf("failed to read key length: %w", err)
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, fmt.Errorf("failed to read key: %w", err)
	}

	var valueLen uint32
	if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
		return nil, fmt.Errorf("failed to read value length: %w", err)
	}

	value := make([]byte, valueLen)
	if valueLen > 0 {
		if _, err := io.ReadFull(r, value); err != nil {
			return nil, fmt.Errorf("failed to read value: %w", err)
		}
	}

	return &Entry{
		Operation: operation,
		Timestamp: timestamp,
		Key:       string(key),
		Value:     value,
	}, nil
}
