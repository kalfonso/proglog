package server

import (
	"fmt"
	"sync"
)

// Record is an external view of an internal record from the log
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

type internalRecord struct {
	value  []byte
	offset uint64
}

// Log represents the commit log
type Log struct {
	mu      sync.Mutex
	records []internalRecord
}

// NewLog creates a new instance of the commit log
func NewLog() *Log {
	return &Log{}
}

// Append appends a value to the log. Values are array of bytes
func (l *Log) Append(value []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	offset := uint64(len(l.records))
	record := internalRecord{
		value:  value,
		offset: offset,
	}
	l.records = append(l.records, record)
	return offset, nil
}

// Read returns the record at the given offset or error if offset does not exist
func (l *Log) Read(offset uint64) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	maxOffset := uint64(len(l.records))
	if offset > maxOffset {
		return Record{}, errOffsetNotFound(offset)
	}
	rec := l.records[offset]
	record := Record{
		Value:  rec.value,
		Offset: rec.offset,
	}
	return record, nil
}

func errOffsetNotFound(offset uint64) error {
	return fmt.Errorf("offet %d not found", offset)
}
