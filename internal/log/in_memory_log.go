package log

import "sync"

// InMemoryLog represents an in-memory write-ahead log
type InMemoryLog struct {
	mu      sync.Mutex
	records []Record
}

// NewInMemoryLog initializes a write-ahead log
func NewInMemoryLog() Log {
	return &InMemoryLog{}
}

// Append appends a record to the write-ahead log
func (l *InMemoryLog) Append(record Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	record.Offset = uint64(len(l.records))
	l.records = append(l.records, record)
	return record.Offset, nil
}

// Read returns the record at the provided offset or error if the offset is out of boundary.
func (l *InMemoryLog) Read(offset uint64) (Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset >= uint64(len(l.records)) {
		return Record{}, NewErrorOffsetNotFound(offset)
	}
	return l.records[offset], nil
}
