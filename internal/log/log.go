package log

import (
	"fmt"
)

type Log interface {
	Append(record Record) (uint64, error)
	Read(offset uint64) (Record, error)
}

// Record represents a record in the write-ahead log
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

//ErrorOffsetNotFound defines an error for an invalid offset.
type ErrorOffsetNotFound struct {
	offset uint64
}

//NewErrorOffsetNotFound creates an ErrorOffsetNotFound.
func NewErrorOffsetNotFound(offset uint64) *ErrorOffsetNotFound {
	return &ErrorOffsetNotFound{offset: offset}
}

func (e *ErrorOffsetNotFound) Error() string {
	return fmt.Sprintf("offset not found: %d", e.offset)
}

