package log

import (
	"bufio"
	"encoding/binary"
	"github.com/pkg/errors"
	"os"
	"sync"
)

const (
	lenWidth = 8 // number of bites used to represent the record length
)

var (
	enc = binary.BigEndian
)

// store is the persistent file where the log records are kept.
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	// get the store file size in case the store is being re-created after a crash or restart
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		mu:   sync.Mutex{},
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}

// Append appends a record to the store.
func (s *store) Append(r []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	// write the length of the record
	err = binary.Write(s.buf, enc, uint64(len(r)))
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	w, err := s.buf.Write(r)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	w += lenWidth // number of bytes written
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read reads the record at the given position.
// It flushes the writer buffer in case it's reading a record that hasn't been flushed yet.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return []byte{}, errors.WithStack(err)
	}
	// read the length of the record we're about to read
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return []byte{}, errors.WithStack(err)
	}
	// read the record's byte now that we have its size
	r := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(r, int64(pos + lenWidth)); err != nil {
		return []byte{}, errors.WithStack(err)
	}
	return r, nil
}

// ReadAt reads len(r) at offset off, flushing the underlying buffer first.
func (s *store) ReadAt(r []byte, off uint64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, errors.WithStack(err)
	}
	n, err := s.ReadAt(r, off)
	return n, errors.WithStack(err)
}

// Close closes the store. It flushes it first before proceeding.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(s.File.Close())
}
