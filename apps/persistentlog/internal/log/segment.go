package log

import (
	"fmt"
	api "github.com/kalfonso/proglog/apps/persistentlog/api/v1"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"os"
	"path"
)

// segment defines a segment of the log.
// A segment has a store and index. The index provides a more efficient way to index
// the data within the store by using a relative base offset. As log can grow to a large size,
// using base offset for a segment whose size is not as large as the log, allows to have a more
// compact index structure by using 32 bits integer to represent the offset instead of
// 64 bits to represent the offset in a large log.
type segment struct {
	store *store
	index *index
	baseOffset uint64
	nextOffset uint64
	config Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	store, err := createStore(dir, baseOffset, c)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	index, err := createIndex(dir, baseOffset, c)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	segment := &segment{
		store:      store,
		index:      index,
		baseOffset: baseOffset,
		config:     c,
	}
	off, _, err := index.Read(-1)
	if err != nil {
		segment.nextOffset = baseOffset
	} else {
		segment.nextOffset = baseOffset + uint64(off) + 1
	}
	return segment, nil
}

func createStore(dir string, offset uint64, c Config) (*store, error) {
	fileName := path.Join(dir, fmt.Sprintf("%d.store", offset))
	storeFile, err := os.OpenFile(fileName,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	store, err := newStore(storeFile, c)
	return store, errors.WithStack(err)
}

func createIndex(dir string, offset uint64, c Config) (*index, error) {
	fileName := path.Join(dir, fmt.Sprintf("%d.index", offset))
	indexFile, err := os.OpenFile(fileName,
		os.O_RDWR | os.O_CREATE,
		0644)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	index, err := newIndex(indexFile, c)
	return index, errors.WithStack(err)
}

// Append appends a record to the segment and returns the offset of the record on this segment.
func (s *segment) Append (record *api.Record) (offset uint64, err error) {
	currOffset := s.nextOffset
	record.Offset = currOffset
	recordBytes, err := proto.Marshal(record)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	_, pos, err := s.store.Append(recordBytes)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	// index offset is relative to the base offset. base offset and next offset are both absolute offsets.
	err = s.index.Write(uint32(s.nextOffset - s.baseOffset), pos)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	s.nextOffset++
	return currOffset, errors.WithStack(err)
}

// Read reads the record at the given absolute offset.
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	recordBytes, err := s.store.Read(pos)
	record := &api.Record{}
	err = proto.Unmarshal(recordBytes, record)
	return record, errors.WithStack(err)
}

// IsMaxed returns whether the segment has reached its maximum size.
// This is used by the log to determine whether it needs to create a new segment.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Close closes the segment. It will close the underlying store and index.
func (s *segment) Close() error {
	err := s.index.Close()
	if err != nil {
		return errors.WithStack(err)
	}
	err = s.store.Close()
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Remove closes this segment and removes its underlying store and index files.
func (s *segment) Remove() error {
	err := s.Close()
	if err != nil {
		return errors.WithStack(err)
	}
	err = os.Remove(s.index.Name())
	if err != nil {
		return errors.WithStack(err)
	}
	err = os.Remove(s.store.Name())
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}