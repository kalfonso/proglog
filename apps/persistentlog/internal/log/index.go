package log

import (
	"github.com/pkg/errors"
	"github.com/tysontate/gommap"
	"io"
	"math"
	"os"
)

var (
	// *Width constants defines the number of bytes to encode the index entry.
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth = offWidth + posWidth
)

// index defines a log index mapping record offset to record position in the log.
// The index entry contains two fields: the record offset and its position in the log file.
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// newIndex creates the index with size c.Segment.MaxIndexBytes before memory-mapping the file.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	idx.size = uint64(fi.Size())
	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ | gommap.PROT_WRITE,
		gommap.MAP_SHARED,
		)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return idx, nil
}

// Close releases the memory before closing the index.
func (i *index) Close() error {
	// flush any changes in memory back to the file.
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return errors.WithStack(err)
	}
	// flush data in file system page cache back to disk.
	if err := i.file.Sync(); err != nil {
		return errors.WithStack(err)
	}
	// truncate the file to the actual data that it's in the index.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(i.file.Close())
}

// Read returns the position of the record in the store given the offset.
// The offset is relative to the segment's base offset.
// For more information read Kafka Storage Internals: https://thehoard.blog/how-kafkas-storage-internals-work-3a29b02e026
func (i *index) Read(offset int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if offset > math.MaxUint32 {
		return 0, 0, errors.Errorf("Offset greater than max offset value %d", math.MaxUint32)
	}
	var indexOffset uint32
	if offset == -1 {
		indexOffset = uint32((i.size / entWidth) - 1)
	} else {
		indexOffset = uint32(offset)
	}
	pos = uint64(indexOffset) * entWidth
	if i.size < pos + entWidth {
		return 0, 0, errors.WithStack(io.EOF)
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends the given position and offset to the index.
// It validates there's space to write the entry in the memory region.
func (i *index) Write(off uint32, pos uint64) error {
	if i.size + entWidth > uint64(len(i.mmap)) {
		return errors.WithStack(io.EOF)
	}
	enc.PutUint32(i.mmap[i.size : i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth : i.size+entWidth], pos)
	i.size += entWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}