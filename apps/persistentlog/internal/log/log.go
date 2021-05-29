package log

import (
	"fmt"
	"github.com/pkg/errors"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Log represents a commit log.
// It consists of a list of segments with an active segment to append the writes to.
// The segments are located in a directory.
type Log struct {
	mu sync.RWMutex

	Dir string
	Config Config

	activeSegment *segment
	segments []*segment
}

// setup Sets up the log. At start, the log must take into account the existing segments on disk or
// create the initial segment for the very first time.
func (l *Log) setup() error {
	// Read all existing segments in the directory and get the base offset
	// Active segment is the segment with the greatest offset.
	var segments []*segment
	offsets, err := l.getOffsets()
	if err != nil {
		return errors.WithStack(err)
	}
	if len(offsets) == 0 {
		segment, err := newSegment(l.Dir, 0, l.Config)
		if err != nil {
			return errors.WithStack(err)
		}
		l.activeSegment = segment
		return nil
	}
	// Sort them in reverse order
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] > offsets[j]
	})
	segment, err := newSegment(l.Dir, offsets[0], l.Config)
	if err != nil {
		return errors.WithStack(err)
	}
	l.activeSegment = segment
	for _, offset := range offsets[1:] {
		segment, err := newSegment(l.Dir, offset, l.Config)
		if err != nil {
			return errors.WithStack(err)
		}
		segments = append(segments, segment)
	}
	return nil
}

// getOffsets returns the current segment offsets in the log directory
func (l *Log) getOffsets() ([]uint64, error) {
	var offsets []uint64
	storeFiles, err := filepath.Glob(fmt.Sprintf("%s/*.store", l.Dir))
	if err != nil {
		return offsets, errors.WithStack(err)
	}
	for _, storeFile := range storeFiles {
		fileName := filepath.Base(storeFile)
		offStr := strings.TrimSuffix(fileName, filepath.Ext(fileName))
		off, err := strconv.ParseUint(offStr, 10, 32)
		if err != nil {
			return []uint64{}, errors.WithStack(err)
		}
		offsets = append(offsets, off)
	}
	return offsets, nil
}

// NewLog creates a new log from the configuration.
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir: dir,
		Config: c,
	}
	return l, l.setup()
}
