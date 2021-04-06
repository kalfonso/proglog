package log

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	index, err := createTestIndex()
	defer os.Remove(index.file.Name())
	_, _, err = index.Read(-1)
	require.Error(t, err)
	require.EqualValues(t, index.file.Name(), index.Name())

	entries := []struct {
		off uint32
		pos uint64
	}{
		{off: 0, pos: 0},
		{off: 1, pos: 10},
	}

	for _, e := range entries {
		err := index.Write(e.off, e.pos)
		require.NoError(t, err)

		_, pos, err := index.Read(int64(e.off))
		require.NoError(t, err)
		require.Equal(t, e.pos, pos)
	}
}

func TestReadingPastExistingEntries(t *testing.T) {
	index, err := createTestIndex()
	require.NoError(t, err)
	defer os.Remove(index.file.Name())

	entries := []struct {
		off uint32
		pos uint64
	}{
		{off: 0, pos: 0},
		{off: 1, pos: 10},
	}
	for _, e := range entries {
		err := index.Write(e.off, e.pos)
		require.NoError(t, err)
	}
	_, _, err = index.Read(int64(len(entries)))
	require.ErrorIs(t, err, io.EOF)
}

func TestIndexBuildStateFromExistingFile(t *testing.T)  {
	index, err := createTestIndex()
	require.NoError(t, err)
	defer os.Remove(index.file.Name())

	entries := []struct {
		off uint32
		pos uint64
	}{
		{off: 0, pos: 0},
		{off: 1, pos: 10},
	}
	for _, e := range entries {
		err := index.Write(e.off, e.pos)
		require.NoError(t, err)
	}
	err = index.Close()
	require.NoError(t, err)

	f, _ := os.OpenFile(index.file.Name(), os.O_RDWR, 0600)
	index, err = newIndex(f, Config{
		Segment: Segment{
			MaxIndexBytes: 1024,
		},
	})
	for _, e := range entries {
		_, pos, err := index.Read(int64(e.off))
		require.NoError(t, err)
		require.Equal(t, e.pos, pos)
	}
}

func createTestIndex() (*index, error) {
	file, err := ioutil.TempFile(os.TempDir(), "log_index")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	index, err := newIndex(file, Config{
		Segment: Segment{
			MaxIndexBytes: 1024,
		},
	})
	return index, errors.WithStack(err)
}
