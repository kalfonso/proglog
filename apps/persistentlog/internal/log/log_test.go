package log

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestInitialSetup(t *testing.T) {
	dir, err := ioutil.TempDir("", "log_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	c := Config{}
	log, err := NewLog(dir, c)
	require.NoError(t, err)
	baseOffset, netxtOffset := uint64(0), uint64(0)
	require.Equal(t, baseOffset, log.activeSegment.baseOffset)
	require.Equal(t, netxtOffset, log.activeSegment.nextOffset)
	require.Empty(t, log.segments)
}

func TestExisting(t *testing.T) {
	dir, err := ioutil.TempDir("", "log_test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	c := Config{}
	_, err = newSegment(dir, 0, c)
	require.NoError(t, err)
	_, err = newSegment(dir, 1, c)
	require.NoError(t, err)

	log, err := NewLog(dir, c)
	require.NoError(t, err)
	baseOffset, netxtOffset := uint64(1), uint64(1)
	require.Equal(t, baseOffset, log.activeSegment.baseOffset)
	require.Equal(t, netxtOffset, log.activeSegment.nextOffset)
	require.Empty(t, log.segments)
}
