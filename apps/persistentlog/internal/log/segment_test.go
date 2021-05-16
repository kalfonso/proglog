package log

import (
	"github.com/golang/protobuf/proto"
	api "github.com/kalfonso/proglog/apps/persistentlog/api/v1"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

var (
	pool = "abcdefghijklmnopqrstuvwxyzABCEFGHIJKLMNOPQRSTUVWXYZ_-|?$%@][{}#&/()*"
	config = Config{
		Segment: Segment{
			MaxStoreBytes: 1024,
			MaxIndexBytes: 372,
			InitialOffset: 0,
		},
	}
)

func TestSegment_Append(t *testing.T) {
	s := createTestSegment(t)
	defer s.Remove()

	val := randomBytes(32)
	r := &api.Record{Value: val}
	offset, err := s.Append(r)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)
}

func TestSegment_AppendMultipleMessages(t *testing.T) {
	s := createTestSegment(t)
	defer s.Remove()

	i := 0
	for {
		val := randomBytes(32)
		r := &api.Record{Value: val}
		if s.store.size + uint64(lenWidth + proto.Size(r)) > s.config.Segment.MaxStoreBytes {
			break
		}
		offset, err := s.Append(r)
		require.NoError(t, err)
		require.Equal(t, uint64(i), offset)
		i++
	}
	val := randomBytes(32)
	r := &api.Record{Value: val}
	_, err := s.Append(r)
	require.EqualError(t, err, storeSizeMaxedError.Error())
}

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	baseOffset := uint64(16)
	s, err := newSegment(dir, baseOffset, c)
	require.NoError(t, err)
	require.Equal(t, baseOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		offset, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, baseOffset + i, offset)

		got, err := s.Read(offset)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	// Maxed index
	_, err = s.Append(want)
	require.ErrorIs(t, err, io.EOF)
	require.True(t, s.IsMaxed())
}

func createTestSegment(t *testing.T) *segment {
	t.Helper()
	dir := os.TempDir()
	s, err := newSegment(dir, 0, config)
	require.NoError(t, err)
	return s
}

func randomBytes(size int) []byte {
	rand.Seed(time.Now().UnixNano())
	bytes := make([]byte, size, size)
	for i := 0; i < size; i++ {
		bytes[i] = pool[rand.Intn(len(pool))]
	}
	return bytes
}
