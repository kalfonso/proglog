package log

import (
	api "github.com/kalfonso/proglog/apps/persistentlog/api/v1"
	"github.com/stretchr/testify/require"
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
	dir := os.TempDir()
	s, err := newSegment(dir, 0, config)
	require.NoError(t, err)
	defer s.Remove()

	val := randomBytes(32)
	r := &api.Record{ Value: val}
	offset, err := s.Append(r)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)
}

func randomBytes(size int) []byte {
	rand.Seed(time.Now().UnixNano())
	bytes := make([]byte, size, size)
	for i := 0; i < size; i++ {
		bytes[i] = pool[rand.Intn(len(pool))]
	}
	return bytes
}
