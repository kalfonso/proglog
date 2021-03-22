package log

import (
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestAppendOnEmptyStore(t *testing.T) {
	store, err := newTestStore()
	require.NoError(t, err)
	defer os.Remove(store.File.Name())
	message := "payment received"
	n, pos, err := store.Append([]byte(message))
	require.NoError(t, err)
	require.Equal(t, n, uint64(len(message))+uint64(lenWidth))
	require.Equal(t, pos, uint64(0))
}

func TestAppendAndRead(t *testing.T) {
	store, err := newTestStore()
	require.NoError(t, err)
	defer os.Remove(store.File.Name())
	message := []byte("hello world")
	_, pos, err := store.Append(message)
	require.NoError(t, err)
	record, err := store.Read(pos)
	require.NoError(t, err)
	require.Equal(t, record, message)
}

func TestReadEmptyStore(t *testing.T) {
	store, err := newTestStore()
	require.NoError(t, err)
	defer os.Remove(store.File.Name())
	_, err = store.Read(0)
	require.EqualError(t, err, NewErrorOffsetNotFound(0).Error())
}

func TestReadAt(t *testing.T) {
	store, err := newTestStore()
	require.NoError(t, err)
	defer os.Remove(store.File.Name())

	rand.Seed(time.Now().UTC().UnixNano())
	messages := generateRandomMessages(5)
	offsets := make([]uint64, 0, len(messages))
	for _, message := range messages {
		_, pos, err := store.Append(message)
		require.NoError(t, err)
		offsets = append(offsets, pos)
	}
	index := rand.Int63n(int64(len(offsets) - 1))
	message := messages[index]
	offset := int64(offsets[index] + lenWidth)
	storedMessage := make([]byte, len(message))
	_, err = store.ReadAt(storedMessage, offset)
	require.NoError(t, err)
	require.EqualValues(t, message, storedMessage)
}

func newTestStore() (*store, error) {
	f, err := ioutil.TempFile("", "store_test")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	store, err := newStore(f)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return store, err
}

func generateRandomMessages(n int) [][]byte {
	var r []byte
	var records [][]byte
	for i := 0; i < n; i++ {
		len := rand.Int63n(9) + 1
		r = make([]byte, len)
		rand.Read(r)
		records = append(records, r)
	}
	return records
}
