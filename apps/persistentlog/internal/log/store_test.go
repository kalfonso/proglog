package log

import (
	"github.com/pkg/errors"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendOnEmptyStore(t *testing.T) {
	store, err := newTestStore()
	require.NoError(t, err)
	message := "payment received"
	n, pos, err := store.Append([]byte(message))
	require.NoError(t, err)
	require.Equal(t, n, uint64(len(message))+uint64(lenWidth))
	require.Equal(t, pos, uint64(0))
}

func TestAppendAndRead(t *testing.T) {
	store, err := newTestStore()
	require.NoError(t, err)
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
	_, err = store.Read(0)
	require.EqualError(t, err, NewErrorOffsetNotFound(0).Error())
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
