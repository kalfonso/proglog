package log

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendOnEmptyStore(t *testing.T) {
	f, err := ioutil.TempFile("", "store_test")
	require.NoError(t, err)
	store, err := newStore(f)
	require.NoError(t, err)
	message := "payment received"
	n, pos, err := store.Append([]byte(message))
	require.NoError(t, err)
	require.Equal(t, n, uint64(len(message)) + uint64(lenWidth))
	require.Equal(t, pos, uint64(0))
}
