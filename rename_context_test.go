package hdfs

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenameContext(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tomovectx")
	baleet(t, "/_test/tomovedestctx")

	err := client.RenameContext(context.Background(), "/_test/tomovectx", "/_test/tomovedestctx")
	require.NoError(t, err)

	fi, err := client.Stat("/_test/tomovectx")
	assert.Nil(t, fi)
	assertPathError(t, err, "stat", "/_test/tomovectx", os.ErrNotExist)

	fi, err = client.Stat("/_test/tomovedestctx")
	require.NoError(t, err)
}
