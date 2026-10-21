package hdfs

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRename(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tomove")
	baleet(t, "/_test/tomovedest")

	err := client.Rename("/_test/tomove", "/_test/tomovedest")
	require.NoError(t, err)

	fi, err := client.Stat("/_test/tomove")
	assert.Nil(t, fi)
	assertPathError(t, err, "stat", "/_test/tomove", os.ErrNotExist)

	fi, err = client.Stat("/_test/tomovedest")
	require.NoError(t, err)
}

func TestRenameNoOverwriteContext(t *testing.T) {
	client := getClient(t)
	const source = "/_test/no-overwrite-src"
	const target = "/_test/no-overwrite-dst"
	baleet(t, source)
	baleet(t, target)
	touch(t, source)
	if err := client.RenameNoOverwriteContext(context.Background(), source, target); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Stat(target); err != nil {
		t.Fatalf("target after rename: %v", err)
	}
	if _, err := client.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source after rename: %v", err)
	}
	touch(t, source)
	if err := client.RenameNoOverwriteContext(context.Background(), source, target); !errors.Is(err, os.ErrExist) {
		t.Fatalf("existing target overwritten: %v", err)
	}
	if _, err := client.Stat(source); err != nil {
		t.Fatalf("losing source removed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.RenameNoOverwriteContext(ctx, source, target); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled rename = %v", err)
	}
}

func TestRenameSrcNotExistent(t *testing.T) {
	client := getClient(t)

	baleet(t, "/_test/nonexistent")
	baleet(t, "/_test/nonexistent2")

	err := client.Rename("/_test/nonexistent", "/_test/nonexistent2")
	assertPathError(t, err, "rename", "/_test/nonexistent", os.ErrNotExist)
}

func TestRenameDestExists(t *testing.T) {
	client := getClient(t)

	touch(t, "/_test/tomove2")
	touch(t, "/_test/tomovedest2")

	err := client.Rename("/_test/tomove2", "/_test/tomovedest2")
	require.NoError(t, err)
}

func TestRenameWithoutPermissionForSrc(t *testing.T) {
	client2 := getClientForUser(t, "gohdfs2")

	mkdirpMask(t, "/_test/accessdenied", 0700)
	touchMask(t, "/_test/accessdenied/foo", 0600)

	err := client2.Rename("/_test/accessdenied/foo", "/_test/tomovedest3")
	assertPathError(t, err, "rename", "/_test/accessdenied/foo", os.ErrPermission)
}

func TestRenameWithoutPermissionForDest(t *testing.T) {
	client2 := getClientForUser(t, "gohdfs2")

	baleet(t, "/_test/ownedbyother2")

	err := client2.CreateEmptyFile("/_test/ownedbyother2")
	require.NoError(t, err)

	err = client2.Rename("/_test/ownedbyother2", "/_test/accessdenied/tomovedest4")
	assertPathError(t, err, "rename", "/_test/accessdenied/tomovedest4", os.ErrPermission)
}
