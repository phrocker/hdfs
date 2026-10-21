package hdfs

import (
	"context"
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

// Rename renames (moves) a file.
func (c *Client) Rename(oldpath, newpath string) error {
	return c.RenameContext(context.Background(), oldpath, newpath)
}

// RenameContext renames (moves) a file, bounded by ctx.
func (c *Client) RenameContext(ctx context.Context, oldpath, newpath string) error {
	_, err := c.getFileInfoContext(ctx, newpath)
	err = interpretException(err)
	if err != nil && !os.IsNotExist(err) {
		return &os.PathError{"rename", newpath, err}
	}

	req := &hdfs.Rename2RequestProto{
		Src:           proto.String(oldpath),
		Dst:           proto.String(newpath),
		OverwriteDest: proto.Bool(true),
	}
	resp := &hdfs.Rename2ResponseProto{}

	err = c.namenode.ExecuteContext(ctx, "rename2", req, resp)
	if err != nil {
		return &os.PathError{"rename", oldpath, interpretException(err)}
	}

	return nil
}

// RenameNoOverwriteContext atomically moves src to dst only while dst is
// absent. The namenode enforces the condition in rename2; a separate Stat
// check would race with another writer and must not be used as the fence.
func (c *Client) RenameNoOverwriteContext(ctx context.Context, oldpath, newpath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	req := &hdfs.Rename2RequestProto{
		Src: proto.String(oldpath), Dst: proto.String(newpath),
		OverwriteDest: proto.Bool(false),
	}
	resp := &hdfs.Rename2ResponseProto{}
	if err := c.namenode.ExecuteContext(ctx, "rename2", req, resp); err != nil {
		return &os.PathError{Op: "rename", Path: newpath, Err: interpretException(err)}
	}
	return nil
}
