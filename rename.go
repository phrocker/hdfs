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
