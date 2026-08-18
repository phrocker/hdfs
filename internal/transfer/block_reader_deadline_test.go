package transfer

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
)

func TestBlockReaderAppliesDeadlineBeforeHandshake(t *testing.T) {
	conn := &deadlineOrderConn{}
	deadline := time.Now().Add(time.Minute)
	var dialDeadline time.Time
	reader := &BlockReader{
		Block:     &hdfs.LocatedBlockProto{},
		deadline:  deadline,
		datanodes: newDatanodeFailover([]string{"datanode"}),
		DialFunc: func(ctx context.Context, _, _ string) (net.Conn, error) {
			dialDeadline, _ = ctx.Deadline()
			return conn, nil
		},
	}

	err := reader.connectNext()
	if !errors.Is(err, errDeadlineProbe) {
		t.Fatalf("connectNext error = %v, want %v", err, errDeadlineProbe)
	}
	if !conn.deadline.Equal(deadline) {
		t.Fatalf("deadline at handshake = %v, want %v", conn.deadline, deadline)
	}
	if !dialDeadline.Equal(deadline) {
		t.Fatalf("dial deadline = %v, want %v", dialDeadline, deadline)
	}
}

var errDeadlineProbe = errors.New("deadline probe")

type deadlineOrderConn struct {
	deadline time.Time
}

func (c *deadlineOrderConn) Read([]byte) (int, error) { return 0, errors.New("unexpected handshake") }
func (c *deadlineOrderConn) Write([]byte) (int, error) {
	return 0, errors.New("unexpected handshake")
}
func (c *deadlineOrderConn) Close() error         { return nil }
func (c *deadlineOrderConn) LocalAddr() net.Addr  { return nil }
func (c *deadlineOrderConn) RemoteAddr() net.Addr { return nil }
func (c *deadlineOrderConn) SetDeadline(t time.Time) error {
	c.deadline = t
	return errDeadlineProbe
}
func (c *deadlineOrderConn) SetReadDeadline(time.Time) error  { return nil }
func (c *deadlineOrderConn) SetWriteDeadline(time.Time) error { return nil }
