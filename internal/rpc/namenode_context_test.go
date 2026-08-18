package rpc

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestExecuteContextBoundsBlockedResponse(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	lock := make(chan struct{}, 1)
	lock <- struct{}{}
	host := &namenodeHost{address: "namenode"}
	conn := &NamenodeConnection{
		conn:      client,
		host:      host,
		hostList:  []*namenodeHost{host},
		transport: blockingResponseTransport{},
		reqLock:   lock,
		done:      make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := conn.ExecuteContext(ctx, "complete", nil, nil)
	if err != context.DeadlineExceeded {
		t.Fatalf("ExecuteContext error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("ExecuteContext took %v; blocked response was not interrupted", elapsed)
	}
}

func TestExecuteContextBoundsRequestLockWait(t *testing.T) {
	lock := make(chan struct{}, 1)
	conn := &NamenodeConnection{reqLock: lock}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	err := conn.ExecuteContext(ctx, "complete", nil, nil)
	if err != context.DeadlineExceeded {
		t.Fatalf("ExecuteContext error = %v, want context deadline exceeded", err)
	}
}

type blockingResponseTransport struct{}

func (blockingResponseTransport) writeRequest(io.Writer, string, int32, proto.Message) error {
	return nil
}

func (blockingResponseTransport) readResponse(r io.Reader, _ string, _ int32, _ proto.Message) error {
	var b [1]byte
	_, err := r.Read(b[:])
	return err
}
