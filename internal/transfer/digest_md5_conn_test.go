package transfer

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestDigestMD5ConnBufferedRead(t *testing.T) {
	macKey := []byte("integrity-key")
	cipherKey := []byte("0123456789abcdef")
	tests := []struct {
		name string
		wrap func(net.Conn, net.Conn) (net.Conn, net.Conn)
	}{
		{
			name: "integrity",
			wrap: func(sender, receiver net.Conn) (net.Conn, net.Conn) {
				return newDigestMD5IntegrityConn(sender, macKey, macKey),
					newDigestMD5IntegrityConn(receiver, macKey, macKey)
			},
		},
		{
			name: "privacy",
			wrap: func(sender, receiver net.Conn) (net.Conn, net.Conn) {
				return newDigestMD5PrivacyConn(sender, macKey, macKey, cipherKey, cipherKey),
					newDigestMD5PrivacyConn(receiver, macKey, macKey, cipherKey, cipherKey)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			senderRaw, receiverRaw := net.Pipe()
			defer senderRaw.Close()
			defer receiverRaw.Close()
			sender, receiver := tt.wrap(senderRaw, receiverRaw)
			if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			written := make(chan error, 1)
			go func() {
				n, err := sender.Write([]byte("abc"))
				if err == nil && n != 3 {
					err = fmt.Errorf("Write reported %d bytes, want 3", n)
				}
				written <- err
			}()

			first := make([]byte, 1)
			if n, err := receiver.Read(first); n != 1 || err != nil || string(first) != "a" {
				t.Fatalf("first Read = %q, %d, %v", first, n, err)
			}
			rest := make([]byte, 10)
			if n, err := receiver.Read(rest); n != 2 || err != nil || string(rest[:n]) != "bc" {
				t.Fatalf("buffered Read = %q, %d, %v", rest[:n], n, err)
			}
			if err := <-written; err != nil {
				t.Fatalf("Write: %v", err)
			}
		})
	}
}
