package goredis

import (
	"bufio"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestReconnectAfterWrappedConnectionError(t *testing.T) {
	for _, cause := range []error{syscall.EPIPE, syscall.ECONNRESET} {
		t.Run(cause.Error(), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			serverResult := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					serverResult <- err
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(2 * time.Second))
				_, err = readResponse(bufio.NewReader(conn))
				if err == nil {
					_, err = io.WriteString(conn, ":1\r\n")
				}
				serverResult <- err
			}()

			client := NewClient(1)
			client.Addr = listener.Addr().String()
			stale := &brokenConnection{cause: cause}
			<-client.pool
			client.pool <- stale
			count, err := client.Incr("test-key")
			if err != nil || count != 1 {
				t.Fatalf("INCR after disconnect = %d, %v", count, err)
			}
			if !stale.closed {
				t.Fatal("disconnected socket was not closed")
			}
			conn := <-client.pool
			if conn == nil || conn == stale {
				t.Fatal("pool did not keep the replacement connection")
			}
			conn.Close()
			if err := <-serverResult; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReconnectFailureDoesNotKeepDisconnectedSocket(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = readResponse(bufio.NewReader(conn))
		conn.Close() // Redis disconnects again before returning the command result.
	}()

	client := NewClient(1)
	client.Addr = listener.Addr().String()
	<-client.pool
	client.pool <- &brokenConnection{cause: syscall.EPIPE}
	if _, err := client.Incr("test-key"); !errors.Is(err, io.EOF) {
		t.Fatalf("expected disconnect error, got %v", err)
	}
	if conn := <-client.pool; conn != nil {
		conn.Close()
		t.Fatal("disconnected replacement socket was returned to the pool")
	}
}

type brokenConnection struct {
	cause  error
	closed bool
}

func (conn *brokenConnection) Read([]byte) (int, error) { return 0, io.EOF }
func (conn *brokenConnection) Write([]byte) (int, error) {
	return 0, &net.OpError{Op: "write", Net: "tcp", Err: &os.SyscallError{Syscall: "write", Err: conn.cause}}
}
func (conn *brokenConnection) Close() error                     { conn.closed = true; return nil }
func (conn *brokenConnection) LocalAddr() net.Addr              { return nil }
func (conn *brokenConnection) RemoteAddr() net.Addr             { return nil }
func (conn *brokenConnection) SetDeadline(time.Time) error      { return nil }
func (conn *brokenConnection) SetReadDeadline(time.Time) error  { return nil }
func (conn *brokenConnection) SetWriteDeadline(time.Time) error { return nil }
