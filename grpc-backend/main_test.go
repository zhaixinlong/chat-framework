package main

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	pb "grpc-backend/pb"
)

func TestStreamBroadcastAndDisconnect(t *testing.T) {
	globalHub = NewHub()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterChatServiceServer(server, &ChatServer{})
	go server.Serve(listener)
	defer server.Stop()
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := pb.NewChatServiceClient(conn).ChatStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&pb.ChatMessage{Username: "alice"}); err != nil {
		t.Fatal(err)
	}
	welcome, err := stream.Recv()
	if err != nil || welcome.GetContent() != "欢迎 alice 加入聊天室！" {
		t.Fatalf("welcome: %v %v", welcome, err)
	}
	if err := stream.Send(&pb.ChatMessage{Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	message, err := stream.Recv()
	if err != nil || message.GetContent() != "hello" {
		t.Fatalf("message: %v %v", message, err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("stream should end cleanly: %v", err)
	}
	globalHub.mu.RLock()
	remaining := len(globalHub.clients)
	globalHub.mu.RUnlock()
	if remaining != 0 {
		t.Fatal("stream registration leaked")
	}
}

func TestUnregisterClosesOnlyMatchingQueue(t *testing.T) {
	h := NewHub()
	first := h.Register("same", make(chan *pb.ChatMessage, 100))
	second := h.Register("same", make(chan *pb.ChatMessage, 100))
	h.Unregister("same", second)
	h.Unregister("same", second)
	for range second.send {
	}
	h.broadcast(&pb.ChatMessage{Content: "hello"})
	found := false
	for len(first.send) > 0 {
		if (<-first.send).Content == "hello" {
			found = true
		}
	}
	if !found {
		t.Fatal("wrong connection removed")
	}
	h.Unregister("same", first)
	if len(h.clients) != 0 {
		t.Fatal("empty username entry leaked")
	}
}
