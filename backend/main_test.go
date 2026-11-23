package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWebSocketJoinBroadcastLeave(t *testing.T) {
	hub = NewHub()
	go hub.Run()
	server := httptest.NewServer(http.HandlerFunc(handleWebSocket))
	defer server.Close()
	connect := func(name string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/?username="+name, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	readUntil := func(conn *websocket.Conn, expected string) {
		t.Helper()
		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		for {
			var message Message
			if err := conn.ReadJSON(&message); err != nil {
				t.Fatalf("waiting for %q: %v", expected, err)
			}
			if message.Content == expected {
				return
			}
		}
	}
	alice := connect("alice")
	readUntil(alice, "欢迎 alice 加入聊天室！")
	bob := connect("bob")
	readUntil(bob, "欢迎 bob 加入聊天室！")
	if err := alice.WriteJSON(Message{Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	readUntil(bob, "hello")
	alice.Close()
	readUntil(bob, "alice 已离开聊天室")
	if err := bob.WriteJSON(Message{Content: "still running"}); err != nil {
		t.Fatal(err)
	}
	readUntil(bob, "still running")
}
