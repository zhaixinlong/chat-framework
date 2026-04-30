package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "grpc-backend/pb"

	grpcweb "github.com/improbable-eng/grpc-web/go/grpcweb"
	"google.golang.org/grpc"
)

type Client struct {
	send     chan *pb.ChatMessage
	username string
}

type Hub struct {
	clients map[string][]*Client
	mu      sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{clients: make(map[string][]*Client)}
}

func (h *Hub) Register(username string, send chan *pb.ChatMessage) *Client {
	c := &Client{send: send, username: username}
	h.mu.Lock()
	h.clients[username] = append(h.clients[username], c)
	h.mu.Unlock()

	h.broadcast(&pb.ChatMessage{
		Type:    "system",
		Content: fmt.Sprintf("欢迎 %s 加入聊天室！", username),
		Time:    time.Now().Format("15:04:05"),
	})
	return c
}

func (h *Hub) Unregister(username string, c *Client) {
	h.mu.Lock()
	clients := h.clients[username]
	for i, cl := range clients {
		if cl == c {
			h.clients[username] = append(clients[:i], clients[i+1:]...)
			break
		}
	}
	h.mu.Unlock()

	h.broadcast(&pb.ChatMessage{
		Type:    "system",
		Content: fmt.Sprintf("%s 已离开聊天室", username),
		Time:    time.Now().Format("15:04:05"),
	})
}

func (h *Hub) broadcast(msg *pb.ChatMessage) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, clients := range h.clients {
		for _, c := range clients {
			select {
			case c.send <- msg:
			default:
			}
		}
	}
}

type ChatServer struct {
	pb.UnimplementedChatServiceServer
}

var globalHub *Hub

func (s *ChatServer) ChatStream(stream pb.ChatService_ChatStreamServer) error {
	var username string
	var client *Client

	// 读取第一条消息获取用户名
	firstMsg, err := stream.Recv()
	if err != nil {
		return err
	}
	username = firstMsg.Username
	if username == "" {
		username = fmt.Sprintf("用户%d", time.Now().UnixNano()%10000)
	}

	sendChan := make(chan *pb.ChatMessage, 100)
	client = globalHub.Register(username, sendChan)
	defer globalHub.Unregister(username, client)

	// 启动发送协程
	go func() {
		for msg := range sendChan {
			stream.Send(msg)
		}
	}()

	// 接收循环
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		msg.Type = "chat"
		msg.Username = username
		msg.Time = time.Now().Format("15:04:05")
		log.Printf("[%s] %s: %s", msg.Time, msg.Username, msg.Content)
		globalHub.broadcast(msg)
	}
}

func serveFrontend(w http.ResponseWriter, r *http.Request) {
	execPath, _ := os.Getwd()
	http.ServeFile(w, r, filepath.Join(execPath, "../grpc-frontend/index.html"))
}

// SSE 端点（浏览器接收消息）
func handleSSE(w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, "missing username", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	sendChan := make(chan *pb.ChatMessage, 100)
	client := &Client{send: sendChan, username: username}

	globalHub.mu.Lock()
	globalHub.clients[username] = append(globalHub.clients[username], client)
	globalHub.mu.Unlock()

	defer func() {
		globalHub.mu.Lock()
		clients := globalHub.clients[username]
		for i, c := range clients {
			if c == client {
				globalHub.clients[username] = append(clients[:i], clients[i+1:]...)
				break
			}
		}
		close(sendChan)
		globalHub.mu.Unlock()
	}()

	globalHub.broadcast(&pb.ChatMessage{
		Type:    "system",
		Content: fmt.Sprintf("欢迎 %s 加入聊天室！", username),
		Time:    time.Now().Format("15:04:05"),
	})

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-sendChan:
			data, _ := json.Marshal(msg)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func main() {
	globalHub = NewHub()
	chatServer := &ChatServer{}

	// 纯 gRPC 服务器（端口 50051，给 CLI 客户端用）
	go func() {
		grpcServer := grpc.NewServer()
		pb.RegisterChatServiceServer(grpcServer, chatServer)
		lis, _ := net.Listen("tcp", ":50051")
		log.Println("纯 gRPC 服务器启动: localhost:50051")
		grpcServer.Serve(lis)
	}()

	// gRPC-Web + HTTP 服务器（端口 8082，给浏览器用）
	wrappedServer := grpcweb.WrapServer(grpc.NewServer())
	mux := http.NewServeMux()
	mux.HandleFunc("/", serveFrontend)
	mux.HandleFunc("/chat.v1.ChatService/ChatStream", wrappedServer.ServeHTTP)
	mux.HandleFunc("/sse", handleSSE)

	// 兼容浏览器发送消息 (简单 HTTP POST)
	mux.HandleFunc("/api/message", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", 405)
			return
		}
		var req struct{ Username, Content string }
		json.NewDecoder(r.Body).Decode(&req)
		globalHub.broadcast(&pb.ChatMessage{
			Type: "chat", Username: req.Username, Content: req.Content,
			Time: time.Now().Format("15:04:05"),
		})
		w.Write([]byte(`{"status":"ok"}`))
	})

	log.Println("gRPC-Web 服务器启动: http://localhost:8082")
	log.Println("前端地址: http://localhost:8082")
	log.Println("按 Ctrl+C 停止服务器")

	if err := http.ListenAndServe(":8082", mux); err != nil {
		log.Fatal("服务器启动失败: ", err)
	}
}
