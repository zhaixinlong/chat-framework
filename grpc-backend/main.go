package main

import (
	"encoding/json"
	"fmt"
	"io"
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
	removed := false
	clients := h.clients[username]
	for i, current := range clients {
		if current == c {
			copy(clients[i:], clients[i+1:])
			clients[len(clients)-1] = nil
			clients = clients[:len(clients)-1]
			if len(clients) == 0 {
				delete(h.clients, username)
			} else {
				h.clients[username] = clients
			}
			close(c.send)
			removed = true
			break
		}
	}
	h.mu.Unlock()
	if removed {
		h.broadcast(&pb.ChatMessage{
			Type:    "system",
			Content: fmt.Sprintf("%s 已离开聊天室", username),
			Time:    time.Now().Format("15:04:05"),
		})
	}
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
	firstMsg, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	username := firstMsg.Username
	if username == "" {
		username = fmt.Sprintf("用户%d", time.Now().UnixNano()%10000)
	}
	sendChan := make(chan *pb.ChatMessage, 100)
	client := globalHub.Register(username, sendChan)
	defer globalHub.Unregister(username, client)

	type receiveResult struct {
		message *pb.ChatMessage
		err     error
	}
	received := make(chan receiveResult)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			message, err := stream.Recv()
			select {
			case received <- receiveResult{message, err}:
			case <-done:
				return
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case message, ok := <-sendChan:
			if !ok {
				return nil
			}
			if err := stream.Send(message); err != nil {
				return err
			}
		case result := <-received:
			if result.err == io.EOF {
				return nil
			}
			if result.err != nil {
				return result.err
			}
			message := result.message
			message.Type = "chat"
			message.Username = username
			message.Time = time.Now().Format("15:04:05")
			globalHub.broadcast(message)
		}
	}
}

func serveFrontend(w http.ResponseWriter, r *http.Request) {
	execPath, _ := os.Getwd()
	http.ServeFile(w, r, filepath.Join(execPath, "../sse-frontend/index.html"))
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
	client := globalHub.Register(username, sendChan)
	defer globalHub.Unregister(username, client)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-sendChan:
			if !ok {
				return
			}
			data, _ := json.Marshal(msg)
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func main() {
	globalHub = NewHub()
	chatServer := &ChatServer{}

	grpcServer := grpc.NewServer()
	pb.RegisterChatServiceServer(grpcServer, chatServer)
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("gRPC 监听失败: ", err)
	}
	go func() {
		log.Println("纯 gRPC 服务器启动: localhost:50051")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal("gRPC 服务失败: ", err)
		}
	}()

	// Both transports expose the same registered service and Hub.
	wrappedServer := grpcweb.WrapServer(grpcServer)
	mux := http.NewServeMux()
	mux.HandleFunc("/", serveFrontend)
	mux.HandleFunc("/chat.v1.ChatService/ChatStream", wrappedServer.ServeHTTP)
	mux.HandleFunc("/sse", handleSSE)
	mux.HandleFunc("/api/online", func(w http.ResponseWriter, r *http.Request) {
		globalHub.mu.RLock()
		count := 0
		for _, clients := range globalHub.clients {
			count += len(clients)
		}
		globalHub.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"count": count})
	})

	// 兼容浏览器发送消息 (简单 HTTP POST)
	mux.HandleFunc("/api/message", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", 405)
			return
		}
		var req struct{ Username, Content string }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if req.Username == "" || req.Content == "" {
			http.Error(w, "Username and content are required", http.StatusBadRequest)
			return
		}
		globalHub.broadcast(&pb.ChatMessage{
			Type: "chat", Username: req.Username, Content: req.Content,
			Time: time.Now().Format("15:04:05"),
		})
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	log.Println("gRPC-Web 服务器启动: http://localhost:8082")
	log.Println("前端地址: http://localhost:8082")
	log.Println("按 Ctrl+C 停止服务器")

	if err := http.ListenAndServe(":8082", mux); err != nil {
		log.Fatal("服务器启动失败: ", err)
	}
}
