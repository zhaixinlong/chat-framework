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
)

// 消息结构
type Message struct {
	Type     string `json:"type"`
	Username string `json:"username"`
	Content  string `json:"content"`
	Time     string `json:"time"`
}

// SSE 客户端
type Client struct {
	username    string
	messageChan chan Message
}

// Hub 管理所有客户端
type Hub struct {
	clients []*Client
	mu      sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		clients: make([]*Client, 0),
	}
}

// 注册新客户端
func (h *Hub) Register(username string) *Client {
	client := &Client{
		username:    username,
		messageChan: make(chan Message, 100),
	}
	h.mu.Lock()
	h.clients = append(h.clients, client)
	count := len(h.clients)
	h.mu.Unlock()

	log.Printf("用户 %s 已连接，当前在线人数: %d", username, count)

	// 广播加入消息
	h.broadcast(Message{
		Type:    "system",
		Content: fmt.Sprintf("欢迎 %s 加入聊天室！", username),
		Time:    time.Now().Format("15:04:05"),
	})

	return client
}

// Unregister preserves username-based removal for existing callers.
func (h *Hub) Unregister(username string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, client := range h.clients {
		if client.username == username {
			h.removeClient(i)
			return
		}
	}
}

func (h *Hub) unregisterClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, current := range h.clients {
		if current == client {
			h.removeClient(i)
			return
		}
	}
}

// The caller holds the write lock, excluding concurrent broadcasts.
func (h *Hub) removeClient(index int) {
	client := h.clients[index]
	copy(h.clients[index:], h.clients[index+1:])
	h.clients[len(h.clients)-1] = nil
	h.clients = h.clients[:len(h.clients)-1]
	close(client.messageChan)
	log.Printf("用户 %s 已断开，当前在线人数: %d", client.username, len(h.clients))
}

// 广播消息
func (h *Hub) broadcast(msg Message) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, client := range h.clients {
		select {
		case client.messageChan <- msg:
		default:
			// 通道满了，跳过
		}
	}
}

// 处理用户发送的消息
func (h *Hub) HandleMessage(username, content string) {
	msg := Message{
		Type:     "chat",
		Username: username,
		Content:  content,
		Time:     time.Now().Format("15:04:05"),
	}
	log.Printf("[%s] %s: %s", msg.Time, msg.Username, msg.Content)
	h.broadcast(msg)
}

// 获取在线人数
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

var hub = NewHub()

// SSE 连接
func handleSSE(w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, "缺少用户名", http.StatusBadRequest)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "不支持 SSE", http.StatusInternalServerError)
		return
	}

	client := hub.Register(username)
	defer hub.unregisterClient(client)

	// 设置 SSE 响应头
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ctx := r.Context()

	for {
		select {
		case <-ctx.Done():
			log.Printf("客户端 %s 断开连接", username)
			return
		case msg, ok := <-client.messageChan:
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

// 接收客户端发送的消息 (POST)
func handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "仅支持 POST", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Username string `json:"username"`
		Content  string `json:"content"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "请求格式错误", http.StatusBadRequest)
		return
	}

	if req.Username == "" || req.Content == "" {
		http.Error(w, "用户名和内容不能为空", http.StatusBadRequest)
		return
	}

	hub.HandleMessage(req.Username, req.Content)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// 获取在线人数
func handleOnlineCount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(map[string]int{"count": hub.Count()})
}

func serveFrontend(w http.ResponseWriter, r *http.Request) {
	execPath, _ := os.Getwd()
	frontendPath := filepath.Join(execPath, "../sse-frontend/index.html")
	http.ServeFile(w, r, frontendPath)
}

func getLocalIP() string {
	addrs, _ := net.InterfaceAddrs()
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return "0.0.0.0"
}

func main() {
	http.HandleFunc("/", serveFrontend)
	http.HandleFunc("/sse", handleSSE)
	http.HandleFunc("/api/message", handleMessage)
	http.HandleFunc("/api/online", handleOnlineCount)

	ip := getLocalIP()
	log.Printf("SSE 服务器启动: http://%s:8081", ip)
	log.Printf("手机访问地址: http://%s:8081", ip)
	log.Println("消息 API: POST http://localhost:8081/api/message")
	log.Println("按 Ctrl+C 停止服务器")

	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatal("服务器启动失败: ", err)
	}
}
