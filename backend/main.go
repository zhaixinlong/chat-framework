package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// 消息结构
type Message struct {
	Type     string `json:"type"`
	Username string `json:"username"`
	Content  string `json:"content"`
	Time     string `json:"time"`
}

// 客户端连接
type Client struct {
	conn     *websocket.Conn
	username string
	send     chan Message
}

// Hub管理所有客户端连接
type Hub struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan Message
	mu         sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan Message),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Printf("用户 %s 已连接，当前在线人数: %d", client.username, len(h.clients))

			// 发送系统欢迎消息
			welcomeMsg := Message{
				Type:    "system",
				Content: fmt.Sprintf("欢迎 %s 加入聊天室！", client.username),
				Time:    time.Now().Format("15:04:05"),
			}
			h.broadcast <- welcomeMsg

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()
			log.Printf("用户 %s 已断开，当前在线人数: %d", client.username, len(h.clients))

			// 发送系统离开消息
			leaveMsg := Message{
				Type:    "system",
				Content: fmt.Sprintf("%s 已离开聊天室", client.username),
				Time:    time.Now().Format("15:04:05"),
			}
			h.broadcast <- leaveMsg

		case message := <-h.broadcast:
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
			h.mu.RUnlock()
		}
	}
}

var hub = NewHub()

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // 允许跨域
	},
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket升级失败: %v", err)
		return
	}

	// 获取用户名
	username := r.URL.Query().Get("username")
	if username == "" {
		username = fmt.Sprintf("用户%d", time.Now().UnixNano()%10000)
	}

	client := &Client{
		conn:     conn,
		username: username,
		send:     make(chan Message, 256),
	}

	hub.register <- client

	// 启动读写协程
	go writePump(client)
	readPump(client)
}

func readPump(client *Client) {
	defer func() {
		hub.unregister <- client
		client.conn.Close()
	}()

	for {
		var msg Message
		err := client.conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("读取错误: %v", err)
			}
			break
		}

		msg.Username = client.username
		msg.Time = time.Now().Format("15:04:05")
		msg.Type = "chat"

		log.Printf("[%s] %s: %s", msg.Time, msg.Username, msg.Content)
		hub.broadcast <- msg
	}
}

func writePump(client *Client) {
	defer client.conn.Close()

	for message := range client.send {
		err := client.conn.WriteJSON(message)
		if err != nil {
			log.Printf("写入错误: %v", err)
			return
		}
	}
}

func serveFrontend(w http.ResponseWriter, r *http.Request) {
	// Get the absolute path to the frontend directory
	execPath, _ := os.Getwd()
	frontendPath := filepath.Join(execPath, "../frontend/index.html")
	http.ServeFile(w, r, frontendPath)
}

func main() {
	go hub.Run()

	// WebSocket路由
	http.HandleFunc("/ws", handleWebSocket)

	// 静态文件服务
	http.HandleFunc("/", serveFrontend)

	log.Println("服务器启动: http://localhost:8080")
	log.Println("WebSocket地址: ws://localhost:8080/ws")
	log.Println("按 Ctrl+C 停止服务器")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("服务器启动失败: ", err)
	}
}
