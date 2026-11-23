package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	pb "grpc-backend/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Print("输入昵称: ")
	reader := bufio.NewReader(os.Stdin)
	username, _ := reader.ReadString('\n')
	username = strings.TrimSpace(username)
	if username == "" {
		username = fmt.Sprintf("用户%d", time.Now().UnixNano()%10000)
	}

	// 连接 gRPC 服务器（纯 gRPC 端口 50051）
	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Println("连接失败:", err)
		return
	}
	defer conn.Close()

	client := pb.NewChatServiceClient(conn)

	// 建立双向流
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.ChatStream(ctx)
	if err != nil {
		fmt.Println("建立流失败:", err)
		return
	}

	// 发送第一条消息（身份标识）
	if err := stream.Send(&pb.ChatMessage{Username: username}); err != nil {
		fmt.Println("发送昵称失败:", err)
		return
	}

	fmt.Printf("=== 已连接 gRPC 双向流 (%s) ===\n", username)

	done := make(chan struct{})

	// 接收协程
	go func() {
		defer close(done)
		for {
			msg, err := stream.Recv()
			if err != nil {
				fmt.Println("\n连接断开:", err)
				return
			}
			if msg.Type == "system" {
				fmt.Printf("\n[系统] %s\n> ", msg.Content)
			} else {
				fmt.Printf("\n[%s] %s: %s\n> ", msg.Time, msg.Username, msg.Content)
			}
		}
	}()

	// Stdin is read separately so a disconnected stream can end the client promptly.
	lines := make(chan string)
	go func() {
		defer close(lines)
		for {
			text, err := reader.ReadString('\n')
			if text != "" {
				select {
				case lines <- strings.TrimSpace(text):
				case <-ctx.Done():
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					fmt.Println("读取输入失败:", err)
				}
				return
			}
		}
	}()
	for {
		fmt.Print("> ")
		select {
		case <-done:
			return
		case text, ok := <-lines:
			if !ok || text == "quit" || text == "exit" {
				stream.CloseSend()
				return
			}
			if text == "" {
				continue
			}
			if err := stream.Send(&pb.ChatMessage{Content: text}); err != nil {
				fmt.Println("发送失败:", err)
				return
			}
		}
	}
}
