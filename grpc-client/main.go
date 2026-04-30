package main

import (
	"bufio"
	"context"
	"fmt"
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
	stream, err := client.ChatStream(context.Background())
	if err != nil {
		fmt.Println("建立流失败:", err)
		return
	}

	// 发送第一条消息（身份标识）
	stream.Send(&pb.ChatMessage{Username: username})

	fmt.Printf("=== 已连接 gRPC 双向流 (%s) ===\n", username)

	done := make(chan bool)

	// 接收协程
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				fmt.Println("\n连接断开:", err)
				done <- true
				return
			}
			if msg.Type == "system" {
				fmt.Printf("\n[系统] %s\n> ", msg.Content)
			} else {
				fmt.Printf("\n[%s] %s: %s\n> ", msg.Time, msg.Username, msg.Content)
			}
		}
	}()

	// 发送协程
	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		if text == "quit" || text == "exit" {
			fmt.Println("断开连接...")
			stream.CloseSend()
			return
		}

		err := stream.Send(&pb.ChatMessage{
			Content: text,
		})
		if err != nil {
			fmt.Println("发送失败:", err)
			return
		}
	}
}
