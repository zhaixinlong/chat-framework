# chat-framework

使用 Go 实现的聊天通信示例，涵盖 WebSocket、SSE 和 gRPC 双向流。各示例通过内存中的 Hub 管理连接并广播消息，配有浏览器页面或命令行客户端，便于运行、比较和学习不同通信方式。

## 环境

- Go 1.24 或更高版本，首次运行需要下载模块依赖。
- 浏览器，用于 WebSocket 与 SSE 页面。
- 本次验证使用 Go 1.26.0。

各后端和 gRPC 客户端使用独立 Go 模块，运行命令前请进入对应目录。静态页面通过相邻目录加载，直接启动编译后的程序时也需要保持对应工作目录。

## 目录与端口

| 目录 | 内容 | 默认端口 |
| --- | --- | --- |
| `backend/` | WebSocket 服务 | 8080 |
| `frontend/` | WebSocket 聊天页面，由服务端提供 | — |
| `sse-backend/` | SSE 接收、HTTP POST 发送、在线连接数 | 8081 |
| `sse-frontend/` | SSE 聊天页面，由服务端提供 | — |
| `grpc-backend/` | gRPC 双向流，以及共享同一 Hub 的 HTTP/SSE 入口 | 50051、8082 |
| `grpc-client/` | gRPC 命令行客户端 | — |
| `grpc-backend/proto/` | Protobuf 服务定义 | — |
| `grpc-backend/pb/` | 已生成的 Go 消息与服务代码 | — |

## WebSocket 示例

```bash
git clone https://github.com/zhaixinlong/chat-framework.git
cd chat-framework/backend
go run .
```

浏览器打开 <http://localhost:8080/>，使用两个标签页输入不同昵称，验证加入通知、消息广播和离开通知。

- 连接地址为 `ws://localhost:8080/ws?username=alice`。
- 消息使用 JSON，聊天文本放在 `content` 字段中。
- Hub 在同一事件循环中处理注册、注销和广播。加入与离开通知直接分发，避免循环向自身的无缓冲通道发送消息而阻塞。
- 慢连接的发送队列满时，服务端移除并关闭该连接，避免阻塞其他客户端；单次写入设有 10 秒截止时间。

## SSE 示例

另开终端运行：

```bash
cd chat-framework/sse-backend
go run .
```

浏览器打开 <http://localhost:8081/>，输入昵称。接收消息使用 SSE，发送消息使用 HTTP POST。

```bash
# 保持连接，观察事件
curl -N 'http://localhost:8081/sse?username=alice'

# 在另一个终端发送消息
curl -X POST http://localhost:8081/api/message \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","content":"hello"}'

# 查询当前连接数量
curl http://localhost:8081/api/online
```

同一昵称可创建多个连接。HTTP 连接退出时按连接对象注销，避免关闭另一个同名连接。在线数按连接计数。

## gRPC 示例

启动服务端：

```bash
cd chat-framework/grpc-backend
go run .
```

另开终端启动客户端，可以重复启动两个客户端互相聊天：

```bash
cd chat-framework/grpc-client
go run .
```

输入昵称后发送文本，输入 `quit` 或 `exit` 退出。输入流结束或服务端断开时，客户端也会退出。

### 浏览器与 HTTP 入口

浏览器打开 <http://localhost:8082/>。此页面复用 `sse-frontend/index.html`，通过 SSE 和 HTTP POST 与 gRPC 客户端共享消息。

| 地址 | 用途 |
| --- | --- |
| `localhost:50051` | 原生 gRPC 双向流，供命令行客户端使用 |
| `GET /sse` | 浏览器接收广播 |
| `POST /api/message` | 浏览器发送消息 |
| `GET /api/online` | 所有传输方式的连接总数 |
| `/chat.v1.ChatService/ChatStream` | 注册到同一服务实例的 gRPC-Web 入口 |

当前浏览器页面使用 SSE + POST，尚未提供生成的 gRPC-Web 浏览器客户端。服务端的每条原生 gRPC 流使用单一发送路径，并在结束时注销连接和关闭发送队列。

## 消息结构

WebSocket、SSE 与 HTTP 示例使用以下字段。gRPC 对应类型见 `grpc-backend/proto/chat.proto`。

```json
{
  "type": "chat",
  "username": "alice",
  "content": "hello",
  "time": "15:04:05"
}
```

`type` 为 `system` 时表示加入或离开通知。时间由服务端生成，仅用于显示。服务之间不共享状态；8082 的 HTTP/SSE 与 50051 的 gRPC 因使用同一 Hub 而互通。

## 测试与构建

在仓库根目录运行：

```bash
for module in backend sse-backend grpc-backend grpc-client; do
  (cd "$module" && go test -race -timeout 45s ./... && go vet ./...) || break
done
```

回归测试覆盖 WebSocket 加入、广播、离开后的继续收发，SSE 同名连接注销与并发注册广播，以及 gRPC 收发、流结束与发送队列回收。

构建后端时在对应模块目录执行 `go build -o server .`，构建命令行客户端时在 `grpc-client` 中执行 `go build -o client .`。二进制属于本机构建产物，已从版本库移除并加入忽略规则。修改 proto 时需要安装兼容的 `protoc`、`protoc-gen-go` 与 `protoc-gen-go-grpc` 后重新生成代码。

## 示例边界

- 消息与连接状态存储在单进程内存中，重启后清空；没有离线消息、持久化与多实例同步。
- 发送队列容量有限。SSE 和 gRPC 广播在队列满时跳过消息；WebSocket 会断开慢连接。需要可靠投递时应另行实现确认、重试、去重及恢复机制。
- 昵称由客户端提供，当前没有身份认证、权限控制或业务限流。WebSocket/SSE 的跨域配置用于演示。
- 本地示例使用明文 HTTP/gRPC。公开部署需要配置 TLS、访问控制、资源限制及适合环境的超时和健康检查。
