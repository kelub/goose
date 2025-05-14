# 第一阶段：构建依赖
FROM golang:1.24.1 AS builder

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY go.mod go.sum ./
RUN go mod download

# 复制项目代码
COPY . .

# 编译所有 Go 文件到 `/bin`
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/goose ./cmd/...

# 第二阶段：运行测试
FROM golang:1.24.1
WORKDIR /app

# 复制构建后的二进制和源代码
COPY --from=builder /bin/goose /app/goose
COPY --from=builder /app/pkg /app/pkg

# 设置默认执行命令：跑所有的测试
CMD ["go", "test", "-v", "./..."]
