# Multi-stage build for Go bot
FROM golang:alpine AS builder

# Install ca-certificates and git (required for downloading Go modules)
RUN apk update && apk add --no-cache ca-certificates git

WORKDIR /app

# Copy dependency manifests
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o bot ./cmd/bot/main.go

# Minimal runtime image
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bot .

# Expose health check port
EXPOSE 8080

ENTRYPOINT ["./bot"]
