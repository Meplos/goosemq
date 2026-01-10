# Stage 1: Build
FROM docker.io/library/golang:1.25-alpine AS builder

# Install git (nécessaire si tu utilises des modules depuis github)
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum first for caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the broker binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o broker ./cmd/broker/main.go

# Stage 2: Minimal image
FROM docker.io/library/alpine:latest

# Install ca-certificates (if your app makes https requests)
RUN apk add --no-cache ca-certificates

WORKDIR /app

# Copy the binary from the builder
COPY --from=builder /app/broker .
COPY ./static/ ./static

# Expose the port your broker listens on (example 7456)
EXPOSE 7456

# Run the broker
CMD ["./broker"]
