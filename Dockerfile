FROM golang:alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /storage-node ./cmd/storage-node && \
    CGO_ENABLED=0 go build -o /metadata-server ./cmd/metadata-server

FROM alpine:latest

COPY --from=builder /storage-node /usr/local/bin/storage-node
COPY --from=builder /metadata-server /usr/local/bin/metadata-server