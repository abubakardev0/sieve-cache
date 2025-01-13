FROM golang:1.20-alpine AS builder

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /app

RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o sieve-cache ./main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/sieve-cache /app/sieve-cache

EXPOSE 8080

CMD ["/app/sieve-cache"]