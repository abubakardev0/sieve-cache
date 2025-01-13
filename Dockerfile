FROM golang:1.22.3-alpine AS builder

ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod ./

RUN go mod download

COPY . .

RUN go build -o sieve-cache ./cmd/main.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/sieve-cache /app/sieve-cache

EXPOSE 8080

CMD ["/app/sieve-cache"]