FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -o /gateway ./cmd/gateway

FROM alpine:3.20
COPY --from=builder /gateway /usr/local/bin/gateway
EXPOSE 8080
ENTRYPOINT ["gateway"]
