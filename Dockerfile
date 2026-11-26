FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
COPY web ./web
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /gateway ./cmd/gateway

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget \
    && adduser -D -u 10001 appuser
COPY --from=builder /gateway /usr/local/bin/gateway
USER appuser
EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD /bin/sh -c 'P="${PORT:-8080}"; if [ -n "${LISTEN_ADDR:-}" ]; then P="${LISTEN_ADDR##*:}"; fi; wget -qO- "http://127.0.0.1:${P}/health" || exit 1'
ENTRYPOINT ["gateway"]
