#!/usr/bin/env bash
set -euo pipefail

# Hardened Linux VM provisioning script for distributed-rate-limiter-gateway
APP_USER="gatewayd"
BIN_PATH="/usr/local/bin/rate-limiter-gateway"
TMP_BIN="$(mktemp)"
trap 'rm -f "$TMP_BIN"' EXIT

if ! id -u "$APP_USER" >/dev/null 2>&1; then
  sudo useradd --system --no-create-home --shell /usr/sbin/nologin "$APP_USER"
fi

CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "$TMP_BIN" ./cmd/gateway
sudo install -m 0755 "$TMP_BIN" "$BIN_PATH"
sudo mkdir -p /etc/rate-limiter-gateway

sudo tee /etc/systemd/system/rate-limiter-gateway.service >/dev/null <<EOF
[Unit]
Description=FluxGate Distributed Rate-Limiter & API Gateway
After=network.target

[Service]
User=$APP_USER
Group=$APP_USER
Environment=PORT=8080
EnvironmentFile=-/etc/rate-limiter-gateway/env
ExecStart=$BIN_PATH
Restart=always
RestartSec=3
NoNewPrivileges=true
ProtectSystem=strict

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now rate-limiter-gateway.service
