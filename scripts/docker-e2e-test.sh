#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/.." && pwd)"
SERVER_TOKEN="${SERVER_TOKEN:-vnts2-cf-local-peer-token-ChangeMe-123!}"
NETWORKS="${NETWORKS:-default}"
DOCKER_BIN="${DOCKER_BIN:-docker}"

if [[ "${USE_SUDO:-1}" == "1" ]]; then
  DOCKER_BIN="sudo -S docker"
fi

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "[vnts2-cf] 构建服务端镜像"
  $DOCKER_BIN build -t vnts2-cf "$ROOT"

  echo "[vnts2-cf] 构建 Alpine vnt2 客户端镜像"
  $DOCKER_BIN build -t vnts2-cf-client -f "$ROOT/test/client.Dockerfile" "$REPO_ROOT"
else
  echo "[vnts2-cf] SKIP_BUILD=1，使用已有 vnts2-cf 和 vnts2-cf-client 镜像"
fi

TMP_CONFIG_DIR="/tmp/vnts2-cf-e2e"
mkdir -p "$TMP_CONFIG_DIR"
cp "$ROOT/wrangler.toml" "$TMP_CONFIG_DIR/wrangler-a.toml"
cp "$ROOT/wrangler.toml" "$TMP_CONFIG_DIR/wrangler-b.toml"

echo "[vnts2-cf] 清理旧容器"
$DOCKER_BIN rm -f vnts2cf-a vnts2cf-b vnt2c-a vnt2c-b >/dev/null 2>&1 || true

echo "[vnts2-cf] 启动两个 vnts2-cf 服务端"
$DOCKER_BIN run -d --name vnts2cf-a \
  --add-host host.docker.internal:host-gateway \
  -p 8787:8787 \
  -p 18786:8786 \
  -v "$TMP_CONFIG_DIR/wrangler-a.toml:/app/wrangler.toml" \
  -v "$ROOT/src:/app/src:ro" \
  -v "$ROOT/scripts/docker-start.mjs:/app/scripts/docker-start.mjs:ro" \
  -v "$ROOT/scripts/wss-proxy.mjs:/app/scripts/wss-proxy.mjs:ro" \
  -e LOCAL_PROTOCOL=http-wss-proxy \
  -e NETWORKS="$NETWORKS" \
  -e SERVER_TOKEN="$SERVER_TOKEN" \
  -e PEER_SERVERS=http://host.docker.internal:18787 \
  -e LOG_LEVEL=debug \
  -e LOG_PASSWORD="${LOG_PASSWORD:-}" \
  vnts2-cf

$DOCKER_BIN run -d --name vnts2cf-b \
  --add-host host.docker.internal:host-gateway \
  -p 8788:8787 \
  -p 18787:8786 \
  -v "$TMP_CONFIG_DIR/wrangler-b.toml:/app/wrangler.toml" \
  -v "$ROOT/src:/app/src:ro" \
  -v "$ROOT/scripts/docker-start.mjs:/app/scripts/docker-start.mjs:ro" \
  -v "$ROOT/scripts/wss-proxy.mjs:/app/scripts/wss-proxy.mjs:ro" \
  -e LOCAL_PROTOCOL=http-wss-proxy \
  -e NETWORKS="$NETWORKS" \
  -e SERVER_TOKEN="$SERVER_TOKEN" \
  -e PEER_SERVERS=http://host.docker.internal:18786 \
  -e LOG_LEVEL=debug \
  -e LOG_PASSWORD="${LOG_PASSWORD:-}" \
  vnts2-cf

echo "[vnts2-cf] 等待服务端启动"
for port in 8787 8788; do
  ready=0
  for _ in $(seq 1 60); do
    if $DOCKER_BIN run --rm --add-host host.docker.internal:host-gateway vnts2-cf-client \
      wget -q -O - --no-check-certificate "https://host.docker.internal:$port/test?format=json" | grep -q '"ok":true'; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "$ready" != "1" ]]; then
    echo "[vnts2-cf] 服务端端口 $port 未在超时时间内就绪"
    $DOCKER_BIN logs "vnts2cf-$([[ "$port" == "8787" ]] && echo a || echo b)" --tail 120 || true
    exit 1
  fi
done

echo "[vnts2-cf] 启动两个 Alpine vnt2 客户端，不使用 host 网络，不使用 --no-tun"
$DOCKER_BIN run -d --name vnt2c-a \
  --add-host host.docker.internal:host-gateway \
  --cap-add NET_ADMIN --device /dev/net/tun \
  vnts2-cf-client \
  vnt2_cli -s wss://host.docker.internal:8787 -n default --ip 10.46.0.2 \
  --cert-mode skip --device-id cf-client-a --device-name cf-client-a \
  -i 172.30.2.0/24,10.46.0.2 --ctrl-port 11233

$DOCKER_BIN run -d --name vnt2c-b \
  --add-host host.docker.internal:host-gateway \
  --cap-add NET_ADMIN --device /dev/net/tun \
  vnts2-cf-client \
  vnt2_cli -s wss://host.docker.internal:8788 -n default --ip 10.46.0.3 \
  --cert-mode skip --device-id cf-client-b --device-name cf-client-b \
  -o 172.30.2.0/24 --ctrl-port 11233

echo "[vnts2-cf] 等待客户端注册和互联客户端列表同步"
sleep 18

echo "[vnts2-cf] vnt2_ctrl clients 输出"
$DOCKER_BIN exec vnt2c-a vnt2_ctrl -p 11233 clients || true
$DOCKER_BIN exec vnt2c-b vnt2_ctrl -p 11233 clients || true

echo "[vnts2-cf] vnt2_ctrl route 输出，验证 -i/-o 参数已进入客户端路由"
$DOCKER_BIN exec vnt2c-a vnt2_ctrl -p 11233 route || true
$DOCKER_BIN exec vnt2c-b vnt2_ctrl -p 11233 route || true

echo "[vnts2-cf] 跨服务端虚拟 IP ping"
$DOCKER_BIN exec vnt2c-a ping -c 4 10.46.0.3
$DOCKER_BIN exec vnt2c-b ping -c 4 10.46.0.2

echo "[vnts2-cf] 状态页面 JSON"
curl -sk "https://127.0.0.1:8787/test?format=json"
echo
curl -sk "https://127.0.0.1:8787/room?format=json&network=default&gateway=10.26.0.1&token=$SECRET"
echo

echo "[vnts2-cf] 最近服务端日志"
$DOCKER_BIN logs vnts2cf-a --tail 80
$DOCKER_BIN logs vnts2cf-b --tail 80
