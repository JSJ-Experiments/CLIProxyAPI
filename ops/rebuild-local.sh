#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
DEPLOY_DIR="${CLIPROXY_DEPLOY_DIR:-$ROOT/deployment}"
BIN_DIR="$DEPLOY_DIR/bin"
OUT="$BIN_DIR/cli-proxy-api"
NEW="$OUT.new"
PREV="$OUT.prev"
SERVICE="${CLIPROXY_SERVICE:-cli-proxy-api.service}"

VERSION="${VERSION:-$(git describe --tags --always --dirty | sed 's/^v//')}"
COMMIT="$(git rev-parse --short=12 HEAD)"
BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

mkdir -p "$BIN_DIR"
rm -f "$NEW"

echo "Building CLIProxyAPI $VERSION ($COMMIT)..."
CGO_ENABLED=1 go build -buildvcs=false \
  -ldflags="-s -w -X main.Version=$VERSION -X main.Commit=$COMMIT -X main.BuildDate=$BUILD_DATE" \
  -o "$NEW" ./cmd/server/
chmod 0755 "$NEW"

if [[ -x "$OUT" ]]; then
  cp -a "$OUT" "$PREV"
fi
mv -f "$NEW" "$OUT"

if ! sudo systemctl restart "$SERVICE"; then
  echo "Restart failed; restoring previous binary." >&2
  if [[ -x "$PREV" ]]; then
    cp -a "$PREV" "$OUT"
    sudo systemctl restart "$SERVICE" || true
  fi
  exit 1
fi

sudo systemctl --no-pager --full status "$SERVICE" | sed -n '1,12p'
