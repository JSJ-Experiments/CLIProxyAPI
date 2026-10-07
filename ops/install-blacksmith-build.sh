#!/usr/bin/env bash
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
REPO="${CLIPROXY_GITHUB_REPO:-JSJ-Experiments/CLIProxyAPI}"
WORKFLOW="${CLIPROXY_WORKFLOW:-blacksmith-arm64-build.yml}"
SERVICE="${CLIPROXY_SERVICE:-cli-proxy-api.service}"
DEPLOY_DIR="${CLIPROXY_DEPLOY_DIR:-$ROOT/deployment}"
OUT="$DEPLOY_DIR/bin/cli-proxy-api"
PREV="$OUT.prev"
RUN_ID="${1:-}"

if [[ -z "$RUN_ID" ]]; then
  RUN_ID="$(gh run list \
    --repo "$REPO" \
    --workflow "$WORKFLOW" \
    --branch main \
    --status success \
    --limit 1 \
    --json databaseId \
    --jq '.[0].databaseId')"
fi

if [[ -z "$RUN_ID" || "$RUN_ID" == "null" ]]; then
  echo "No successful Blacksmith build found." >&2
  exit 1
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

echo "Downloading Blacksmith build from GitHub Actions run $RUN_ID..."
gh run download "$RUN_ID" \
  --repo "$REPO" \
  --name cli-proxy-api-linux-arm64 \
  --dir "$tmp"

(cd "$tmp" && sha256sum -c cli-proxy-api.sha256)
"$tmp/cli-proxy-api" --help >/dev/null

mkdir -p "$(dirname "$OUT")"
if [[ -x "$OUT" ]]; then
  cp -a "$OUT" "$PREV"
fi
sudo systemctl stop "$SERVICE"
install -m 0755 "$tmp/cli-proxy-api" "$OUT"

if ! sudo systemctl start "$SERVICE"; then
  echo "New binary failed to start; restoring previous binary." >&2
  if [[ -x "$PREV" ]]; then
    cp -a "$PREV" "$OUT"
    sudo systemctl start "$SERVICE" || true
  fi
  exit 1
fi

cat "$tmp/build-info.txt"
sudo systemctl --no-pager --full status "$SERVICE" | sed -n '1,12p'
