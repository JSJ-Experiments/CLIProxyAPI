# Local server deployment

This fork is the editable source tree for the live CLIProxyAPI instance on the ARM server.

## Live layout

- Source: this repository
- Config/auth/logs/runtime data: `deployment/` (intentionally local and untracked)
- Systemd unit: `cli-proxy-api.service`
- Listener: `127.0.0.1:8317`
- Public Caddy paths:
  - `https://cfarm.jadenjsj.com/cliproxy/`
  - `https://arm.jadenjsj.com/cliproxy/`
- Caddy imports `ops/caddy-cliproxy-route.caddy` into those two hosts.

`tsarm.jadenjsj.com` intentionally does not expose CLIProxyAPI.

## Modify and redeploy

Normal/heavy builds run on Blacksmith using `.github/workflows/blacksmith-arm64-build.yml`.
The workflow runs the full Go test suite and produces a native Linux ARM64 artifact.

After a successful build on `main`, install the latest artifact with:

```sh
./ops/install-blacksmith-build.sh
```

You can also pass a specific GitHub Actions run ID. The installer verifies the artifact checksum,
keeps `deployment/bin/cli-proxy-api.prev`, and rolls back if the service cannot start.

For a quick local fallback build:

Edit the source normally, then run:

```sh
./ops/rebuild-local.sh
```

The script builds `./cmd/server` from the current checkout, atomically replaces the live binary,
keeps the previous binary at `deployment/bin/cli-proxy-api.prev`, and restarts the systemd service.

## Request logging

The local config enables debug/application logs plus full request logging. Request logs include the
downstream request body, translated upstream request, upstream response, downstream response, and
WebSocket timelines where applicable. Both log-retention cleanup limits are set to `0`, so CLIProxyAPI
does not delete logs based on total size or error-file count.

These logs can contain prompts, model output, API keys/auth headers, and other sensitive request data.
Keep `deployment/` private and do not commit it.
