# Session Reset Proxy

A lightweight HTTP proxy that manages OpenClaw sessions via the Gateway WebSocket protocol.

Exposes simple REST endpoints so you can reset, delete, compact, or create sessions with a single HTTP call — no WebSocket client code needed on the caller side.

## Setup

```bash
cd tools/session-reset-proxy
npm install

# Copy and edit env
cp .env.example .env
# Set OPENCLAW_GATEWAY_TOKEN to your gateway token
```

## Run

```bash
node server.mjs
```

Or with env vars inline:

```bash
OPENCLAW_GATEWAY_TOKEN=xxx node server.mjs
```

## Endpoints

| Method | Path       | Description                                      |
|--------|------------|--------------------------------------------------|
| POST   | `/new`     | Reset a user's session (new transcript)          |
| POST   | `/delete`  | Delete a user's session and transcript            |
| POST   | `/compact` | Compact a user's session (summarize old messages) |
| POST   | `/create`  | Create a new session for a user                   |
| GET    | `/health`  | Health check                                      |
| GET    | `/`        | Usage info                                        |

### Request Body

All mutation endpoints accept:

```json
{ "user": "<user-id>" }
```

Also accepts `userId` or `user_id` as aliases.

### Examples

```bash
# Reset a user's session
curl -X POST http://127.0.0.1:18800/new \
  -H 'Content-Type: application/json' \
  -d '{"user":"REDACTED_USER_ID"}'

# Delete a user's session
curl -X POST http://127.0.0.1:18800/delete \
  -H 'Content-Type: application/json' \
  -d '{"user":"REDACTED_USER_ID"}'

# Health check
curl http://127.0.0.1:18800/health
```

## Session Key Format

The proxy constructs session keys as `agent:<agentId>:<userId>`, matching OpenClaw's convention for user-scoped sessions. The agent id defaults to `main` and can be overridden via `RESET_PROXY_AGENT`.

Note: `sessions.reset` creates a new session if one doesn't exist for the key. This means you can use it idempotently — calling it on a user that has no session simply creates one.

## Architecture

```
HTTP Client ──POST /new──▶ [Proxy Server] ──sessions.reset──▶ [OpenClaw Gateway WS]
                                   │
                              (reconnects
                               on drop)
```

The proxy maintains a persistent WebSocket connection to the Gateway and reconnects automatically on disconnect. All HTTP requests are forwarded as Gateway RPC calls.

### Gateway Connection

The proxy connects as a `gateway-client` backend on loopback, which bypasses device identity requirements per the Gateway protocol spec. This means:

- No device keypair needed
- No pairing required
- Token auth is sufficient
- Only works on localhost/loopback (by design)

For remote connections, you'd need to implement the full device auth handshake (keypair + challenge signing) or enable `gateway.controlUi.dangerouslyDisableDeviceAuth`.

## Running

On the target platform, copy all files from `./systemd` into `~/.config/systemd/user`.

```shell
chmod 644 ~/.config/systemd/user/openclaw-session-reset-proxy.service
chmod 600 ~/.config/systemd/user/openclaw-session-reset-proxy.env

systemctl --user enable openclaw-session-reset-proxy
systemctl --user start openclaw-session-reset-proxy
```
