#!/usr/bin/env node
/**
 * Session Reset Proxy
 *
 * Exposes a simple HTTP endpoint that resets OpenClaw sessions
 * via the Gateway WebSocket protocol.
 *
 * Usage:
 *   node server.mjs
 *
 * Environment:
 *   OPENCLAW_GATEWAY_URL    — Gateway WS URL (default: ws://127.0.0.1:18789)
 *   OPENCLAW_GATEWAY_TOKEN  — Gateway auth token (required)
 *   RESET_PROXY_SECRET      — Shared secret for HTTP auth (required)
 *   RESET_PROXY_PORT        — HTTP listen port (default: 18800)
 *   RESET_PROXY_HOST        — HTTP listen host (default: 127.0.0.1)
 *   RESET_PROXY_AGENT       — Agent id (default: main)
 *   RESET_PROXY_TIMEOUT     — RPC timeout ms (default: 10000)
 */

import http from 'node:http';
import crypto from 'node:crypto';
import WebSocket from 'ws';

// ── Config ──────────────────────────────────────────────────────────────────
const GW_URL     = process.env.OPENCLAW_GATEWAY_URL   || 'ws://127.0.0.1:18789';
const GW_TOKEN   = process.env.OPENCLAW_GATEWAY_TOKEN;
const PROXY_SECRET = process.env.RESET_PROXY_SECRET;
const PORT       = parseInt(process.env.RESET_PROXY_PORT || '18800', 10);
const HOST       = process.env.RESET_PROXY_HOST || '127.0.0.1';
const AGENT_ID   = process.env.RESET_PROXY_AGENT || 'main';
const RPC_TIMEOUT = parseInt(process.env.RESET_PROXY_TIMEOUT || '10000', 10);

if (!GW_TOKEN) {
  console.error('ERROR: OPENCLAW_GATEWAY_TOKEN is required');
  process.exit(1);
}

if (!PROXY_SECRET) {
  console.error('ERROR: RESET_PROXY_SECRET is required');
  process.exit(1);
}

// ── Gateway WS Client ──────────────────────────────────────────────────────
class GatewayClient {
  #url;
  #token;
  #ws = null;
  #connected = false;
  #connecting = false;
  #readyResolve = null;
  #readyReject = null;
  #readyPromise = null;
  #pending = new Map(); // id → { resolve, reject, timer }
  #reconnectTimer = null;
  #connId = null;

  constructor(url, token) {
    this.#url = url;
    this.#token = token;
  }

  /** Returns a promise that resolves when the WS is authenticated and ready. */
  async ready() {
    if (this.#connected) return;
    if (this.#readyResolve) return this.#readyPromise;
    this.#connecting = true;
    this.#readyPromise = new Promise((resolve, reject) => {
      this.#readyResolve = resolve;
      this.#readyReject = reject;
    });
    this.#connect();
    return this.#readyPromise;
  }

  /** Send an RPC request and wait for the response. */
  async request(method, params, timeout = RPC_TIMEOUT) {
    await this.ready();
    const id = crypto.randomUUID();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(id);
        reject(new Error(`RPC ${method} timed out after ${timeout}ms`));
      }, timeout);

      this.#pending.set(id, { resolve, reject, timer });

      this.#ws.send(JSON.stringify({ type: 'req', id, method, params }));
    });
  }

  get connected() {
    return this.#connected;
  }

  get reconnecting() {
    return !this.#connected && this.#connecting;
  }

  close() {
    clearTimeout(this.#reconnectTimer);
    if (this.#ws) {
      this.#ws.removeAllListeners();
      this.#ws.close();
    }
    this.#connected = false;
    this.#connecting = false;
  }

  // ── Private ────────────────────────────────────────────────────────────────

  #connect() {
    if (this.#ws) {
      this.#ws.removeAllListeners();
      if (this.#ws.readyState === WebSocket.OPEN) this.#ws.close();
    }

    this.#ws = new WebSocket(this.#url);

    this.#ws.on('open', () => {
      // Wait for the connect.challenge event from the gateway
    });

    this.#ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }
      this.#handleMessage(msg);
    });

    this.#ws.on('close', (code, reason) => {
      this.#connected = false;
      this.#connecting = false;
      console.log(`[gw] disconnected (code=${code}), reconnecting in 2s...`);
      this.#reconnectTimer = setTimeout(() => this.#connect(), 2000);
    });

    this.#ws.on('error', (err) => {
      console.error('[gw] ws error:', err.message);
      if (!this.#connected && this.#readyReject) {
        this.#readyReject(err);
        this.#readyResolve = null;
        this.#readyReject = null;
      }
    });
  }

  #handleMessage(msg) {
    // Gateway challenge → respond with connect
    if (msg.type === 'event' && msg.event === 'connect.challenge') {
      const connectId = crypto.randomUUID();
      this.#ws.send(JSON.stringify({
        type: 'req',
        id: connectId,
        method: 'connect',
        params: {
          minProtocol: 4,
          maxProtocol: 4,
          client: {
            id: 'gateway-client',
            version: '1.0.0',
            platform: 'linux',
            mode: 'backend',
          },
          role: 'operator',
          scopes: ['operator.read', 'operator.write', 'operator.admin'],
          auth: { token: this.#token },
        },
      }));
      return;
    }

    // Connect response
    if (msg.type === 'res' && msg.ok && msg.payload?.type === 'hello-ok') {
      this.#connected = true;
      this.#connecting = false;
      this.#connId = msg.payload.server?.connId;
      console.log(`[gw] connected (connId=${this.#connId})`);
      if (this.#readyResolve) {
        this.#readyResolve();
        this.#readyResolve = null;
        this.#readyReject = null;
      }
      return;
    }

    // RPC response
    if (msg.type === 'res' && msg.id) {
      const pending = this.#pending.get(msg.id);
      if (pending) {
        clearTimeout(pending.timer);
        this.#pending.delete(msg.id);
        if (msg.ok) {
          pending.resolve(msg.payload);
        } else {
          pending.reject(new Error(msg.error?.message || JSON.stringify(msg.error)));
        }
      }
      return;
    }

    // Connect failure
    if (msg.type === 'res' && !msg.ok && msg.payload?.type === 'hello-error') {
      console.error('[gw] connect failed:', msg.error?.message);
      if (this.#readyReject) {
        this.#readyReject(new Error(msg.error?.message || 'connect failed'));
        this.#readyResolve = null;
        this.#readyReject = null;
      }
      return;
    }
  }
}

// ── HTTP Server ─────────────────────────────────────────────────────────────
const gw = new GatewayClient(GW_URL, GW_TOKEN);

/** Guard: reject if gateway is not connected. */
function requireGateway(res) {
  if (gw.reconnecting) {
    json(res, 503, { ok: false, error: 'Gateway is reconnecting, try again shortly' });
    return false;
  }
  if (!gw.connected) {
    json(res, 503, { ok: false, error: 'Gateway not connected' });
    return false;
  }
  return true;
}

function parseBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString()));
      } catch (e) {
        reject(new Error('Invalid JSON'));
      }
    });
    req.on('error', reject);
  });
}

function json(res, status, body) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

/** Constant-time string comparison to prevent timing attacks. */
function timingSafeEqual(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') return false;
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return crypto.timingSafeEqual(bufA, bufB);
}

/** Extract Bearer token from Authorization header. */
function extractBearer(req) {
  const auth = req.headers['authorization'] || '';
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim();
  return null;
}

/** Guard: reject if shared secret is missing or wrong. */
function requireAuth(req, res) {
  const token = extractBearer(req);
  if (!token || !timingSafeEqual(token, PROXY_SECRET)) {
    json(res, 401, { ok: false, error: 'Unauthorized: invalid or missing Authorization header' });
    return false;
  }
  return true;
}

const server = http.createServer(async (req, res) => {
  // CORS — permissive for local use
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // Health check (unauthenticated — safe for monitoring)
  if (req.method === 'GET' && req.url === '/health') {
    return json(res, 200, {
      ok: true,
      gwConnected: gw.connected,
      gwReconnecting: gw.reconnecting,
      uptime: process.uptime(),
    });
  }

  // All other routes require auth
  if (!requireAuth(req, res)) return;

  // POST /new — reset a user's session
  if (req.method === 'POST' && req.url === '/new') {
    if (!requireGateway(res)) return;
    let body;
    try {
      body = await parseBody(req);
    } catch (e) {
      return json(res, 400, { ok: false, error: e.message });
    }

    const userId = body.user || body.userId || body.user_id;
    if (!userId) {
      return json(res, 400, { ok: false, error: 'Missing required field: user' });
    }

    const sessionKey = `agent:${AGENT_ID}:${userId}`;

    try {
      const result = await gw.request('sessions.reset', {
        key: sessionKey,
        reason: 'reset',
      });
      return json(res, 200, result);
    } catch (e) {
      const status = e.message?.includes('timed out') ? 504 : 502;
      console.error(`[proxy] reset failed for user=${userId}:`, e.message);
      return json(res, status, { ok: false, error: e.message });
    }
  }

  // POST /delete — delete a user's session entirely
  if (req.method === 'POST' && req.url === '/delete') {
    if (!requireGateway(res)) return;
    let body;
    try {
      body = await parseBody(req);
    } catch (e) {
      return json(res, 400, { ok: false, error: e.message });
    }

    const userId = body.user || body.userId || body.user_id;
    if (!userId) {
      return json(res, 400, { ok: false, error: 'Missing required field: user' });
    }

    const sessionKey = `agent:${AGENT_ID}:${userId}`;

    try {
      const result = await gw.request('sessions.delete', {
        key: sessionKey,
        deleteTranscript: body.deleteTranscript !== false,
      });
      return json(res, 200, result);
    } catch (e) {
      const status = e.message?.includes('timed out') ? 504 : 502;
      console.error(`[proxy] delete failed for user=${userId}:`, e.message);
      return json(res, status, { ok: false, error: e.message });
    }
  }

  // POST /compact — compact a user's session
  if (req.method === 'POST' && req.url === '/compact') {
    if (!requireGateway(res)) return;
    let body;
    try {
      body = await parseBody(req);
    } catch (e) {
      return json(res, 400, { ok: false, error: e.message });
    }

    const userId = body.user || body.userId || body.user_id;
    if (!userId) {
      return json(res, 400, { ok: false, error: 'Missing required field: user' });
    }

    const sessionKey = `agent:${AGENT_ID}:${userId}`;

    try {
      const result = await gw.request('sessions.compact', {
        key: sessionKey,
      });
      return json(res, 200, result);
    } catch (e) {
      const status = e.message?.includes('timed out') ? 504 : 502;
      console.error(`[proxy] compact failed for user=${userId}:`, e.message);
      return json(res, status, { ok: false, error: e.message });
    }
  }

  // POST /create — create a new session
  if (req.method === 'POST' && req.url === '/create') {
    if (!requireGateway(res)) return;
    let body;
    try {
      body = await parseBody(req);
    } catch (e) {
      return json(res, 400, { ok: false, error: e.message });
    }

    const userId = body.user || body.userId || body.user_id;
    if (!userId) {
      return json(res, 400, { ok: false, error: 'Missing required field: user' });
    }

    const sessionKey = `agent:${AGENT_ID}:${userId}`;

    try {
      const result = await gw.request('sessions.create', {
        key: sessionKey,
      });
      return json(res, 200, result);
    } catch (e) {
      const status = e.message?.includes('timed out') ? 504 : 502;
      console.error(`[proxy] create failed for user=${userId}:`, e.message);
      return json(res, status, { ok: false, error: e.message });
    }
  }

  // GET / — usage info
  if (req.method === 'GET' && req.url === '/') {
    return json(res, 200, {
      service: 'openclaw-session-reset-proxy',
      endpoints: {
        'POST /new':     'Reset session for a user (creates new transcript)',
        'POST /delete':  'Delete session for a user (removes transcript)',
        'POST /compact': 'Compact session for a user (summarize old messages)',
        'POST /create':  'Create a new session for a user',
        'GET /health':   'Health check',
      },
      body: { user: '<user-id>' },
      auth: 'Authorization: Bearer <secret> (required on all endpoints except /health)',
      example: `curl -X POST http://127.0.0.1:${PORT}/new -H 'Authorization: Bearer <secret>' -H 'Content-Type: application/json' -d '{"user":"REDACTED_USER_ID"}'`,
    });
  }

  json(res, 404, { ok: false, error: 'Not found' });
});

// ── Startup ─────────────────────────────────────────────────────────────────
async function main() {
  console.log(`[proxy] connecting to gateway at ${GW_URL}...`);
  await gw.ready();

  server.listen(PORT, HOST, () => {
    console.log(`[proxy] listening on http://${HOST}:${PORT}`);
    console.log(`[proxy] agent: ${AGENT_ID}`);
    console.log(`[proxy] endpoints: POST /new, /delete, /compact, /create | GET /health`);
  });
}

main().catch((err) => {
  console.error('[proxy] fatal:', err);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGINT', () => { gw.close(); server.close(); process.exit(0); });
process.on('SIGTERM', () => { gw.close(); server.close(); process.exit(0); });
