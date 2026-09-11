# OpenFlux Control Plane — API reference

[English](API.md) | [Русский](API.ru.md)

This documents the full HTTP API exposed by `controlplane` (see
[README.md](README.md) for what the service is and how to run it). The web
admin panel at `/admin/` is a thin client over the exact same `/v1/admin/*`
endpoints described here — anything it can do, you can do with `curl`.

## Base URL

Everything below is relative to wherever `controlplane` is reachable, e.g.
`https://your-domain/` after `deploy/install.sh` has put it behind Nginx +
TLS, or `http://127.0.0.1:8080` talking to it directly.

## Authentication

There are four independent bearer tokens, scoped to four different callers.
Every one of them is sent the same way: `Authorization: Bearer <token>`.

| Token             | Who holds it                              | Used on                          | How it's obtained |
|-------------------|--------------------------------------------|-----------------------------------|--------------------|
| Admin token        | You (the operator)                        | `/v1/admin/*`                    | `CONTROLPLANE_ADMIN_TOKEN` env var, set once at deploy time |
| Ingest token       | A third-party service that issues its own keys (a bot, a shop) | `/v1/ingest/keys` | `POST /v1/admin/ingest-tokens` |
| Node token         | An exit-node process                      | `/v1/nodes/*`                    | `POST /v1/admin/nodes` (or `.../rotate-token`) |
| Key token          | An end user's client                      | `/v1/resolve`                    | `POST /v1/admin/keys` (or `/v1/ingest/keys`) |

The admin token is a single static value compared in constant time — it is
not stored in the database and cannot be listed or rotated via the API
(only by changing `CONTROLPLANE_ADMIN_TOKEN` and restarting). Ingest, node,
and key tokens are all generated server-side, returned to you **exactly
once** at creation/rotation time, and stored hashed — there is no way to
retrieve a previously-issued token's value again, so save it immediately.

## Conventions

- All request and response bodies are JSON (`Content-Type: application/json`).
- Request bodies reject unknown fields (HTTP 400), rather than silently
  ignoring typos.
- On failure, the response body is `{"error": "<message>"}` with a 4xx/5xx
  status. A handful of endpoints that succeed with nothing to say back
  (e.g. key deletion) return `204 No Content` instead.
- **Field-casing quirk to know about:** the three admin *list/get* endpoints
  (`GET /v1/admin/nodes`, `GET /v1/admin/keys`, `GET /v1/admin/keys/{id}`,
  `GET /v1/admin/ingest-tokens`) serialize the internal Go structs directly
  and have no `json` tags, so their fields come back **capitalized**
  (`ID`, `Name`, `MaxKeys`, `Status`, ...). Every *create* endpoint uses a
  dedicated response type with normal lowercase `snake_case` JSON instead
  (`id`, `name`, `max_keys`, `token`, ...). This is a real inconsistency in
  the API, not a documentation typo — code against both shapes as shown
  below, don't assume they match.

## Health

### `GET /healthz`

No auth. Used by the systemd unit / a load balancer.

```json
{"status": "ok"}
```

## Admin: nodes

An exit node is one running instance of the OpenFlux engine (typically the
same VPS controlplane itself runs on, though nothing stops you from
registering more and running them elsewhere — see README.md's "Scaling
notes"). Each node has its own bearer token and a `max_keys` capacity;
new keys are auto-assigned to whichever active node has the most free
capacity.

### `POST /v1/admin/nodes` — register a node

```json
// request
{"name": "node-eu-1", "max_keys": 500}
```
`max_keys` defaults to 500 if omitted or `<= 0`.

```json
// 201 response
{"id": "5b1e...", "name": "node-eu-1", "max_keys": 500, "token": "of_node_..."}
```
`token` is shown once — this is what you pass to the exit-node process as
`--node-token`.

### `GET /v1/admin/nodes` — list nodes

```json
// 200 response — note the capitalized fields (see the quirk above)
[
  {
    "ID": "5b1e...",
    "Name": "node-eu-1",
    "MaxKeys": 500,
    "Status": "active",
    "LastHeartbeatAt": "2026-09-11T10:00:00Z",
    "CreatedAt": "2026-09-01T12:00:00Z"
  }
]
```
`LastHeartbeatAt` is `null` if the node has never called
`POST /v1/nodes/{id}/heartbeat`.

### `POST /v1/admin/nodes/{id}/rotate-token` — rotate a node's token

Invalidates the node's current token immediately and issues a new one.

```json
// 200 response
{"token": "of_node_..."}
```
`404` if `{id}` doesn't exist.

## Admin: keys

A key is one end user's access token. It's bound to a `transport` (currently
always `"yandex"`) and a `doc_url` (the Yandex Docs URL the client tunnels
through), and can carry a traffic quota.

### `POST /v1/admin/keys` — create a key

```json
// request
{
  "label": "user-42",
  "doc_url": "https://docs.yandex.ru/docs/edit?url=...",
  "transport": "yandex",
  "traffic_limit_bytes": 10737418240,
  "owner_ref": "customer-id",
  "expires_at": "2027-01-01T00:00:00Z"
}
```
Only `doc_url` is required; `transport` defaults to `"yandex"`;
`traffic_limit_bytes`, `owner_ref` and `expires_at` are all optional
(no limit / no owner / never expires).

```json
// 201 response
{"id": "9c2f...", "token": "of_key_..."}
```

### `GET /v1/admin/keys` — list keys

Optional query param `?owner_ref=<value>` filters to one owner.

```json
// 200 response — capitalized fields (see the quirk above)
[
  {
    "ID": "9c2f...",
    "Label": "user-42",
    "Transport": "yandex",
    "DocURL": "https://docs.yandex.ru/docs/edit?url=...",
    "AssignedNodeID": "5b1e...",
    "Enabled": true,
    "TrafficLimitBytes": 10737418240,
    "BytesSentTotal": 1048576,
    "BytesReceivedTotal": 2097152,
    "OwnerRef": "customer-id",
    "ExpiresAt": null,
    "CreatedAt": "2026-09-01T12:00:00Z",
    "UpdatedAt": "2026-09-01T12:00:00Z",
    "LastSeenAt": "2026-09-11T09:55:00Z"
  }
]
```

### `GET /v1/admin/keys/{id}` — get one key

Same shape as one element of the list above. `404` if it doesn't exist.

### `PATCH /v1/admin/keys/{id}` — change a key's traffic limit

```json
// request — set a limit, or send null to remove it
{"traffic_limit_bytes": 21474836480}
```
```json
// 200 response
{"status": "updated"}
```

### `POST /v1/admin/keys/{id}/enable` / `.../disable`

```json
// 200 response
{"enabled": true}
```

### `DELETE /v1/admin/keys/{id}`

`204 No Content` on success, `404` if it doesn't exist. Irreversible.

## Admin: ingest tokens

An ingest token lets an external service (a bot, a storefront, whatever
generates its own Yandex Docs and wants to hand out OpenFlux keys for them)
create keys via the API on its own, without your admin token.

### `POST /v1/admin/ingest-tokens` — create one

```json
// request
{"label": "key-gen-bot", "scope": "keys:write"}
```
`scope` defaults to `"keys:write"` — the only scope the ingest endpoint
currently checks for.

```json
// 201 response
{"id": "1a2b...", "label": "key-gen-bot", "scope": "keys:write", "token": "of_ingest_..."}
```

### `GET /v1/admin/ingest-tokens` — list

```json
// 200 response — capitalized fields (see the quirk above)
[
  {"ID": "1a2b...", "Label": "key-gen-bot", "Scope": "keys:write", "Enabled": true, "CreatedAt": "2026-09-01T12:00:00Z"}
]
```

### `POST /v1/admin/ingest-tokens/{id}/enable` / `.../disable`

```json
// 200 response
{"enabled": true}
```
There is no delete endpoint for ingest tokens — disable a leaked one
instead.

## Ingest: create keys as a third party

### `POST /v1/ingest/keys`

Auth: `Authorization: Bearer <ingest token>` (not the admin token). Accepts
either a single object or a JSON array of up to 200, for bulk creation.

```json
// request — single
{"label": "user-42", "doc_url": "https://docs.yandex.ru/docs/edit?url=...", "traffic_limit_bytes": 10737418240}
```
`doc_url` is required on every item; `transport` defaults to `"yandex"`;
`owner_ref` is honored per item.

```json
// 201 response — single request, single object back
{"id": "9c2f...", "token": "of_key_..."}
```
A batch (array) request gets a JSON array of the same objects back, in the
same order. `403` if the token's scope isn't `keys:write`.

## Node: pull assigned keys and report usage

These are called by the exit-node engine process itself, authenticated
with its own node token. `{id}` in the two path-parameterized routes below
also accepts the literal string `me`, so a node doesn't need to know its
own generated ID up front — but if you do pass an explicit ID, it must
match the node the token belongs to (`403` otherwise).

### `GET /v1/nodes/keys` — list this node's active, assigned keys

```json
// 200 response
[
  {
    "id": "9c2f...",
    "doc_url": "https://docs.yandex.ru/docs/edit?url=...",
    "transport": "yandex",
    "traffic_limit_bytes": 10737418240,
    "bytes_used_total": 3145728
  }
]
```
Only keys that are currently enabled, not expired, and under quota are
included — a key that flips to disabled/over-quota simply stops appearing
here on the node's next poll.

### `POST /v1/nodes/{id|me}/usage` — report a batch of traffic deltas

```json
// request — up to 1000 entries
{
  "deltas": [
    {"key_id": "9c2f...", "bytes_sent_delta": 1048576, "bytes_received_delta": 2097152}
  ]
}
```
Deltas are additive (not absolute totals) — report what moved since the
last call. Entries with an empty `key_id` are silently skipped.

```json
// 200 response
{"disabled_now": ["9c2f..."]}
```
`disabled_now` lists the IDs of any keys that crossed their traffic quota
as a direct result of this report and got auto-disabled.

### `POST /v1/nodes/{id|me}/heartbeat`

No request body.

```json
// 200 response
{"status": "ok"}
```
Updates the node's `LastHeartbeatAt` (see `GET /v1/admin/nodes`), which is
the only thing today's `Status` field/UI rely on to infer liveness.

## Client: resolve a key

### `POST /v1/resolve`

Auth: `Authorization: Bearer <key token>`. Rate-limited per source IP
(`CONTROLPLANE_RATE_LIMIT_RPS`, default 1 request/second, small burst) —
this is the one endpoint an untrusted client token hits directly.

```json
// 200 response — active key
{
  "status": "active",
  "doc_url": "https://docs.yandex.ru/docs/edit?url=...",
  "transport": "yandex",
  "bytes_used_total": 3145728,
  "traffic_limit_bytes": 10737418240
}
```
`status` is one of `active`, `disabled`, `over_quota`, or `not_found`
(unknown/invalid token — returned as `200`, not `401`, so a client can't
distinguish "wrong token" from "revoked token" by status code alone).
`doc_url`/`transport` are only present when `status` is `active`.

## Admin web panel

### `GET /admin/` or `GET /admin/index.html`

No auth on the GET itself — it serves a static, self-contained HTML page
(`internal/api/web/admin.html`, embedded in the binary) that then asks for
the admin token in its own login screen and stores it in the browser's
`localStorage`, using it as the `Authorization` header on every
`/v1/admin/*` call it makes from there on. It's a thin client over exactly
this API: a "Keys" tab (create/list/enable/disable/delete keys) and a
"Settings" tab showing the one exit node this deployment registered
(status, heartbeat, rotate its token) plus ingest-token management — all
in Russian, nothing more.
