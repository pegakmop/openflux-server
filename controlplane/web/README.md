# OpenFlux control-plane web panel

SvelteKit 5 + Tailwind CSS dashboard for the OpenFlux control plane, served under the
same `/admin/` URL as the old embedded page. Talks to the Go controlplane's `/v1/admin/*`
API (see `../API.md`). RU/EN, dark/light theme.

## Requirements

- Node 20+ or Bun (`bun` recommended — the dev server and the production entry both run on it)
- A running Go controlplane (`http://127.0.0.1:8080`) for the `/v1/admin/*` calls

## Layout

| Path | What it is |
|---|---|
| `/` | (prod) `308` → `/admin/` |
| `/admin/` | dashboard: stat cards (lifetime/today traffic, keys, nodes), traffic chart per day (7/30/90/365), server load card, nodes table |
| `/admin/keys` | create key (with QR deep link), list/filter/paginate, enable/disable, set/clear traffic limit, rotate, delete |
| `/admin/nodes` | register node, rotate its token, one-time token display |
| `/admin/ingest` | create ingest token, enable/disable |
| `/admin/login` | paste `CONTROLPLANE_ADMIN_TOKEN` once; kept in `localStorage` = `openflux_admin_token` |
| `/healthz`, `/v1/*` | (prod, no Nginx) proxied to the Go controlplane / health of the panel |

## Develop

```bash
bun install
bun run dev        # vite dev on :5173, /v1/* proxied to CONTROLPLANE_UPSTREAM (default :8080)
bun run check      # svelte-check
bun run build
```

## Production

```bash
bun ./node_modules/.bin/svelte-kit sync && bun run build
bun server.js      # runs ./build output, proxies /v1/* and /healthz to CONTROLPLANE_UPSTREAM
```

Env (defaults): `CONTROLPLANE_UPSTREAM=http://127.0.0.1:8080`, `CONTROLPLANE_WEB_HOST=127.0.0.1`,
`CONTROLPLANE_WEB_PORT=3000` (`PORT`/`HOST` are honored as aliases).

### Nginx (recommended)

Split `/admin/*` → Bun, `/v1/*` → Go; then Go is never reached through Bun:

```nginx
location /admin/ { proxy_pass http://127.0.0.1:3000; proxy_set_header Host $host; }
location /v1/    { proxy_pass http://127.0.0.1:8080; proxy_set_header Host $host; }
```

Deploy fragments and a systemd unit live in `deploy/`.

### http-mode (no Nginx)

Point a public `:80`/`:443` port at `bun server.js`. It serves `/admin/*` from the SvelteKit build
and proxies `/healthz` and `/v1/*` to Go, so the browser still talks to a single origin. Do NOT do
this while an Nginx split is active — requests would loop.

## Notes

- The admin token is used as `Authorization: Bearer <token>` on every admin call; a `401` logs the
  UI out.
- Key list is paginated server-side (`limit=50`), label/status filtering and sorting happen client-side.
- `paths.base = '/admin'`: SvelteKit answers everything outside `/base` with 404 before any hook,
  which is exactly why the `/v1` proxy is implemented at the HTTP layer (`server.js`), not in hooks.