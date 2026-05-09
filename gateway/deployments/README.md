# Deployments

Reference configuration for running the IndexQube gateway on a single droplet
behind Caddy.

## Files

- `gateway.env.example` — environment template. Copied to `/etc/indexqube/gateway.env`
  by `scripts/deploy.sh` on first run. Single source of truth for runtime config.
- `systemd/indexqube-gateway.service` — installed to `/etc/systemd/system/`. Runs
  the binary as the `indexqube` system user with hardening enabled.
- `caddy/Caddyfile` — drop-in for Caddy. Reverse-proxies the public hostname to
  the gateway on `127.0.0.1:8080` with SSE-friendly settings.

## Layout on the host

| Path                                         | Owner               | Purpose                          |
| -------------------------------------------- | ------------------- | -------------------------------- |
| `/opt/indexqube/indexqube-gateway`           | `indexqube`         | Compiled binary                  |
| `/etc/indexqube/gateway.env`                 | `root` (mode 0600)  | Runtime environment              |
| `/etc/systemd/system/indexqube-gateway.service` | `root`           | systemd unit                     |
| `/var/log/caddy/indexqube-access.log`        | Caddy user          | Access log                       |

## Install

From a checkout on the droplet:

```sh
sudo bash gateway/scripts/deploy.sh
```

This builds the binary, creates the `indexqube` user, installs the systemd unit,
seeds `/etc/indexqube/gateway.env` from `gateway.env.example` (only on first
run), then enables and restarts the service.

After the first install, edit `/etc/indexqube/gateway.env` to set
`ANTHROPIC_API_KEY` and any production knobs, then:

```sh
sudo systemctl restart indexqube-gateway
```

## Caddy

Place `caddy/Caddyfile` (or merge its site block) at `/etc/caddy/Caddyfile` and
reload Caddy. The site block assumes the gateway listens on
`INDEXQUBE_BIND_ADDR=127.0.0.1:8080`. The `request_body max_size` is kept in
sync with `INDEXQUBE_MAX_BODY_BYTES`; bump both together.

## Production checklist

In production (`INDEXQUBE_ENV=production`), config validation requires
`SUPABASE_DB_URL`. Also set, at minimum:

- `ANTHROPIC_API_KEY`
- `CORS_ALLOWED_ORIGINS` (do not rely on the dev defaults)
- `CORS_ALLOW_CHROME_EXTENSIONS=false` unless the extension is in scope
- `OTEL_EXPORTER_OTLP_ENDPOINT` if you have a collector
