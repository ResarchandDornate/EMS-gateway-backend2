# EMS-gateway-backend2

## Running locally

```bash
uvicorn src.main:asgi_app --reload
```

Serve `src.main:asgi_app` (not `src.main:app`): the `asgi_app` entrypoint wraps the
application with the request-correlation middleware, so `X-Request-ID` response headers
and the per-request access log are active only on this path. This documents the supported
local command; it does not verify or change any external production launch configuration.

## Deploying

Production must serve the same entrypoint, `src.main:asgi_app`, without `--reload`. Example
(illustrative, not a verified production command): `uvicorn src.main:asgi_app --host 0.0.0.0
--port 8000`, behind any ASGI server / process manager. Serving `src.main:app` starts a
working API but **bypasses `RequestContextMiddleware`**: no `X-Request-ID` response header,
no per-request access log, and no correlated `unhandled_exception` log on 500s.

The launch artifacts in this repository are the `Dockerfile` CMD and the docker-compose `api` service; both must serve `src.main:asgi_app`. The deployment configuration is the only place this entrypoint is
enforced. Documentation cannot confirm which target a deployment actually runs - confirm the
deployed command points at `src.main:asgi_app`.
