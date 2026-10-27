# EMS-gateway-backend2

## Running locally

```bash
uvicorn src.main:asgi_app --reload
```

Serve `src.main:asgi_app` (not `src.main:app`): the `asgi_app` entrypoint wraps the
application with the request-correlation middleware, so `X-Request-ID` response headers
and the per-request access log are active only on this path. This documents the supported
local command; it does not verify or change any external production launch configuration.
