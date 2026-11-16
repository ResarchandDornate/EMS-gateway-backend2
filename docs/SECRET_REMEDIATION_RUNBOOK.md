# Secret Remediation Runbook (audit D-02 / D-03)

**Status: PREPARATORY — nothing in this runbook has been executed.** It requires explicit
approval and must be run by an operator. No secret values are reproduced here. The code
changes in this branch are non-destructive: they only *parameterize* `docker-compose.yml`
and harden `.gitignore`. They do **not** rotate, revoke, delete, or rewrite anything.

## What the branch already changed (safe, additive)

1. `docker-compose.yml` no longer contains literal credentials. `POSTGRES_PASSWORD`, the
   InfluxDB admin password and admin token, the `api` `DATABASE_URL` password, and the
   `influx-init` token now use `${VAR:?...}` substitution — identical to the pattern already
   used in `docker-compose.staging.yml`.
2. `.gitignore` now ignores `.env.*` and `.env.bak*` (while keeping the two `*.example`
   files tracked), so backup env files can never be committed again.

## Consequence you must handle BEFORE the next `docker compose up`

Because the compose file now **requires** these variables, the production `.env` on the Mac
mini must define them, or `docker compose up` will fail fast with a clear message. To
**preserve the current working credentials** (no rotation yet), add the current values to
`/Users/ornateserver/portals/EMS-gateway-backend2/.env`:

```
POSTGRES_PASSWORD=<current working value>
INFLUXDB_ADMIN_PASSWORD=<current working value>
INFLUXDB_ADMIN_TOKEN=<current working Influx token>
```

(The repo now uses `INFLUXDB_ADMIN_TOKEN`; your existing `.env` may already carry the token
under `INFLUX_TOKEN` for the app — keep both in sync until rotation.) Do this as a quiet
`.env` edit; `.env` is gitignored and must never be committed.

## Step 1 — Stop tracking the leaked backup file (approval required)

The file `.env.bak.20261030-115058` is tracked in the (public) repo. Remove it from tracking
without deleting your local copy, then commit:

```
git rm --cached .env.bak.20261030-115058
git commit -m "chore(security): stop tracking leaked env backup; parameterize compose secrets"
```

This stops future exposure but **does not** remove it from existing history (see Step 3).

## Step 2 — Make the repository private (approval required)

Until the secrets are rotated, the repository must be private. In GitHub:
`Settings → General → Danger Zone → Change visibility → Private`. (Visibility changes are
explicitly out of scope for automated tooling; a human performs this.)

## Step 3 — Purge the secrets from git history (approval required)

`git rm --cached` leaves the secret in every historical commit. Purge with `git filter-repo`
(preferred) or BFG, on a fresh clone, then force-push. This rewrites history for all clones —
coordinate with everyone who has a clone (lucifer and the Mac mini both do).

```
# fresh mirror clone
git clone --mirror git@github.com:ResarchandDornate/EMS-gateway-backend2.git
cd EMS-gateway-backend2.git
git filter-repo --invert-paths --path .env.bak.20261030-115058
# also scrub the old compose literals from history (strings only; values not shown here)
git filter-repo --replace-text ../replacements.txt     # map each old literal -> ***REMOVED***
git push --force
```

`replacements.txt` is prepared by the operator from the known old literals; it is **not**
included in this repo so the values are not re-committed. After the force-push, every
collaborator re-clones fresh (old clones still contain the secrets).

## Step 4 — Rotate every exposed credential (approval required)

Because the values were public, treat all of them as compromised and rotate, regardless of
Step 3. Rotate in this order and verify each before the next:

1. **`JWT_SECRET`** — signs both user and gateway tokens, so rotating it invalidates all
   active sessions and gateway tokens; gateways re-authenticate via `/gateway/token`
   automatically, users re-login. Update `.env`, recreate the `api` service.
2. **InfluxDB admin token** — create a new token, update `INFLUXDB_ADMIN_TOKEN` /
   `INFLUX_TOKEN` in `.env`, recreate `api`, confirm `/ready` shows `influx: ok`, then revoke
   the old token (confirm S2 writes still succeed first — see the DB preflight below).
3. **PostgreSQL password** — `ALTER ROLE ems PASSWORD …`, update `POSTGRES_PASSWORD` in
   `.env`, recreate `api`, confirm `/ready` shows `postgres: ok`.

Follow the detailed, gate-by-gate procedures already prepared for this system
(`CREDENTIAL_ROTATION_RUNBOOK` and `DATABASE_URL_ROTATION_RUNBOOK`) — this runbook does not
duplicate their step-level gates.

## Guardrails (unchanged from the audit scope)

- Do **not** rotate, revoke, delete, force-push, or change repo visibility without explicit
  approval.
- Preserve the current working credentials until their replacement is verified.
- Never commit `.env` or any `.env.bak*`.
- Never paste a secret value into a commit message, PR, log, or this document.
