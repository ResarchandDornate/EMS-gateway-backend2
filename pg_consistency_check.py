import sys, yaml, urllib.parse, pathlib
# Fail-closed structured consistency check: no values/hashes printed, YAML-parsed, staging untouched.
def die(msg): print("RESULT: FAIL —", msg); sys.exit(2)
def env_value(path, key):
    vals=[]
    for raw in pathlib.Path(path).read_text().splitlines():
        s=raw.strip()
        if not s or s.startswith("#") or "=" not in s: continue
        k,v=s.split("=",1)
        if k.strip()==key: vals.append(v.strip().strip('"').strip("'"))
    if len(vals)!=1: die(f"{path}:{key} must appear exactly once (found {len(vals)})")
    return vals[0]
def dsn_password(dsn, where):
    try: u=urllib.parse.urlparse(dsn)
    except Exception: die(f"{where}: DATABASE_URL not parseable")
    if not u.scheme.startswith("postgres"): die(f"{where}: unexpected scheme")
    if u.password is None or u.password=="": die(f"{where}: DSN has no password")
    return urllib.parse.unquote(u.password)
def compose_env(doc, service):
    try: env=doc["services"][service]["environment"]
    except Exception: die(f"compose services.{service}.environment missing")
    if isinstance(env, dict): return {str(k):("" if v is None else str(v)) for k,v in env.items()}
    if isinstance(env, list):
        out={}
        for item in env:
            if "=" not in str(item): die(f"compose {service} env list item malformed")
            k,v=str(item).split("=",1); out[k]=v
        return out
    die(f"compose {service}.environment has unexpected type {type(env).__name__}")

envf, compf = sys.argv[1], sys.argv[2]
doc=yaml.safe_load(pathlib.Path(compf).read_text())
if not isinstance(doc, dict) or "services" not in doc: die("compose: no services mapping")
api_env=compose_env(doc,"api"); pg_env=compose_env(doc,"postgres")
if "DATABASE_URL" not in api_env: die("compose services.api.environment.DATABASE_URL MISSING")
if "POSTGRES_PASSWORD" not in pg_env: die("compose services.postgres.environment.POSTGRES_PASSWORD MISSING")
s1=dsn_password(env_value(envf,"DATABASE_URL"), "env.DATABASE_URL")
s2=dsn_password(api_env["DATABASE_URL"], "compose.api.DATABASE_URL")
s3=pg_env["POSTGRES_PASSWORD"]
if not s3: die("compose.POSTGRES_PASSWORD empty")
for name in ("env.DATABASE_URL","compose.api.DATABASE_URL","compose.POSTGRES_PASSWORD"): print(f"{name}: PRESENT")
print("RESULT:", "ALL THREE MATCH" if s1==s2==s3 else "MISMATCH — sources disagree")
