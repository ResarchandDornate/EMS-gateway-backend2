FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.lock.txt .
# lock file was frozen on Windows; pywin32 has no Linux wheel
RUN grep -v '^pywin32==' requirements.lock.txt > /tmp/req.txt \
    && pip install --no-cache-dir -r /tmp/req.txt

COPY alembic.ini .
COPY migrations ./migrations
COPY scripts ./scripts
COPY src ./src

EXPOSE 8000
# apply pending migrations, then serve
CMD ["sh", "-c", "alembic upgrade head && uvicorn src.main:app --host 0.0.0.0 --port 8000 --proxy-headers --forwarded-allow-ips='*' --workers 2"]
