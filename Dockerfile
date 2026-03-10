# Stage 1: Build frontend
FROM node:20-slim AS frontend-build
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Python runtime
FROM python:3.12-slim@sha256:ac212230555ffb7ec17c214fb4cf036ced11b30b5b460994376b0725c7f6c151 AS runtime
WORKDIR /app

# Create non-root user
RUN groupadd --system a2d && useradd --system --gid a2d a2d

# Install Python dependencies
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --no-cache-dir .

# Copy server and frontend build
COPY server/ ./server/
COPY --from=frontend-build /app/frontend/dist ./frontend/dist

# Switch to non-root user
USER a2d

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')" || exit 1

CMD ["uvicorn", "server.main:app", "--host", "0.0.0.0", "--port", "8000"]
