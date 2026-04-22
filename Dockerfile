# Bani — Database Migration Engine
# Multi-stage build for minimal image size.

# ── Stage 1: Build UI + wheel ────────────────────────────────────────
FROM node:20-slim AS ui-builder

WORKDIR /ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci
COPY ui/ .
RUN npm run build

# ── Stage 2: Build Python wheel ─────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src/ src/
COPY --from=ui-builder /ui/dist ui/dist/

RUN pip install --no-cache-dir build \
    && python -m build --wheel --outdir /build/dist

# ── Stage 2: Runtime ─────────────────────────────────────────────────
FROM python:3.12-slim

# System dependencies for database drivers
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    unixodbc-dev \
    curl \
    build-essential \
    libkrb5-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Build FreeTDS 1.4.x from source (Debian ships 1.3.x which has
# known DBPROCESS-dead bugs under concurrent load)
RUN curl -fsSL https://www.freetds.org/files/stable/freetds-1.4.26.tar.gz \
        -o /tmp/freetds.tar.gz \
    && tar xzf /tmp/freetds.tar.gz -C /tmp \
    && cd /tmp/freetds-1.4.26 \
    && ./configure --prefix=/usr --with-unixodbc=/usr --with-tdsver=7.4 \
    && make -j"$(nproc)" \
    && make install \
    && rm -rf /tmp/freetds*

# FreeTDS config — disable idle timeout for long transfers
COPY docker/freetds.conf /etc/freetds/freetds.conf

# Install Microsoft ODBC Driver 18 for SQL Server (amd64 + arm64)
# Use Debian 12 (bookworm) repo — Trixie (13) has signing issues
RUN ARCH=$(dpkg --print-architecture) \
    && apt-get update \
    && apt-get install -y --no-install-recommends gnupg apt-transport-https \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=${ARCH} signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get purge -y gnupg apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Install Bani from the wheel built in stage 1
COPY --from=builder /build/dist/*.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl && rm /tmp/*.whl

# Rebuild pymssql against FreeTDS 1.4 then remove build tools
RUN pip install --no-cache-dir --force-reinstall --no-binary=pymssql pymssql \
    && apt-get purge -y build-essential \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-built React UI into the package directory (belt-and-suspenders;
# the wheel already includes it, but this ensures it's always present)
COPY --from=ui-builder /ui/dist /usr/local/lib/python3.12/site-packages/bani/ui/dist/

# Non-root user for security
RUN useradd --create-home bani
USER bani
WORKDIR /home/bani

# Default projects directory
RUN mkdir -p /home/bani/.bani/projects

# Ensure print() output appears immediately in docker logs
ENV PYTHONUNBUFFERED=1

# Expose ports: 8910 for Web UI
EXPOSE 8910

# Default entrypoint
ENTRYPOINT ["bani"]
CMD ["--help"]
