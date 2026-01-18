# syntax=docker/dockerfile:1

# =============================================================================
# Stage 1: Chef - Install cargo-chef for dependency caching
# =============================================================================
FROM rust:bookworm AS chef
RUN cargo install cargo-chef
WORKDIR /app

# =============================================================================
# Stage 2: Planner - Generate recipe.json (dependency manifest)
# =============================================================================
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# =============================================================================
# Stage 3: Builder - Build dependencies (cached), then build application
# =============================================================================
FROM chef AS builder

# Copy recipe and build dependencies first (cached layer)
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Copy source and build the application
COPY . .
RUN cargo build --release --bin roodb --bin roodb_init

# =============================================================================
# Stage 4: Runtime - Minimal image with just the binaries
# =============================================================================
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --create-home --shell /bin/bash roodb

# Copy binaries from builder
COPY --from=builder /app/target/release/roodb /usr/local/bin/roodb
COPY --from=builder /app/target/release/roodb_init /usr/local/bin/roodb_init

# Copy entrypoint script
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Create data and certs directories
RUN mkdir -p /data /certs && chown -R roodb:roodb /data

# Switch to non-root user
USER roodb

# Expose ports
# 3307 - Client connections (MySQL protocol over TLS)
# 4307 - Raft consensus (internal cluster communication)
EXPOSE 3307 4307

# Mount points
VOLUME ["/data", "/certs"]

ENTRYPOINT ["/entrypoint.sh"]
