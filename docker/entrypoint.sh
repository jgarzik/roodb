#!/bin/bash
set -e

DATA_DIR="${ROODB_DATA_DIR:-/data}"
CERT_PATH="${ROODB_CERT_PATH:-/certs/server.crt}"
KEY_PATH="${ROODB_KEY_PATH:-/certs/server.key}"
CA_CERT_PATH="${ROODB_CA_CERT_PATH:-/certs/ca.crt}"
PORT="${ROODB_PORT:-3307}"

# Handle ROODB_ROOT_PASSWORD_FILE (Docker secrets pattern)
if [ -n "$ROODB_ROOT_PASSWORD_FILE" ] && [ -f "$ROODB_ROOT_PASSWORD_FILE" ]; then
    export ROODB_ROOT_PASSWORD="$(cat -- "$ROODB_ROOT_PASSWORD_FILE")"
fi

# Validate TLS certificates exist
if [ ! -f "$CERT_PATH" ] || [ ! -f "$KEY_PATH" ] || [ ! -f "$CA_CERT_PATH" ]; then
    echo "ERROR: TLS certificates not found"
    echo "  Expected: $CERT_PATH, $KEY_PATH, and $CA_CERT_PATH"
    echo "  Mount your certificates to /certs or set ROODB_CERT_PATH/ROODB_KEY_PATH/ROODB_CA_CERT_PATH"
    exit 1
fi

# Initialize database if not already initialized
# roodb_init is idempotent - exits 0 if already initialized
echo "Checking database initialization..."
if ! roodb_init --data-dir "$DATA_DIR"; then
    echo "ERROR: Database initialization failed"
    exit 1
fi

# Start the server
echo "Starting RooDB server on port $PORT..."
exec roodb --port "$PORT" --data-dir "$DATA_DIR" --cert-path "$CERT_PATH" --key-path "$KEY_PATH" --raft-ca-cert-path "$CA_CERT_PATH"
