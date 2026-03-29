#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# IronOraClaw — Oracle Database setup script
#
# This script:
#   1. Starts the Oracle Database Free container (or checks if running)
#   2. Waits for the database to become healthy
#   3. Creates the ironoraclaw database user with required grants
#   4. Runs `ironoraclaw setup-oracle` to initialize the schema
#
# Usage:
#   ./scripts/setup-oracle.sh
#   ORACLE_PWD=MyPassword ./scripts/setup-oracle.sh
# ──────────────────────────────────────────────────────────────
set -euo pipefail

ORACLE_PWD="${ORACLE_PWD:-IronOraClaw2026}"
ORACLE_CONTAINER="${ORACLE_CONTAINER:-ironoraclaw-oracle}"
ORACLE_PORT="${ORACLE_PORT:-1521}"
ORACLE_SERVICE="${ORACLE_SERVICE:-FREEPDB1}"
ORACLE_USER="${ORACLE_USER:-ironoraclaw}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-600}"

info()  { echo "==> $*"; }
warn()  { echo "warning: $*" >&2; }
error() { echo "error: $*" >&2; }

# ── 1. Ensure the Oracle container is running ─────────────────

CONTAINER_CLI="${CONTAINER_CLI:-docker}"
if ! command -v "$CONTAINER_CLI" >/dev/null 2>&1; then
    if command -v podman >/dev/null 2>&1; then
        CONTAINER_CLI="podman"
    else
        error "Neither docker nor podman found. Install one and try again."
        exit 1
    fi
fi

container_running() {
    "$CONTAINER_CLI" inspect -f '{{.State.Running}}' "$ORACLE_CONTAINER" 2>/dev/null | grep -q true
}

container_exists() {
    "$CONTAINER_CLI" inspect "$ORACLE_CONTAINER" >/dev/null 2>&1
}

if container_running; then
    info "Oracle container '$ORACLE_CONTAINER' is already running."
elif container_exists; then
    info "Starting existing Oracle container '$ORACLE_CONTAINER'..."
    "$CONTAINER_CLI" start "$ORACLE_CONTAINER"
else
    info "Starting Oracle Database Free via docker compose..."
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
    ORACLE_PWD="$ORACLE_PWD" "$CONTAINER_CLI" compose -f "$PROJECT_DIR/docker-compose.yml" up oracle-db -d
fi

# ── 2. Wait for the database to become healthy ───────────────

info "Waiting for Oracle Database to become healthy (up to ${MAX_WAIT_SECONDS}s)..."

elapsed=0
interval=10
while [ "$elapsed" -lt "$MAX_WAIT_SECONDS" ]; do
    # Check container health status
    health=$("$CONTAINER_CLI" inspect -f '{{.State.Health.Status}}' "$ORACLE_CONTAINER" 2>/dev/null || echo "unknown")

    if [ "$health" = "healthy" ]; then
        info "Oracle Database is healthy."
        break
    fi

    # Also try a direct connection test
    if "$CONTAINER_CLI" exec "$ORACLE_CONTAINER" bash -c \
        "echo 'SELECT 1 FROM DUAL;' | sqlplus -s sys/\"${ORACLE_PWD}\"@localhost:${ORACLE_PORT}/${ORACLE_SERVICE} as sysdba" \
        >/dev/null 2>&1; then
        info "Oracle Database is responding to queries."
        break
    fi

    printf "  ... still waiting (%ds elapsed, health=%s)\n" "$elapsed" "$health"
    sleep "$interval"
    elapsed=$((elapsed + interval))
done

if [ "$elapsed" -ge "$MAX_WAIT_SECONDS" ]; then
    error "Oracle Database did not become healthy within ${MAX_WAIT_SECONDS} seconds."
    error "Check container logs: $CONTAINER_CLI logs $ORACLE_CONTAINER"
    exit 1
fi

# ── 3. Create the ironoraclaw database user ───────────────────

info "Creating database user '${ORACLE_USER}'..."

"$CONTAINER_CLI" exec "$ORACLE_CONTAINER" bash -c "
sqlplus -s sys/\"${ORACLE_PWD}\"@localhost:${ORACLE_PORT}/${ORACLE_SERVICE} as sysdba <<EOSQL
-- Create user (ignore ORA-01920 if user already exists)
DECLARE
  user_exists EXCEPTION;
  PRAGMA EXCEPTION_INIT(user_exists, -1920);
BEGIN
  EXECUTE IMMEDIATE 'CREATE USER ${ORACLE_USER} IDENTIFIED BY \"${ORACLE_PWD}\"';
  DBMS_OUTPUT.PUT_LINE('User ${ORACLE_USER} created.');
EXCEPTION
  WHEN user_exists THEN
    DBMS_OUTPUT.PUT_LINE('User ${ORACLE_USER} already exists, updating password.');
    EXECUTE IMMEDIATE 'ALTER USER ${ORACLE_USER} IDENTIFIED BY \"${ORACLE_PWD}\"';
END;
/

-- Grant privileges
GRANT CONNECT, RESOURCE, CREATE SESSION TO ${ORACLE_USER};
GRANT CREATE TABLE, CREATE SEQUENCE TO ${ORACLE_USER};
GRANT CREATE MINING MODEL TO ${ORACLE_USER};
GRANT DB_DEVELOPER_ROLE TO ${ORACLE_USER};
GRANT UNLIMITED TABLESPACE TO ${ORACLE_USER};

COMMIT;
EXIT;
EOSQL
"

info "Database user '${ORACLE_USER}' is ready."

# ── 4. Run ironoraclaw setup-oracle ──────────────────────────

IRONORACLAW_BIN=""
if command -v ironoraclaw >/dev/null 2>&1; then
    IRONORACLAW_BIN="ironoraclaw"
elif [ -x "./target/release/ironoraclaw" ]; then
    IRONORACLAW_BIN="./target/release/ironoraclaw"
elif [ -x "./target/debug/ironoraclaw" ]; then
    IRONORACLAW_BIN="./target/debug/ironoraclaw"
fi

if [ -n "$IRONORACLAW_BIN" ]; then
    info "Running schema initialization: $IRONORACLAW_BIN setup-oracle"
    IRONORACLAW_ORACLE_HOST="${IRONORACLAW_ORACLE_HOST:-localhost}" \
    IRONORACLAW_ORACLE_PORT="${ORACLE_PORT}" \
    IRONORACLAW_ORACLE_SERVICE="${ORACLE_SERVICE}" \
    IRONORACLAW_ORACLE_USER="${ORACLE_USER}" \
    IRONORACLAW_ORACLE_PASSWORD="${ORACLE_PWD}" \
        "$IRONORACLAW_BIN" setup-oracle
else
    warn "ironoraclaw binary not found. Build first with 'cargo build --release'."
    warn "Then run: ironoraclaw setup-oracle"
fi

info "Oracle Database setup complete."
echo ""
echo "  Connection details:"
echo "    Host:     localhost"
echo "    Port:     ${ORACLE_PORT}"
echo "    Service:  ${ORACLE_SERVICE}"
echo "    User:     ${ORACLE_USER}"
echo "    Password: (as configured)"
echo ""
echo "  Next steps:"
echo "    ironoraclaw setup-oracle   # Initialize schema (if not done above)"
echo "    ironoraclaw onboard        # Configure LLM provider"
echo "    ironoraclaw agent -m 'Hello!'  # Start chatting"
