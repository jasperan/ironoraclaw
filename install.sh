#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# ironoraclaw — One-Command Installer
# ---
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/jasperan/ironoraclaw/main/install.sh | bash
# ============================================================

REPO_URL="https://github.com/jasperan/ironoraclaw.git"
PROJECT="ironoraclaw"
BRANCH="main"
INSTALL_DIR="${PROJECT_DIR:-$(pwd)/$PROJECT}"

# ── Colors ──────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

info()    { echo -e "${BLUE}→${NC} $1"; }
success() { echo -e "${GREEN}✓${NC} $1"; }
warn()    { echo -e "${YELLOW}!${NC} $1"; }
fail()    { echo -e "${RED}✗ $1${NC}"; exit 1; }
command_exists() { command -v "$1" &>/dev/null; }

print_banner() {
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}  ironoraclaw${NC}"
    echo -e "  ---"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

clone_repo() {
    if [ -d "$INSTALL_DIR" ]; then
        warn "Directory $INSTALL_DIR already exists"
        info "Pulling latest changes..."
        (cd "$INSTALL_DIR" && git pull origin "$BRANCH" 2>/dev/null) || true
    else
        info "Cloning repository..."
        git clone --depth 1 -b "$BRANCH" "$REPO_URL" "$INSTALL_DIR" || fail "Clone failed. Check your internet connection."
    fi
    success "Repository ready at $INSTALL_DIR"
}

check_prereqs() {
    info "Checking prerequisites..."
    command_exists git || fail "Git is required — https://git-scm.com/"
    success "Git $(git --version | cut -d' ' -f3)"

    command_exists cargo || fail "Rust/Cargo is required — https://rustup.rs/"
    success "Cargo $(cargo --version | cut -d' ' -f2)"
}

install_deps() {
    cd "$INSTALL_DIR"
    info "Building release binary (this may take a few minutes)..."
    cargo build --release
    success "Build complete"

    BIN_NAME=$(ls target/release/ 2>/dev/null | grep -v -E '\.(d|o|rlib|rmeta)$' | head -1)
    if [ -n "$BIN_NAME" ]; then
        LOCAL_BIN="${HOME}/.local/bin"
        mkdir -p "$LOCAL_BIN"
        cp "target/release/$BIN_NAME" "$LOCAL_BIN/" && \
            success "Installed to $LOCAL_BIN/$BIN_NAME" || true
    fi
}

main() {
    print_banner
    check_prereqs
    clone_repo
    install_deps
    print_done
}

print_done() {
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "  ${BOLD}Installation complete!${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  ${BOLD}Location:${NC}  $INSTALL_DIR"
    echo -e "  ${BOLD}Binary:${NC}   $INSTALL_DIR/target/release/"
    echo -e "  ${BOLD}Docker:${NC}   cd $INSTALL_DIR && docker compose up -d"
    echo ""
}

main "$@"
