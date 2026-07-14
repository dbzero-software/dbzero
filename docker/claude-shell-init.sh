#!/usr/bin/env bash
set -euo pipefail

CLAUDE_CONFIG_DIR="${CLAUDE_CONFIG_DIR:-$HOME/.claude}"
export CLAUDE_CONFIG_DIR

mkdir -p "$CLAUDE_CONFIG_DIR"
chmod 700 "$CLAUDE_CONFIG_DIR"

if [[ ! -f "$CLAUDE_CONFIG_DIR/settings.json" && -f /opt/claude/settings.json ]]; then
    cp /opt/claude/settings.json "$CLAUDE_CONFIG_DIR/settings.json"
    chmod 600 "$CLAUDE_CONFIG_DIR/settings.json"
fi

if [[ $# -eq 0 ]]; then
    exec /bin/bash
fi

exec "$@"