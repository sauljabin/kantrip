#!/bin/sh

# Source this file so its exports remain in the current shell.

if [ -n "${KANTRIP_MANUAL_ROOT:-}" ]; then
    printf 'Error: a manual test environment is already active: %s\n' \
        "$KANTRIP_MANUAL_ROOT" >&2
    return 1
fi

_KANTRIP_MANUAL_TMP="${TMPDIR:-/tmp}"
KANTRIP_MANUAL_ROOT="$(
    mktemp -d "${_KANTRIP_MANUAL_TMP%/}/kantrip-manual.XXXXXX"
)" || return 1
unset _KANTRIP_MANUAL_TMP

chmod 700 "$KANTRIP_MANUAL_ROOT" || return 1
mkdir -m 700 "$KANTRIP_MANUAL_ROOT/runtime" || return 1

KANTRIP_DATABASE="$KANTRIP_MANUAL_ROOT/data/profiles.db"
XDG_RUNTIME_DIR="$KANTRIP_MANUAL_ROOT/runtime"
export KANTRIP_MANUAL_ROOT KANTRIP_DATABASE XDG_RUNTIME_DIR

printf 'Manual test environment created: %s\n' "$KANTRIP_MANUAL_ROOT"
