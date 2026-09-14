# Manual Testing

These checks complement the offline test suite and the automated sandbox smoke
workflow. Each scenario describes how to create a meaningful initial state, the
action to take, and the observable result. Run commands from the repository root.

## Isolated test state

Use a fresh private workspace for scenarios that do not explicitly define a
different setup:

```bash
. ./scripts/manual-environment.sh
```

The helper creates a unique private directory and exports
`KANTRIP_MANUAL_ROOT`, `KANTRIP_DATABASE`, and `XDG_RUNTIME_DIR` into the current
shell. It refuses to replace an active manual environment.

For scenarios that use two terminals, copy the resolved values of these three
variables into the second terminal. Remove the exact temporary directory after
the scenarios are complete:

```bash
rm -rf "$KANTRIP_MANUAL_ROOT"
unset KANTRIP_MANUAL_ROOT KANTRIP_DATABASE XDG_RUNTIME_DIR
```

## Edit a profile without changing its identity

### Setup

Initialize the isolated state, then create a profile without a Registry:

```bash
uv run --locked kantrip add manual \
  --bootstrap-servers localhost:9092 \
  --description 'Initial description'
uv run --locked kantrip show manual
```

Record the profile `id` shown by the second command.

### Exercise

Add a Registry without specifying a provider, update the other editable fields,
then remove the Registry explicitly:

```bash
uv run --locked kantrip edit manual \
  --bootstrap-servers broker-1.example.com:9092,broker-2.example.com:9092 \
  --description 'Shared development cluster' \
  --label environment=development \
  --label owner=platform \
  --registry-url http://registry.example.com:8081
uv run --locked kantrip show manual

uv run --locked kantrip edit manual \
  --clear-description \
  --remove-label owner \
  --remove-registry
uv run --locked kantrip show manual
```

### Expected result

- The first edit keeps the original profile `id`, replaces the broker list and
  description, adds both labels, and stores `provider: confluent` with
  `schema.registry.url`.
- The second edit removes the complete description and Registry fields, removes
  only the `owner` label, and preserves `environment`.
- Running `kantrip edit manual` without any edit option fails without changing
  the profile.

## Repair a pending database migration

This scenario recreates the exact schema produced after migration sequence 1;
it is not a compatibility path for a former storage format.

### Setup

Initialize isolated state and create a current database:

```bash
uv run --locked kantrip add migration-check
sqlite3 "$KANTRIP_DATABASE" \
  "DROP TABLE credential_reconciliation; DELETE FROM schema_migrations WHERE sequence = 2; PRAGMA user_version = 1;"
chmod 600 "$KANTRIP_DATABASE"
```

Confirm the simulated starting state:

```bash
sqlite3 "$KANTRIP_DATABASE" \
  "SELECT sequence, name FROM schema_migrations ORDER BY sequence; PRAGMA user_version;"
```

### Exercise

```bash
uv run --locked kantrip doctor --no-color
sqlite3 "$KANTRIP_DATABASE" \
  "SELECT name FROM sqlite_master WHERE name = 'credential_reconciliation';"

uv run --locked kantrip doctor --repair --no-color
sqlite3 "$KANTRIP_DATABASE" \
  "SELECT sequence, name FROM schema_migrations ORDER BY sequence; PRAGMA user_version;"
find "$(dirname "$KANTRIP_DATABASE")" -maxdepth 1 \
  -name 'profiles.db.pre-migration-*' -print
```

### Expected result

- The normal doctor run reports one pending database migration and does not
  create the reconciliation table or a backup.
- The repair pass reports `Applied database migrations: 2`.
- The final query contains sequences 1 and 2 and reports `user_version` 2.
- Exactly one private backup exists. Its name contains a UTC timestamp and a
  unique suffix, and its mode is `0600`.
- A later migration creates a new backup instead of overwriting this one.

The final doctor status may still report unrelated local client or credential
backend problems; evaluate the Profiles and Repair sections for this scenario.

## Accept only an approved credential backend

### Setup

Initialize isolated state. On macOS, unlock the user's login Keychain. On Linux,
run a Secret Service-compatible backend in the current desktop or user session.

### Exercise

Inspect the backend and Kantrip's classification:

```bash
uv run --locked keyring diagnose
uv run --locked kantrip doctor --verbose --no-color
```

Then force keyring's null backend for one invocation:

```bash
PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring \
  uv run --locked kantrip doctor --no-color
echo $?
```

### Expected result

- The real backend is reported as `macOS Keychain` on macOS or `Secret Service`
  on Linux. Verbose output includes its non-secret implementation name.
- The null-backend invocation reports
  `Credential store backend is unavailable or unsafe` and exits with status 1.
- Neither command reads or prints a stored credential value.

## Reconcile an orphaned credential reference

This scenario creates a synthetic credential plus the exact cleanup intent that
would remain after a partial cross-store mutation.

### Setup

Complete the approved-backend scenario first, initialize isolated state, and
create a profile:

```bash
uv run --locked kantrip add journal-check
uv run --locked python - <<'PY'
import os
import sqlite3
from pathlib import Path

from kantrip.profiles import load_profiles
from kantrip.reconciliation import queue_secret_cleanup
from kantrip.secret_store import load_secret_store, secret_reference

path = Path(os.environ["KANTRIP_DATABASE"])
profile = load_profiles(path).profile("journal-check")
reference = secret_reference(profile["id"], "oauth/client-secret")
load_secret_store().set(reference, "kantrip-manual-synthetic-secret")
with sqlite3.connect(path) as connection:
    queue_secret_cleanup(connection, reference)
print(reference)
PY
```

The printed reference is an identifier, not the credential value.

### Exercise

```bash
uv run --locked kantrip doctor --no-color
sqlite3 "$KANTRIP_DATABASE" \
  "SELECT secret_reference FROM credential_reconciliation;"

uv run --locked kantrip doctor --repair --no-color
sqlite3 "$KANTRIP_DATABASE" \
  "SELECT COUNT(*) FROM credential_reconciliation;"
```

### Expected result

- Normal doctor reports one pending credential reconciliation entry and leaves
  both the journal record and credential untouched.
- Repair reports `Credential reconciliation: removed 1`.
- The final SQL query returns `0`; repeating repair reports no pending entries.
- The exact synthetic credential is absent from the OS store. No backend-wide
  listing or wildcard deletion occurs.

## Observe an active session without cleaning it

### Setup

Initialize isolated state and create a profile:

```bash
uv run --locked kantrip add active-session
```

Copy the isolated-state environment variables into a second terminal.

### Exercise

In terminal A, start a supervised command and leave it running:

```bash
uv run --locked kantrip exec active-session -- sh -c 'while :; do sleep 60; done'
```

In terminal B, inspect and repair while terminal A is still running:

```bash
uv run --locked kantrip doctor --no-color
uv run --locked kantrip doctor --repair --no-color
```

Return to terminal A, press Ctrl-C, and inspect its exit status:

```bash
echo $?
```

### Expected result

- Doctor reports `Runtime active: 1 session` as a valid state.
- Repair reports one active session and does not remove its directory.
- Ctrl-C terminates the managed command and produces exit status 130.
- A later doctor run reports zero active sessions.

## Recover an abandoned stale session

### Setup

Initialize isolated state and create a realistic crash artifact. `os._exit`
intentionally bypasses `SessionRuntime.close`, so the OS releases the liveness
lock while leaving valid session metadata behind:

```bash
uv run --locked python - <<'PY'
import os

from kantrip.runtime import create_session_runtime

runtime = create_session_runtime()
runtime.mark_running()
print(runtime.path, flush=True)
os._exit(0)
PY
```

Backdate only that synthetic marker so the five-minute safety window does not
slow the check:

```bash
uv run --locked python - <<'PY'
import json
import os
from pathlib import Path

root = Path(os.environ["XDG_RUNTIME_DIR"]) / "kantrip" / "sessions"
sessions = list(root.glob("session-*"))
assert len(sessions) == 1
marker_path = sessions[0] / "session.json"
marker = json.loads(marker_path.read_text(encoding="utf-8"))
marker["createdAt"] = 0
marker_path.write_text(
    json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
marker_path.chmod(0o600)
PY
```

### Exercise

```bash
uv run --locked kantrip doctor --no-color
uv run --locked kantrip doctor --repair --no-color
uv run --locked kantrip doctor --no-color
```

### Expected result

- The first doctor run reports one stale session and does not remove it.
- Repair reports `Sessions: removed 1 stale`.
- The final doctor run reports zero stale sessions.

## Refuse an invalid runtime entry

### Setup

Initialize isolated state and create an unexpected direct child in the runtime:

```bash
mkdir -p "$XDG_RUNTIME_DIR/kantrip/sessions/unexpected"
chmod 700 "$XDG_RUNTIME_DIR/kantrip" \
  "$XDG_RUNTIME_DIR/kantrip/sessions" \
  "$XDG_RUNTIME_DIR/kantrip/sessions/unexpected"
```

### Exercise

```bash
uv run --locked kantrip doctor --no-color
uv run --locked kantrip doctor --repair --no-color
test -d "$XDG_RUNTIME_DIR/kantrip/sessions/unexpected"
echo $?
```

### Expected result

- Doctor reports one invalid runtime session and exits with status 1.
- Repair also exits with status 1 and does not delete the invalid entry.
- The `test` command returns 0, confirming the fail-closed behavior.

Remove only the synthetic entry after the check:

```bash
rmdir "$XDG_RUNTIME_DIR/kantrip/sessions/unexpected"
```

## Run the real adapter smoke workflow

This workflow is retained independently of the manual lifecycle checks. It
validates external client integration against real Kafka and Registry services.

### Setup

Install the supported local clients, then start the sandbox:

```bash
docker compose --project-directory sandbox up -d
docker compose --project-directory sandbox ps
```

Wait until Kafka, Confluent Schema Registry, and Apicurio report healthy.

### Exercise

Run the default Confluent-compatible workflow and the interactive shell
contract:

```bash
uv run --locked python -m sandbox
uv run --locked python -m sandbox \
  --shell bash --shell zsh --shell fish
```

Exercise Apicurio once through its Confluent compatibility API and once through
its native Core API:

```bash
uv run --locked python -m sandbox apicurio-ccompat \
  --profile sandbox-apicurio-ccompat \
  --registry-provider confluent \
  --registry-url http://localhost:8082/apis/ccompat/v7

uv run --locked python -m sandbox apicurio-native \
  --profile sandbox-apicurio-native \
  --registry-provider apicurio \
  --registry-url http://localhost:8082/apis/registry/v3
```

### Expected result

- Every installed compatible adapter reports a passed result.
- The shell run completes for Bash, Zsh, and Fish without exceeding its timeout.
- The compatibility endpoint exercises Confluent framing; the native endpoint
  exercises Apicurio discovery and Kaskade's native configuration.
- Temporary profiles and topics are removed even when a later probe fails.

Stop and remove the sandbox services when finished:

```bash
docker compose --project-directory sandbox down -v
```
