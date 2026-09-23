# Manual Testing

These checks complement the unit suite and the automated sandbox E2E suite.
Each scenario describes how to create a meaningful initial state, the
action to take, and the observable result. Run commands from the repository root.
These scenarios describe current behavior. The pending
[first-release manual QA checklist](MVP.md#manual-qa--first-release-checklist)
is kept with the roadmap until its commands are implemented; move those checks
here as each owning PR lands. Human QA supplements both unit and E2E tests.

## Isolated test state

Use a fresh private workspace for scenarios that do not explicitly define a
different setup:

```bash
export KANTRIP_MANUAL_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/kantrip-manual.XXXXXX")"
chmod 700 "$KANTRIP_MANUAL_ROOT"
mkdir -m 700 "$KANTRIP_MANUAL_ROOT/runtime"
export KANTRIP_DATABASE="$KANTRIP_MANUAL_ROOT/data/profiles.db"
export XDG_RUNTIME_DIR="$KANTRIP_MANUAL_ROOT/runtime"
```

For scenarios that use two terminals, copy the resolved values of these three
variables into the second terminal. Remove the exact temporary directory after
the scenarios are complete:

```bash
rm -rf "$KANTRIP_MANUAL_ROOT"
```

## Source sandbox variables without blanket export

The generated `sandbox/.state/credentials.env` contains private laboratory
values and shell assignments. These checks use public paths/identifiers in the
parent shell and private client files for authentication. They do not require
exporting every sandbox secret to every subsequently launched child.

Use a dedicated parent shell, disable automatic export, and source the file
before invoking Kantrip:

```bash
set +a
. sandbox/.state/credentials.env
```

`set +a` does not remove export attributes already present. If you previously
used `set -a` or `export` for these names, close that laboratory shell and start
from a parent that has not exported them, or explicitly unexport/unset the
sandbox names before reloading. Do not print `env`, `set`, the credential file,
or complete client configs to verify this. Do not source credentials inside
`kantrip exec`, or in shell startup files used by the session.

`exec` removes the complete `KAFKA_*`, `SCHEMA_REGISTRY_*`, `APICURIO_*`, and
`KANTRIP_SANDBOX_*` namespaces plus JVM option injection variables before it
adds the selected profile values. Bash, Zsh, and Fish repeat this after startup
files and restore `KCAT_CONFIG`, owned environment, and shims. The generated
sandbox assignments all use the `KANTRIP_SANDBOX_*` prefix and therefore cannot
become an ambient child credential source even when the parent used `set -a`.

The laboratory exports PKCS12 mTLS properties for external clients. This does
not imply that Kantrip can import PKCS12 or execute every authentication mode
available in the laboratory. Check the current compatibility matrix before
selecting a Kantrip scenario.

### Check current direct-child precedence without a broker

Create an isolated plaintext profile, then use synthetic conflicting values in
a disposable subshell. No real credential is needed or printed:

```bash
uv run --locked kantrip add env-manual --bootstrap-servers localhost:9092
(
  export KAFKA_BOOTSTRAP_SERVERS=unselected.invalid:19092
  export KAFKA_SECURITY_PROTOCOL=SASL_SSL
  export KCAT_CONFIG=/synthetic/unselected.conf
  export SCHEMA_REGISTRY_URL=http://unselected.invalid
  export APICURIO_REGISTRY_URL=http://unselected.invalid
  uv run --locked kantrip exec env-manual -- python -c '
import os
assert os.environ["KAFKA_BOOTSTRAP_SERVERS"] == "localhost:9092"
assert os.environ["KAFKA_SECURITY_PROTOCOL"] == "PLAINTEXT"
assert os.environ["KCAT_CONFIG"] != "/synthetic/unselected.conf"
assert "SCHEMA_REGISTRY_URL" not in os.environ
assert "APICURIO_REGISTRY_URL" not in os.environ
print("Selected profile environment verified")'
  test "$KAFKA_BOOTSTRAP_SERVERS" = unselected.invalid:19092
  test "$KCAT_CONFIG" = /synthetic/unselected.conf
)
```

Expect child assertions and parent-shell assertions to pass, with no network
request and no change to the outer environment. Repeat with an exported
`KANTRIP_SANDBOX_SYNTHETIC_SECRET`, `JAVA_TOOL_OPTIONS`, and startup-file
override of `KCAT_CONFIG`; none may survive in the child after startup.
Unrelated application variables must survive.

## Edit a profile without changing its identity

### Setup

Initialize the isolated state, then create a profile without a Registry:

```bash
uv run --locked kantrip add manual \
  --bootstrap-servers localhost:9092 \
  --description 'Initial description'
uv run --locked kantrip describe manual
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
uv run --locked kantrip describe manual

uv run --locked kantrip edit manual \
  --clear-description \
  --remove-label owner \
  --remove-registry
uv run --locked kantrip describe manual
```

### Expected result

- The first edit keeps the original profile `id`, replaces the broker list and
  description, adds both labels, and stores `provider: confluent` with
  `schema.registry.url`.
- The second edit removes the complete description and Registry fields, removes
  only the `owner` label, and preserves `environment`.
- Running `kantrip edit manual` without options opens the field editor. Choosing
  `done` immediately fails without changing the profile; replacing a field
  commits one revision.

## Create a verified TLS profile

### Setup

Initialize isolated state, then generate a disposable public CA in the manual
temporary root:

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$KANTRIP_MANUAL_ROOT/manual-ca.key" \
  -out "$KANTRIP_MANUAL_ROOT/manual-ca.pem" \
  -subj '/CN=Kantrip Manual Test CA' -days 1
chmod 600 "$KANTRIP_MANUAL_ROOT/manual-ca.key" "$KANTRIP_MANUAL_ROOT/manual-ca.pem"
```

### Exercise

```bash
uv run --locked kantrip add tls-manual \
  --bootstrap-servers kafka.example.com:9093 \
  --transport tls \
  --ca-file "$KANTRIP_MANUAL_ROOT/manual-ca.pem"
uv run --locked kantrip describe tls-manual
uv run --locked kantrip exec tls-manual -- sh -c \
  'grep -E "^(security.protocol|ssl.ca.location|ssl.endpoint.identification.algorithm|enable.ssl.certificate.verification)=" "$KAFKA_LIBRDKAFKA_CONFIG_FILE"; stat -f "%Lp" "$KANTRIP_SESSION_DIR/kafka-ca.pem" 2>/dev/null || stat -c "%a" "$KANTRIP_SESSION_DIR/kafka-ca.pem"'
```

### Expected result

- `describe` reports transport `tls`, custom TLS trust, and no authentication.
- The generated librdkafka configuration uses `security.protocol=SSL`, enables
  hostname verification, and references the session-owned CA file.
- The copied CA file has mode `0600` and disappears with the private session.
- Replacing the disposable CA with malformed text causes `add` to fail without
  creating the profile.

## Exercise authenticated Kafka and Registry lifecycle

Start the sandbox, then run its disposable authenticated acceptance matrix:

```bash
uv run --locked python -m scripts.tests --suite e2e
```

The E2E suite exercises allowed and no-ACL Kafka identities for
PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and mTLS; native OAuth; unauthenticated
plaintext and server-only TLS; Bash, Zsh, and Fish; invalid passwords and client
certificate; wrong CA and hostname; and an unavailable broker. It also creates
Kantrip profiles for Confluent and Apicurio Basic/OAuth plus Confluent mTLS,
proves the configured read query and authentication gate, rejects invalid
credentials, and verifies long-lived Registry renewal before a revoked client
causes a new token acquisition to fail without decoding the final record.
Allowed Kafka identities can use only `kantrip-auth-`, OAuth can use
only `kantrip-oauth-`, and unauthenticated smoke clients can use only
`kantrip-smoke-`. Each no-ACL identity must complete `ping` and then receive a
resource authorization denial.

The matrix uses verified TLS plus PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and
mTLS. For each mechanism it creates the profile through the no-echo prompt,
runs `ping`, exercises direct producer, consumer, admin and kcat operations,
and invokes adapters through Bash, Zsh, and Fish. OAuth consumers stay alive
past the 15-second access-token lifetime and consume a second record after
native refresh. Its authorizer-enabled broker also proves that a no-ACL
principal passes Kafka `ping` while resource creation fails. It uses an isolated
profile database and removes the temporary profiles and keyring entries on
completion. Inspect process argv during a run when performing the release
secrecy check; no credential should appear there or in terminal output.

Rotate `kafka/password` with:

```bash
uv run --locked kantrip edit authenticated \
  --replace-secret kafka/password
uv run --locked kantrip describe authenticated --output json
```

The revision advances once, the safe state is `stored`, the reference and value
are absent from output, and new sessions use only the replacement. Repeat mTLS
with a matching encrypted key, then confirm a mismatched key is rejected before
the profile changes. Remove the profile once with a declined confirmation and
once with `--force`; only the latter removes the captured UUID/revision.

During a live session, edit the same profile and run:

```bash
uv run --locked kantrip doctor authenticated --sessions --verbose
```

The session retains its captured revision and is reported as older than the
current profile without being classified as corrupt. Removing and recreating
the same display name must not associate the new UUID with the old session.

## Filter and describe profiles

### Setup

Initialize the isolated state and create profiles with overlapping labels:

```bash
uv run --locked kantrip add production \
  --label environment=production \
  --label owner=platform
uv run --locked kantrip add analytics \
  --label environment=production \
  --label owner=data
```

### Exercise

```bash
uv run --locked kantrip list \
  --label environment=production \
  --label owner=platform
uv run --locked kantrip list --output json
uv run --locked kantrip describe production --output yaml
uv run --locked kantrip describe production --output yaml --no-color
```

### Expected result

- The filtered human list contains only `production` and displays both labels.
- On a colored TTY, JSON and YAML output use syntax highlighting. With
  `--no-color` or when redirected, structured output is unstyled and contains no
  ANSI escapes.
- JSON list output omits profile IDs and revisions.
- YAML describe output contains the profile ID and revision but no arbitrary
  client properties, secret values, or internal credential references.

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
reference = secret_reference(profile["id"], "kafka/oauth/client-secret")
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

## Start and inspect the Kubernetes sandbox

The sandbox is a loopback-only Kind cluster. It runs one persistent,
authorizer-enabled, multi-listener Strimzi Kafka cluster, two baseline
registries, authenticated endpoints for both registry products, Keycloak, and a
cert-manager-issued local CA. It deliberately excludes Amazon MSK IAM and
Confluent Cloud.

### Setup

Install Docker, Kind, kubectl, Helm, HTTPie, and at least one supported Kafka CLI.
Create the cluster and inspect its workloads:

```bash
uv run --locked python -m sandbox up
uv run --locked python -m sandbox status
uv run --locked python -m sandbox credentials
```

The last command lists private files and variable names, never values. Generated
credentials and client configurations live below ignored `sandbox/.state` with
private permissions. Load their paths and identifiers as shell variables,
following [the source-environment precautions](#source-sandbox-variables-without-blanket-export):

```bash
set +a
. sandbox/.state/credentials.env
```

### Exercise

List the single Strimzi cluster, its persistent node pool, and the
preconfigured native users without reading generated Secrets:

```bash
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox get kafkausers
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox get kafka,kafkanodepool
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  get kafkauser kantrip-scram -o yaml
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  get kafkauser kantrip-mtls -o yaml
```

Inspect the non-secret service discovery endpoints with HTTPie and the exported
CA. A TLS error is a failure; do not use `--verify=no`:

```bash
http --verify "$KANTRIP_SANDBOX_CA" \
  GET https://localhost:8443/realms/kantrip/.well-known/openid-configuration
http GET http://localhost:8081/schemas/types
http GET http://localhost:8082/apis/registry/v3/system/info
```

### Expected result

- Kubernetes reports one Kafka cluster, one persistent node pool, and every
  `KafkaUser` resource as ready. User YAML identifies SCRAM-SHA-512 and TLS
  authentication but contains no credential value.
- Keycloak discovery reports issuer `https://localhost:8443/realms/kantrip`.
- Both baseline Registry requests return successful JSON responses.
- Every exposed host port is bound to `127.0.0.1`, not all interfaces.

## Verify Apicurio persistence through KafkaSQL

Both Apicurio deployments use isolated KafkaSQL journal and snapshot topics.
Kafka uses a persistent volume inside Kind, so this check survives both an
Apicurio pod restart and a Kafka broker pod restart. Deleting the Kind cluster
still intentionally deletes all sandbox data.

### Setup

Start the sandbox, then inspect the topics and their retention policy:

```bash
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  get kafkatopics apicurio-journal apicurio-snapshots \
  apicurio-secure-journal apicurio-secure-snapshots \
  registry-events \
  schema-registry schema-registry-secure \
  schema-registry-oauth
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  get kafkatopic apicurio-journal -o yaml
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  get kafkatopic schema-registry -o yaml
```

### Exercise

Create a synthetic Avro artifact through the baseline HTTP endpoint:

```bash
http POST http://localhost:8082/apis/registry/v3/groups/sandbox/artifacts \
  artifactId=restart-proof artifactType=AVRO \
  firstVersion:='{"content":{"content":"{\"type\":\"string\"}","contentType":"application/json"}}'
```

Restart only Apicurio, wait for KafkaSQL to rebuild its local state, and read
the artifact again:

```bash
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  rollout restart deployment/apicurio
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  rollout status deployment/apicurio --timeout=5m
http GET \
  http://localhost:8082/apis/registry/v3/groups/sandbox/artifacts/restart-proof
http GET \
  http://localhost:8082/apis/registry/v3/groups/sandbox/artifacts/restart-proof/versions/1/content
```

For the stronger storage check, restart the broker and repeat the read:

```bash
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox delete pod kantrip-dual-role-0
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  wait --for=create pod/kantrip-dual-role-0 --timeout=5m
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  wait pod/kantrip-dual-role-0 --for=condition=Ready --timeout=5m
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  rollout restart deployment/apicurio
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  rollout status deployment/apicurio --timeout=5m
http GET \
  http://localhost:8082/apis/registry/v3/groups/sandbox/artifacts/restart-proof
http GET \
  http://localhost:8082/apis/registry/v3/groups/sandbox/artifacts/restart-proof/versions/1/content
```

### Expected result

- The four Apicurio KafkaSQL topics and shared `registry-events` topic are ready
  with `cleanup.policy: delete`,
  `retention.ms: -1`, and `retention.bytes: -1`.
- The three Schema Registry topics are ready with `cleanup.policy: compact`:
  `schema-registry`, `schema-registry-secure`, and `schema-registry-oauth`.
- The artifact identity and its version 1 content are returned after each restart.
- The baseline and secure Apicurio instances never share a journal or snapshot
  topic.

## Exercise every Kafka listener

The single Strimzi cluster exposes plaintext, TLS, SCRAM-SHA-512, mTLS, OAuth,
PLAIN, and SCRAM-SHA-256 connections under one `StandardAuthorizer`.
`PLAINTEXT` means no authentication and no encryption; it is not SASL/PLAIN.
PLAIN and SCRAM-SHA-256 use verified TLS on `localhost:9097` and
`localhost:9098`. Registry services and the provisioning Job use only the
internal TLS/SCRAM-SHA-512 listener on port 9099.

### Setup

Start the Kubernetes sandbox and create a private directory for this section.
The helper reads one Kubernetes Secret field, but must never be invoked alone
for a password or key: capture public identifiers in command substitutions and
redirect certificate material to private files. This setup is independent of
the Registry section below.

```bash
uv run --locked python -m sandbox up
umask 077
export KANTRIP_MANUAL_KAFKA_ROOT="$PWD/sandbox/.state/manual-kafka"
install -d -m 700 "$KANTRIP_MANUAL_KAFKA_ROOT"
export KANTRIP_DATABASE="$KANTRIP_MANUAL_KAFKA_ROOT/profiles.db"
sandbox_secret_field() {
  kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
    get secret "$1" -o json |
    python3 -c 'import base64, json, sys; sys.stdout.buffer.write(base64.b64decode(json.load(sys.stdin)["data"][sys.argv[1]]))' "$2"
}
export KANTRIP_SANDBOX_CA="$KANTRIP_MANUAL_KAFKA_ROOT/ca.crt"
sandbox_secret_field sandbox-root-ca ca.crt > "$KANTRIP_SANDBOX_CA"
```

The sandbox lifecycle also wrote private Java client properties below
`sandbox/.state`. Do not display those files or export their contents.

### Exercise

Run the complete automated acceptance matrix against the already-running
sandbox. It includes Bash, Zsh, and Fish:

```bash
uv run --locked python -m scripts.tests --suite e2e
```

#### Plaintext, no authentication

This listener has neither TLS nor a credential, so no Kubernetes value is
needed. `ping` should prove broker connectivity only.

```bash
uv run --locked kantrip add sandbox-plaintext \
  --bootstrap-servers localhost:9092
uv run --locked kantrip ping sandbox-plaintext
```

#### Verified TLS, no client authentication

The CA file in the shared setup comes from `secret/sandbox-root-ca` key
`ca.crt`. The broker certificate must verify without `--insecure` or disabled
hostname checks.

```bash
uv run --locked kantrip add sandbox-tls \
  --bootstrap-servers localhost:9093 \
  --transport tls \
  --ca-file "$KANTRIP_SANDBOX_CA"
uv run --locked kantrip ping sandbox-tls
uv run --locked kantrip exec sandbox-tls -- kafka-topics --list
```

#### SCRAM-SHA-512 over verified TLS

The public username is the Strimzi `kantrip-scram` KafkaUser Secret's name. At
Kantrip's no-echo password prompt use that Secret's `password` field; do not
print it or put it in a command argument.

```bash
export KANTRIP_SANDBOX_KAFKA_SCRAM_USERNAME="$(
  kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
    get secret kantrip-scram -o jsonpath='{.metadata.name}'
)"
uv run --locked kantrip add sandbox-scram \
  --bootstrap-servers localhost:9094 \
  --transport tls \
  --ca-file "$KANTRIP_SANDBOX_CA" \
  --auth scram-sha-512 \
  --username "$KANTRIP_SANDBOX_KAFKA_SCRAM_USERNAME"
uv run --locked kantrip ping sandbox-scram
uv run --locked kantrip exec sandbox-scram -- kafka-topics --list
```

Expect an authenticated broker connection and resource results bounded by
this principal's ACLs.

#### Kafka client mTLS

Write `secret/kantrip-mtls` keys `user.crt` and `user.key` to private files. The
shell variables hold only paths; Kantrip validates the certificate/key pair.

```bash
export KANTRIP_SANDBOX_KAFKA_MTLS_CERTIFICATE="$KANTRIP_MANUAL_KAFKA_ROOT/user.crt"
export KANTRIP_SANDBOX_KAFKA_MTLS_KEY="$KANTRIP_MANUAL_KAFKA_ROOT/user.key"
sandbox_secret_field kantrip-mtls user.crt > "$KANTRIP_SANDBOX_KAFKA_MTLS_CERTIFICATE"
sandbox_secret_field kantrip-mtls user.key > "$KANTRIP_SANDBOX_KAFKA_MTLS_KEY"
uv run --locked kantrip add sandbox-mtls \
  --bootstrap-servers localhost:9095 \
  --transport tls \
  --ca-file "$KANTRIP_SANDBOX_CA" \
  --auth mtls \
  --client-certificate-file "$KANTRIP_SANDBOX_KAFKA_MTLS_CERTIFICATE" \
  --client-key-file "$KANTRIP_SANDBOX_KAFKA_MTLS_KEY"
uv run --locked kantrip ping sandbox-mtls
uv run --locked kantrip exec sandbox-mtls -- kafka-topics --list
```

Expect verified server TLS plus the configured client identity. A mismatched
key should be rejected before the profile is changed.

#### SASL/PLAIN over verified TLS

The username comes from `secret/kafka-custom-users` key `plain-username`; use
the matching `plain-password` only at Kantrip's no-echo prompt. PLAIN here is
authenticated and encrypted, unlike the plaintext listener above.

```bash
export KANTRIP_SANDBOX_KAFKA_PLAIN_USERNAME="$(
  sandbox_secret_field kafka-custom-users plain-username
)"
uv run --locked kantrip add sandbox-plain \
  --bootstrap-servers localhost:9097 \
  --transport tls \
  --ca-file "$KANTRIP_SANDBOX_CA" \
  --auth plain \
  --username "$KANTRIP_SANDBOX_KAFKA_PLAIN_USERNAME"
uv run --locked kantrip ping sandbox-plain
uv run --locked kantrip exec sandbox-plain -- kafka-topics --list
```

Expect an authenticated broker connection; neither the command line nor
Kantrip output should contain the password.

#### SCRAM-SHA-256 over verified TLS

The provisioning Job owns this identity. Read only the username from
`secret/kafka-custom-users` key `scram-256-username`; enter the matching
`scram-256-password` at the no-echo prompt.

```bash
export KANTRIP_SANDBOX_KAFKA_SCRAM_256_USERNAME="$(
  sandbox_secret_field kafka-custom-users scram-256-username
)"
uv run --locked kantrip add sandbox-scram-256 \
  --bootstrap-servers localhost:9098 \
  --transport tls \
  --ca-file "$KANTRIP_SANDBOX_CA" \
  --auth scram-sha-256 \
  --username "$KANTRIP_SANDBOX_KAFKA_SCRAM_256_USERNAME"
uv run --locked kantrip ping sandbox-scram-256
uv run --locked kantrip exec sandbox-scram-256 -- kafka-topics --list
```

Expect success before and after the broker restart below, without rerunning
the provisioning Job.

#### Kafka OAuth

`secret/keycloak-realm` key `realm.json` contains the client ID and secret.
Extract only the public ID here, then enter its matching secret at Kantrip's
no-echo prompt. The token endpoint and broker use the sandbox CA.

```bash
export KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_ID="$(
  sandbox_secret_field keycloak-realm realm.json |
    python3 -c 'import json, sys; print(next(c["clientId"] for c in json.load(sys.stdin)["clients"] if c["clientId"].endswith("-kafka")))'
)"
uv run --locked kantrip add sandbox-oauth \
  --bootstrap-servers localhost:9096 \
  --transport tls \
  --ca-file "$KANTRIP_SANDBOX_CA" \
  --auth oauth \
  --oauth-token-url https://localhost:8443/realms/kantrip/protocol/openid-connect/token \
  --oauth-client-id "$KANTRIP_SANDBOX_KAFKA_OAUTH_CLIENT_ID" \
  --oauth-ca-file "$KANTRIP_SANDBOX_CA"
uv run --locked kantrip ping sandbox-oauth
uv run --locked kantrip exec sandbox-oauth -- kafka-topics --list
```

Expect broker authentication and only the OAuth principal's permitted resource
operations. The E2E suite keeps Java and librdkafka clients alive across token
expiry and revocation; a one-shot `ping` does not prove refresh.

For all password and OAuth cases, the named Kubernetes field has the matching
value in private `sandbox/.state/credentials.env`, generated by `sandbox up`.
Transfer it to Kantrip's no-echo prompt through your approved private manual
procedure. Never print it in a terminal, put it in shell history or arguments,
or export it to child processes.

Exercise the prepared authenticated listeners directly with the official Kafka
CLI. These property files contain credentials and must remain private. Each
check is independent of the Kantrip profile created above.

#### Native SCRAM-SHA-512 client

```bash
kafka-topics --bootstrap-server localhost:9094 \
  --command-config sandbox/.state/kafka-scram.properties --list
```

#### Native mTLS client

```bash
kafka-topics --bootstrap-server localhost:9095 \
  --command-config sandbox/.state/kafka-mtls.properties --list
```

#### Native SASL/PLAIN client

```bash
kafka-topics --bootstrap-server localhost:9097 \
  --command-config sandbox/.state/kafka-plain.properties --list
```

#### Native SCRAM-SHA-256 client

```bash
kafka-topics --bootstrap-server localhost:9098 \
  --command-config sandbox/.state/kafka-scram-256.properties --list
```

#### Native Kafka OAuth client

The JVM option permits the sandbox token endpoint without disabling TLS
verification; the private properties file supplies the client credentials.

```bash
KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://localhost:8443/realms/kantrip/protocol/openid-connect/token' \
  kafka-topics --bootstrap-server localhost:9096 \
  --command-config sandbox/.state/kafka-oauth.properties --list
```

Prove that the unified cluster is not reformatted on restart and that its
SCRAM-SHA-256 user remains available without rerunning the provisioning Job:

```bash
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  delete pod/kantrip-dual-role-0
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  wait --for=create pod/kantrip-dual-role-0 --timeout=5m
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
  wait pod/kantrip-dual-role-0 --for=condition=Ready --timeout=5m
uv run --locked python -m scripts.tests --suite e2e
uv run --locked python -m sandbox up
uv run --locked python -m scripts.tests --suite e2e
```

### Expected result

- The E2E workflow passes through plaintext `localhost:9092` and
  cleans up its temporary profile and topic.
- `sandbox-plaintext` reaches `localhost:9092`, and `sandbox-tls` reaches
  `localhost:9093` with the exported CA.
- Kantrip verifies the sandbox CA and reaches the TLS listener on `9093`.
- Kantrip and the native client reach SCRAM-SHA-512 on `9094` and mTLS on
  `9095`; the native OAuth client reaches `9096`; authenticated acceptance
  reaches PLAIN on `9097` and SCRAM-SHA-256 on `9098`.
- Kafka `ping` reports the authenticated exchange without requiring topic or
  cluster ACLs; the subsequent topic command remains subject to broker ACLs.
- After the unified broker restart, the complete authenticated matrix still
  passes without rerunning the provisioning Job.
- A second `sandbox up` completes idempotently and the matrix remains green.
- Wrong credentials, client identity, CA, hostname, and unavailable broker
  cases fail cleanly without revealing credential values.

After recording results, remove only the seven manual Kafka profiles:

```bash
for profile in sandbox-plaintext sandbox-tls sandbox-scram sandbox-mtls \
  sandbox-plain sandbox-scram-256 sandbox-oauth; do
  uv run --locked kantrip remove "$profile" --force
done
```

## Exercise authenticated registries

The authenticated Registry endpoints require HTTPS. Schema Registry exposes
separate Basic and OAuth processes because the local JAAS and OAuth server paths
cannot share this self-contained setup; Apicurio accepts both mechanisms on one
endpoint. HTTPie sessions are generated below private `sandbox/.state`, so
credentials and tokens do not appear in command arguments.

### Setup

Start the sandbox, then prepare a private manual-test directory. This helper
reads one Kubernetes Secret field; use it only in command substitutions or
redirects, never by itself for a password or private key. It does not print a
whole Secret or store credentials in command arguments.

```bash
uv run --locked python -m sandbox up
umask 077
export KANTRIP_MANUAL_SANDBOX_ROOT="$PWD/sandbox/.state/manual-registry"
install -d -m 700 "$KANTRIP_MANUAL_SANDBOX_ROOT"
export KANTRIP_DATABASE="$KANTRIP_MANUAL_SANDBOX_ROOT/profiles.db"
sandbox_secret_field() {
  kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
    get secret "$1" -o json |
    python3 -c 'import base64, json, sys; sys.stdout.buffer.write(base64.b64decode(json.load(sys.stdin)["data"][sys.argv[1]]))' "$2"
}
export KANTRIP_SANDBOX_CA="$KANTRIP_MANUAL_SANDBOX_ROOT/ca.crt"
sandbox_secret_field sandbox-root-ca ca.crt > "$KANTRIP_SANDBOX_CA"
```

Run the automated matrix separately; it supplements the human checks below.
For a baseline comparison without Registry authentication, create two profiles:

```bash
uv run --locked kantrip add sandbox-schema-registry \
  --bootstrap-servers localhost:9092 \
  --registry-provider confluent \
  --registry-url http://localhost:8081
uv run --locked kantrip add sandbox-apicurio \
  --bootstrap-servers localhost:9092 \
  --registry-provider apicurio \
  --registry-url http://localhost:8082/apis/registry/v3
uv run --locked kantrip describe sandbox-schema-registry
uv run --locked kantrip describe sandbox-apicurio
uv run --locked python -m scripts.tests --suite e2e
```

Each case below starts from the shared setup and uses a distinct profile. The
Kubernetes commands capture only public IDs in shell variables. At Kantrip's
no-echo prompt, supply the matching private value generated by `sandbox up`;
the exact Kubernetes source is named in each case. Do not print Secret content,
put a password in an argument, or export it into child environments.

#### Confluent Schema Registry Basic

`schema-registry-auth/password.properties` contains the Basic identity. Extract
only the username; use its matching password at Kantrip's no-echo prompt. The
same value is in the private `sandbox/.state/credentials.env` generated by the
sandbox lifecycle.

```bash
export KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_USERNAME="$(
  sandbox_secret_field schema-registry-auth password.properties |
    python3 -c 'import sys; print(sys.stdin.readline().split(": ", 1)[0])'
)"
uv run --locked kantrip add qa-registry-basic -b localhost:9092 \
  --registry-provider confluent --registry-url https://localhost:8083 \
  --registry-ca-file "$KANTRIP_SANDBOX_CA" --registry-auth basic \
  --registry-username "$KANTRIP_SANDBOX_SCHEMA_REGISTRY_BASIC_USERNAME"
uv run --locked kantrip ping qa-registry-basic
uv run --locked kantrip describe qa-registry-basic --output json
```

Expect Kafka and Registry success with no password in output. To test failure,
replace `registry/password` with a wrong synthetic value at the prompt, run
`ping`, then repeat the replacement with the original sandbox password. Kafka
must retain its successful result while Registry fails, and both must succeed
after restoration:

```bash
uv run --locked kantrip edit qa-registry-basic --replace-secret registry/password
uv run --locked kantrip ping qa-registry-basic
uv run --locked kantrip edit qa-registry-basic --replace-secret registry/password
uv run --locked kantrip ping qa-registry-basic
```

#### Confluent Schema Registry OAuth

The client ID and secret are in `keycloak-realm/realm.json`. Parse the public ID
without printing the embedded client secret; enter that secret at Kantrip's
no-echo prompt. The Registry and token endpoint both use the sandbox CA.

```bash
export KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_ID="$(
  sandbox_secret_field keycloak-realm realm.json |
    python3 -c 'import json, sys; print(next(c["clientId"] for c in json.load(sys.stdin)["clients"] if c["clientId"].endswith("-schema-registry")))'
)"
uv run --locked kantrip add qa-registry-oauth -b localhost:9092 \
  --registry-provider confluent --registry-url https://localhost:8085 \
  --registry-ca-file "$KANTRIP_SANDBOX_CA" --registry-auth oauth \
  --registry-oauth-token-url https://localhost:8443/realms/kantrip/protocol/openid-connect/token \
  --registry-oauth-client-id "$KANTRIP_SANDBOX_SCHEMA_REGISTRY_OAUTH_CLIENT_ID" \
  --registry-oauth-ca-file "$KANTRIP_SANDBOX_CA" \
  --registry-oauth-logical-cluster lsrc-sandbox
uv run --locked kantrip ping qa-registry-oauth
uv run --locked kantrip describe qa-registry-oauth --output json
```

Expect the protected `/subjects?limit=1` read and anonymous-denial control to
succeed. This one-shot ping does not prove token renewal; the E2E matrix tests
expiry and revocation in a long-lived client.

#### Confluent Schema Registry mTLS

Read the client certificate and key from `secret/registry-mtls-client` into
private files. The exported variables name files, never the key bytes.

```bash
export KANTRIP_SANDBOX_REGISTRY_MTLS_CERTIFICATE="$KANTRIP_MANUAL_SANDBOX_ROOT/registry-client.crt"
export KANTRIP_SANDBOX_REGISTRY_MTLS_KEY="$KANTRIP_MANUAL_SANDBOX_ROOT/registry-client.key"
sandbox_secret_field registry-mtls-client tls.crt > "$KANTRIP_SANDBOX_REGISTRY_MTLS_CERTIFICATE"
sandbox_secret_field registry-mtls-client tls.key > "$KANTRIP_SANDBOX_REGISTRY_MTLS_KEY"
uv run --locked kantrip add qa-registry-mtls -b localhost:9092 \
  --registry-provider confluent --registry-url https://localhost:8086 \
  --registry-ca-file "$KANTRIP_SANDBOX_CA" --registry-auth mtls \
  --registry-client-certificate-file "$KANTRIP_SANDBOX_REGISTRY_MTLS_CERTIFICATE" \
  --registry-client-key-file "$KANTRIP_SANDBOX_REGISTRY_MTLS_KEY"
uv run --locked kantrip ping qa-registry-mtls
uv run --locked kantrip describe qa-registry-mtls --output json
```

Expect verified TLS and the configured client-certificate exchange. Repeat a
probe without a client certificate against this server-required mTLS fixture to
confirm denial; do not interpret a server that merely requests an optional
certificate as proof of client authentication.

#### Native Apicurio Basic

The public client ID is `registry-clients/apicurio-client-id`; its matching
password is `registry-clients/apicurio-client-secret`. Capture only the ID in a
shell variable and enter the secret at Kantrip's no-echo prompt.

```bash
export KANTRIP_SANDBOX_APICURIO_CLIENT_ID="$(
  sandbox_secret_field registry-clients apicurio-client-id
)"
uv run --locked kantrip add qa-apicurio-basic -b localhost:9092 \
  --registry-provider apicurio \
  --registry-url https://localhost:8084/apis/registry/v3 \
  --registry-ca-file "$KANTRIP_SANDBOX_CA" --registry-auth basic \
  --registry-username "$KANTRIP_SANDBOX_APICURIO_CLIENT_ID"
uv run --locked kantrip ping qa-apicurio-basic
uv run --locked kantrip describe qa-apicurio-basic --output json
```

Expect a successful protected version-search read and anonymous denial on the
same URL. This proves the fixture's `sr-readonly` access to that query, not
write access or a particular artifact permission.

#### Native Apicurio OAuth

Read the public ID from `registry-clients` again; `keycloak-realm/realm.json`
contains the corresponding OAuth client secret. Enter it at Kantrip's no-echo
prompt without placing it in an environment variable.

```bash
export KANTRIP_SANDBOX_APICURIO_CLIENT_ID="$(
  sandbox_secret_field registry-clients apicurio-client-id
)"
uv run --locked kantrip add qa-apicurio-oauth -b localhost:9092 \
  --registry-provider apicurio \
  --registry-url https://localhost:8084/apis/registry/v3 \
  --registry-ca-file "$KANTRIP_SANDBOX_CA" --registry-auth oauth \
  --registry-oauth-token-url https://localhost:8443/realms/kantrip/protocol/openid-connect/token \
  --registry-oauth-client-id "$KANTRIP_SANDBOX_APICURIO_CLIENT_ID" \
  --registry-oauth-ca-file "$KANTRIP_SANDBOX_CA" \
  --registry-oauth-scope openid
uv run --locked kantrip ping qa-apicurio-oauth
uv run --locked kantrip describe qa-apicurio-oauth --output json
```

Expect the same protected version-search read as the Basic case. Scope handling
requires published Kaskade 5.0.1+ only when a Kaskade Registry consumer is
launched; `ping` itself does not prove native client refresh.

Remove the exact manual profiles after recording results:

```bash
for profile in sandbox-schema-registry sandbox-apicurio qa-registry-basic \
  qa-registry-oauth qa-registry-mtls qa-apicurio-basic qa-apicurio-oauth; do
  uv run --locked kantrip remove "$profile" --force
done
```

For each authenticated profile, a successful Registry result proves only the
documented list/search read and the anonymous denial on that exact route.
Kantrip's normal output must not contain credentials. The fixed-bearer mode
has no sandbox issuer fixture, and the native Apicurio mTLS profile has no
server-required mTLS fixture here; record those manual cells as unsupported
by this laboratory, not as passes. The automated E2E matrix covers real
Confluent console and Kaskade Registry decoding and long-lived OAuth renewal.

Refresh the short-lived OAuth bearer sessions without printing either client
secrets or tokens:

```bash
uv run --locked python -m sandbox oauth-session schema-registry
uv run --locked python -m sandbox oauth-session apicurio
```

### Exercise

First confirm that anonymous requests are rejected:

```bash
http --verify "$KANTRIP_SANDBOX_CA" GET https://localhost:8083/subjects
http --verify "$KANTRIP_SANDBOX_CA" GET https://localhost:8085/subjects
http --verify "$KANTRIP_SANDBOX_CA" \
  GET 'https://localhost:8084/apis/registry/v3/search/versions?limit=1'
```

Then exercise Basic and OAuth independently with read-only HTTPie sessions:

```bash
http --verify "$KANTRIP_SANDBOX_CA" \
  --session-read-only sandbox/.state/schema-registry-basic.json \
  GET 'https://localhost:8083/subjects?limit=1'
http --verify "$KANTRIP_SANDBOX_CA" \
  --session-read-only sandbox/.state/schema-registry-oauth.json \
  GET 'https://localhost:8085/subjects?limit=1'

http --verify "$KANTRIP_SANDBOX_CA" \
  --session-read-only sandbox/.state/apicurio-basic.json \
  GET 'https://localhost:8084/apis/registry/v3/search/versions?limit=1'
http --verify "$KANTRIP_SANDBOX_CA" \
  --session-read-only sandbox/.state/apicurio-oauth.json \
  GET 'https://localhost:8084/apis/registry/v3/search/versions?limit=1'
```

Inspect the public certificate chain without disabling verification:

```bash
openssl s_client -connect localhost:8083 -servername localhost \
  -CAfile "$KANTRIP_SANDBOX_CA" </dev/null
openssl s_client -connect localhost:8085 -servername localhost \
  -CAfile "$KANTRIP_SANDBOX_CA" </dev/null
openssl s_client -connect localhost:8084 -servername localhost \
  -CAfile "$KANTRIP_SANDBOX_CA" </dev/null
openssl s_client -connect localhost:8086 -servername localhost \
  -CAfile "$KANTRIP_SANDBOX_CA" \
  -cert "$KANTRIP_SANDBOX_REGISTRY_MTLS_CERTIFICATE" \
  -key "$KANTRIP_SANDBOX_REGISTRY_MTLS_KEY" </dev/null
```

### Expected result

- Anonymous secure Registry requests return HTTP 401.
- Basic and OAuth requests return successful JSON responses from both products.
- OpenSSL reports `Verify return code: 0 (ok)` for all four TLS endpoints; the
  mTLS endpoint rejects a connection that omits its client certificate.
- No credential or bearer token appears in process arguments, command output, or
  committed files.
- `sandbox-schema-registry` and `sandbox-apicurio` describe the two baseline
  Registry URLs without embedding credentials.
- The baseline Registry profiles are visible through `kantrip describe`.
- Kantrip `ping` validates Confluent's protected `/subjects?limit=1` and native
  Apicurio's `/search/versions?limit=1`, plus an anonymous 401/403 control on
  the same URL. Empty valid result sets succeed.
- Invalid Registry credentials and disabled OAuth clients fail without exposing
  credential values; re-enabling each client restores a successful probe.
- The Apicurio service account has the standard `sr-readonly` realm role;
  authenticated-read bypass and anonymous read access remain disabled.

### Native Registry OAuth refresh matrix

Run the expiry/revocation sequence once for each distinct implementation, not
for every equivalent format, direct command, or shell shim:

1. Confluent's Java Schema Registry client through one console consumer. The
   Avro, JSON Schema, and Protobuf console wrappers share this OAuth client and
   private property mapping.
2. Confluent's Python `SchemaRegistryClient` through Kaskade's Confluent
   provider.
3. Kaskade's native `ApicurioClient` through its Apicurio provider. Scope-free
   profiles retain compatibility with earlier Kaskade releases; scoped profiles
   require the published Kaskade 5.0.1 release or newer.

For each case, use its own IdP client and the 15-second access-token lifetime.
Read a real schema, wait for expiry, then read a different uncached schema or
reference in the same still-running process. The second read must obtain a new
token and return the correct schema. Disable that IdP client, allow the current
token to expire, and request a third uncached schema. The next refresh must fail
cleanly without exposing the secret or token. Re-enable the client afterward.
Do not require an already issued token to become invalid immediately unless the
IdP explicitly guarantees that behavior.

For Confluent's Java wrappers, Kantrip passes the private Java properties file
to both the Kafka command and the formatter/reader. It also passes the validated,
credential-free Registry URL as a formatter/reader property so the wrapper's
`http://localhost:8081` default cannot take precedence. OAuth token endpoints
are allowlisted through the session-owned JVM options, and custom CAs are
rendered as a Java PEM truststore used by both Registry and token requests.

Keep short adapter coverage for all six Confluent console wrappers, Kaskade's
two providers, and Bash/Zsh/Fish. Those checks prove argument/configuration
routing only; they must not add duplicate token-expiry waits. Basic, mTLS, and
fixed bearer modes have no OAuth renewal case.

## Remove or rotate the sandbox

### Exercise

Delete only the managed cluster:

```bash
uv run --locked python -m sandbox down
```

The private generated state remains reusable. To rotate every local sandbox
credential, remove exactly that ignored state directory before recreating the
cluster:

```bash
rm -rf "$PWD/sandbox/.state"
uv run --locked python -m sandbox up
```

### Expected result

- `down` removes `kantrip-sandbox` and no unrelated Kind cluster.
- Removing the exact private state rotates all generated passwords, OAuth client
  secrets, client certificates, and HTTPie sessions on the next `up`.
