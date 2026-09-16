# Manual Testing

These checks complement the offline test suite and the automated sandbox smoke
workflow. Each scenario describes how to create a meaningful initial state, the
action to take, and the observable result. Run commands from the repository root.
These scenarios describe current behavior. The pending
[first-release manual QA checklist](MVP.md#manual-qa--first-release-checklist)
is kept with the roadmap until its commands are implemented; move those checks
here as each owning PR lands. Human QA supplements both offline and smoke tests.

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

Current `exec` gives its documented Kafka/config/session values precedence over
exported values and removes inactive Registry URL/config variables. Other
exported sandbox credentials are still inherited. Interactive startup files can
also change connection variables after the initial injection. See
[the exact current precedence limits](USAGE.md#precedence-and-inherited-sandbox-variables);
the stronger contract and regression checks belong to MVP PR 1.

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
request and no change to the outer environment. This checks only the currently
owned variable set, not future secret scrubbing or post-startup shell repair.

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
- Running `kantrip edit manual` without any edit option fails without changing
  the profile.

## Create a verified TLS profile

### Setup

Initialize isolated state. The repository contains a synthetic public CA
certificate for this configuration-only check.

### Exercise

```bash
uv run --locked kantrip add tls-manual \
  --bootstrap-servers kafka.example.com:9093 \
  --transport tls \
  --ca-file tests/fixtures/kafka-ca.pem
uv run --locked kantrip describe tls-manual
uv run --locked kantrip exec tls-manual -- sh -c \
  'grep -E "^(security.protocol|ssl.ca.location|ssl.endpoint.identification.algorithm|enable.ssl.certificate.verification)=" "$KAFKA_LIBRDKAFKA_CONFIG_FILE"; stat -f "%Lp" "$KANTRIP_SESSION_DIR/kafka-ca.pem" 2>/dev/null || stat -c "%a" "$KANTRIP_SESSION_DIR/kafka-ca.pem"'
```

### Expected result

- `describe` reports transport `tls`, custom TLS trust, and no authentication.
- The generated librdkafka configuration uses `security.protocol=SSL`, enables
  hostname verification, and references the session-owned CA file.
- The copied CA file has mode `0600` and disappears with the private session.
- Replacing the fixture with malformed text causes `add` to fail without
  creating the profile.

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

The sandbox is a loopback-only Kind cluster. It runs one Strimzi Kafka cluster
with several listeners, two baseline registries, authenticated endpoints for
both registry products, Keycloak, and a cert-manager-issued local CA. It
deliberately excludes Amazon MSK IAM and Confluent Cloud.

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

List the preconfigured Strimzi users and inspect each declarative user without
reading its generated Secret:

```bash
kubectl --context kind-kantrip-sandbox -n kantrip-sandbox get kafkausers
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
http GET http://localhost:8081/subjects
http GET http://localhost:8082/apis/registry/v3/search/artifacts
```

### Expected result

- Kubernetes reports both `KafkaUser` resources as ready. Their YAML identifies
  SCRAM-SHA-512 and TLS authentication but contains no credential value.
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

- The four Apicurio KafkaSQL topics are ready with `cleanup.policy: delete`,
  `retention.ms: -1`, and `retention.bytes: -1`.
- The three Schema Registry topics are ready with `cleanup.policy: compact`:
  `schema-registry`, `schema-registry-secure`, and `schema-registry-oauth`.
- The artifact identity and its version 1 content are returned after each restart.
- The baseline and secure Apicurio instances never share a journal or snapshot
  topic.

## Exercise every Kafka listener

One broker cluster exposes all of these connections. `PLAINTEXT` means no
authentication and no encryption; it is not SASL/PLAIN. Password authentication
uses SCRAM-SHA-512 over verified TLS.

### Setup

Start the Kubernetes sandbox and load the generated environment in this shell.
Repeat the command here deliberately so this section can be run independently:

```bash
set +a
. sandbox/.state/credentials.env
```

The lifecycle tool has already written private Java client properties.

### Exercise

Run the current Kantrip adapter smoke test against the baseline listener:

```bash
uv run --locked python -m scripts.smoke
uv run --locked python -m scripts.smoke \
  --shell bash --shell zsh --shell fish
```

Exercise the server-authenticated TLS listener through today's Kantrip profile
contract:

```bash
export KANTRIP_DATABASE="$PWD/sandbox/.state/profiles.db"
uv run --locked kantrip add sandbox-plaintext \
  --bootstrap-servers localhost:9092
uv run --locked kantrip ping sandbox-plaintext

uv run --locked kantrip add sandbox-tls \
  --bootstrap-servers localhost:9093 \
  --transport tls \
  --ca-file "$KANTRIP_SANDBOX_CA"
uv run --locked kantrip ping sandbox-tls
uv run --locked kantrip exec sandbox-tls -- kafka-topics --list
```

Kantrip's schema and shared renderers accept SCRAM-SHA-512 and mTLS, but the
current CLI input, session adapters, and `ping` still accept only `plaintext`
and unauthenticated verified `tls` profiles. The native Kafka client checks
below remain infrastructure-level exercises until those integration steps land;
OAuth is not yet represented by the profile schema.

Exercise the prepared authenticated listeners directly with the official Kafka
CLI. These property files contain credentials and must remain private:

```bash
kafka-topics --bootstrap-server localhost:9094 \
  --command-config sandbox/.state/kafka-scram.properties --list
kafka-topics --bootstrap-server localhost:9095 \
  --command-config sandbox/.state/kafka-mtls.properties --list
KAFKA_OPTS='-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=https://localhost:8443/realms/kantrip/protocol/openid-connect/token' \
  kafka-topics --bootstrap-server localhost:9096 \
  --command-config sandbox/.state/kafka-oauth.properties --list
```

### Expected result

- The smoke workflow continues to pass through plaintext `localhost:9092` and
  cleans up its temporary profile and topic.
- `sandbox-plaintext` reaches `localhost:9092`, and `sandbox-tls` reaches
  `localhost:9093` with the exported CA.
- Kantrip verifies the sandbox CA and reaches the TLS listener on `9093`.
- The native client reaches SCRAM-SHA-512 on `9094`, mTLS on `9095`, and OAuth
  client credentials on `9096`.
- Kantrip does not yet accept the three authenticated profiles; this section
  validates the environment that will exercise those contracts when implemented.

## Exercise authenticated registries

The authenticated Registry endpoints require HTTPS. Schema Registry exposes
separate Basic and OAuth processes because the local JAAS and OAuth server paths
cannot share this self-contained setup; Apicurio accepts both mechanisms on one
endpoint. HTTPie sessions are generated below private `sandbox/.state`, so
credentials and tokens do not appear in command arguments.

### Setup

Start the sandbox and load the generated environment in this shell:

```bash
set +a
. sandbox/.state/credentials.env
```

Create profiles for the two baseline Registry endpoints. These profiles contain
only non-authenticated `http://` Registry URLs, which is the Registry contract
currently supported by Kantrip:

```bash
export KANTRIP_DATABASE="$PWD/sandbox/.state/profiles.db"
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
```

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
  GET https://localhost:8084/apis/registry/v3/search/artifacts
```

Then exercise Basic and OAuth independently with read-only HTTPie sessions:

```bash
http --verify "$KANTRIP_SANDBOX_CA" \
  --session-read-only sandbox/.state/schema-registry-basic.json \
  GET https://localhost:8083/subjects
http --verify "$KANTRIP_SANDBOX_CA" \
  --session-read-only sandbox/.state/schema-registry-oauth.json \
  GET https://localhost:8085/subjects

http --verify "$KANTRIP_SANDBOX_CA" \
  --session-read-only sandbox/.state/apicurio-basic.json \
  GET https://localhost:8084/apis/registry/v3/search/artifacts
http --verify "$KANTRIP_SANDBOX_CA" \
  --session-read-only sandbox/.state/apicurio-oauth.json \
  GET https://localhost:8084/apis/registry/v3/search/artifacts
```

Inspect the public certificate chain without disabling verification:

```bash
openssl s_client -connect localhost:8083 -servername localhost \
  -CAfile "$KANTRIP_SANDBOX_CA" </dev/null
openssl s_client -connect localhost:8085 -servername localhost \
  -CAfile "$KANTRIP_SANDBOX_CA" </dev/null
openssl s_client -connect localhost:8084 -servername localhost \
  -CAfile "$KANTRIP_SANDBOX_CA" </dev/null
```

### Expected result

- Anonymous secure Registry requests return HTTP 401.
- Basic and OAuth requests return successful JSON responses from both products.
- OpenSSL reports `Verify return code: 0 (ok)` for all three TLS endpoints.
- No credential or bearer token appears in process arguments, command output, or
  committed files.
- `sandbox-schema-registry` and `sandbox-apicurio` describe the two baseline
  Registry URLs without embedding credentials.
- The baseline Registry profiles are visible through `kantrip describe`.
- The authenticated Registry variants are HTTPie readiness checks until the
  corresponding Kantrip profile fields and adapters are implemented.

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
