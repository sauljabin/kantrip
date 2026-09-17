# Development Instructions

This guide describes the development environment and repository workflows. Run
user-facing exploratory checks from [Manual Testing](MANUAL_TESTING.md).

## Documentation audiences

- End users: [Usage](USAGE.md) and [Compatibility](COMPATIBILITY.md). Show installed
  `kantrip` commands and current support; keep sandbox and `uv run` instructions
  in developer documentation.
- Developers: this guide, [Architecture](ARCHITECTURE.md),
  [Threat Model](THREAT_MODEL.md), and [Manual Testing](MANUAL_TESTING.md).
- AI agents: [Agent Instructions](AGENT.md),
  [Release Checklist](RELEASE_CHECKLIST.md), and the temporary [MVP roadmap](MVP.md).

Architecture owns technical decisions and their rationale; agent instructions
own implementation conventions. Record each decision when its implementation
lands, keep the threat model aligned, and link to the canonical explanation.

## Setup

Install uv:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# or on macOS
brew install uv
```

Install locked development dependencies and the Git hooks:

```bash
uv sync --locked
uv run pre-commit install
```

Run the editable CLI directly from the checkout:

```bash
uv run kantrip --help
uv run kantrip --version
```

## Development scripts

Apply code styles:

```bash
uv run python -m scripts.styles
```

Run type, formatting, lint, spelling, and workflow analysis:

```bash
uv run python -m scripts.analyze
```

Run the offline unit tests:

```bash
uv run python -m scripts.tests
```

Run the shell contract independently:

```bash
KANTRIP_REQUIRED_SHELLS=bash,zsh,fish \
  uv run --locked python -m scripts.verify_shell_contract
```

The shell contract uses generated fake clients and PTYs; it does not require
Kafka, Docker, kcat, Kaskade, or Java.

Generate the deterministic Rich README banner:

```bash
uv run python -m scripts.banner
```

Shared script code belongs in `scripts/__init__.py`; modules are executable
workflows. Keep test utilities with their suite and the reproducible integration
environment in `sandbox`.

## Schema and application environment

Keep the profile schema in `schemas/`, synthetic examples in `examples/`, and
private-data-free fixtures with their tests.

When an application variable changes, update `USAGE.md`, architecture guidance,
and tests together. Before the first release, obsolete contracts may be replaced
without backward compatibility with development commits. Reject incompatible
state explicitly; do not add aliases or silently reset user data.

Implement the sequential PRs in [MVP.md](MVP.md), including their acceptance
criteria and affected documentation. Its manual first-release QA is a separate
human release gate; the offline suite and sandbox smoke remain required.

## Database migrations

Keep database evolution independent from product releases. The transaction
engine, migration commands, and explicit `MigrationChain` registry live together
in `kantrip/migrations.py`. Each `SqlMigration` subclass has one positive integer
`sequence`, an immutable descriptive name, and an immutable SQL tuple. The
sequence is its only identity and order; the product version that applies it is
history metadata, not part of the migration name. The engine derives a checksum
from that complete identity and payload.

Before merging a database change:

- Choose the next sequence on `main`; resolve branch collisions before merge.
- Keep each schema change in its own `SqlMigration` subclass and append one
  instance to the explicit registry. Never edit or renumber a migration that has
  appeared in a release; add a forward migration.
- Update the schema, `schema_migrations`, and `PRAGMA user_version` in the same
  bounded transaction.
- Test a fresh database, idempotent reopen, supported upgrade paths, rollback,
  uniquely timestamped backups, checksum tampering, missing or future sequences,
  and concurrent initialization.
- Keep migrations inside the package. Do not require a user-run SQL file or add
  a separate migration command.
- Do not implement compatibility for an unreleased database shape. The first
  published release establishes the oldest supported migration state.
- Keep normal `doctor` execution read-only. Explicit maintenance belongs only in
  `doctor --repair`.

Tests should create a repository at a selected migration sequence and then run
the current chain. A published package release is not required to establish an
upgrade fixture.

## Credential store development

Kantrip uses Python `keyring` only as an adapter to approved native stores:
macOS Keychain on macOS and Secret Service-compatible backends on Linux. Null,
plaintext, encrypted-file, chained, and unknown backends must fail closed.

Inspect the backend selected by the development environment with:

```bash
uv run --locked keyring diagnose
uv run --locked kantrip doctor --verbose
```

Tests must inject synthetic in-memory implementations of Kantrip's narrow
`SecretStore` protocol. They must not read or modify a developer's real
credential store.

Secret-bearing profile changes use the transaction engine in
`kantrip/credential_mutations.py`. Each replacement receives a new credential
UUID. The engine commits an exact cleanup record before writing the store,
switches the profile and advances its expected revision in SQLite, then retires
the superseded reference. Profile removal commits the row deletion and cleanup
records before it contacts the credential backend.

Tests for this boundary use deterministic in-process failpoints and
`tests/mutation_worker.py` subprocess barriers. Keep this matrix intact when a
new credential owner or input path is added:

| Cut or race | Required invariant |
| --- | --- |
| Intent committed before the first store write | Old profile or absent add; exact cleanup record |
| Store writes and then raises, including the second of several writes | Every possibly written immutable reference remains journaled |
| Validation, CAS, or database failure before commit | Previous complete generation remains active; no old secret is retired |
| Lost commit acknowledgement | Exact row and journal inspection yields committed (`3`), unchanged (`1`), or unknown (`4`) |
| Secret deleted before journal-row removal | Committed generation survives; repair is idempotent |
| Two adds, edit/edit, edit/remove, or stale remove after recreate | One serial valid outcome; no lost update or wrong-UUID deletion |
| Repair versus staging or snapshot versus rotation | No live value is deleted; readers resolve one coherent generation |
| Live reference appears in cleanup journal | Integrity error and no credential deletion |

The subprocess suite uses pipe barriers and real `SIGKILL`, never timing sleeps,
at durable intent, store readback, post-commit reload, and post-delete journal
boundaries. It verifies exact journal/profile state, private permissions,
idempotent repair, and secret-free output. These tests model process failure,
not hardware power loss or an approved OS store restart; release QA records
those platform boundaries separately. Assertions may inspect references and
journal rows, but must never include a real credential value in diagnostic
output.

## Sandbox services and smoke workflow

The sandbox is a local Kind laboratory with one operator-managed Strimzi Kafka
cluster, Keycloak, Schema Registry, Apicurio Registry, and cert-manager.
Install Docker, Kind, kubectl, and Helm before using it. Component versions are
pinned in `sandbox/versions.env`. The topology decision, trust boundary, and
non-production provisioning limits are canonical in
[Sandbox verification topology](ARCHITECTURE.md#sandbox-verification-topology).

Create, inspect, and delete the environment with:

```bash
uv run --locked python -m sandbox up
uv run --locked python -m sandbox status
uv run --locked python -m sandbox credentials
uv run --locked python -m sandbox down
```

Generated credentials, CA material, and Java client property files are private
and ignored below `sandbox/.state`. The lifecycle command does not print their
values. The generated assignment file is a laboratory input, not Kantrip's
child environment contract; manual checks source it without blanket `set -a`.
See [manual environment setup](MANUAL_TESTING.md#source-sandbox-variables-without-blanket-export)
for the precedence checks. Every generated source variable uses the
`KANTRIP_SANDBOX_*` namespace, which supervised children scrub. `down` removes
the cluster but retains this private state so another
`up` can reuse the same credentials; remove that exact directory to rotate the
local laboratory credentials.

The loopback-only endpoints are:

- Kafka plaintext: `localhost:9092`
- Kafka TLS: `localhost:9093`
- Kafka SCRAM-SHA-512 over TLS: `localhost:9094`
- Kafka mTLS: `localhost:9095`
- Kafka OAuth over TLS: `localhost:9096`
- Kafka PLAIN over TLS (authorizer fixture): `localhost:9097`
- Kafka SCRAM-SHA-256 over TLS (authorizer fixture): `localhost:9098`
- Schema Registry baseline: `http://localhost:8081`
- Apicurio baseline: `http://localhost:8082`
- Schema Registry with HTTPS and Basic Auth: `https://localhost:8083`
- Apicurio with HTTPS and Basic/OAuth: `https://localhost:8084`
- Schema Registry with HTTPS and OAuth: `https://localhost:8085`
- Keycloak: `https://localhost:8443`

The baseline registry endpoints keep today's unauthenticated Kantrip adapters
executable. Schema Registry uses separate Basic and OAuth processes because its
local JAAS property-file login and OAuth `AuthenticationHandler` are different
server authentication paths; Apicurio accepts both mechanisms on one endpoint.
These secure variants prepare authenticated Registry scenarios without claiming
that those profile fields are already implemented. The single Kafka cluster's
`plaintext` listener means no authentication and no encryption. All other
external mechanisms share its `StandardAuthorizer`; authenticated no-ACL
principals prove that `kantrip ping` does not depend on Kafka resource
authorization. PLAIN JAAS is read from the mounted `kafka-custom-users` Secret.
An idempotent Kubernetes Job authenticates as the dedicated `sandbox-admin`
through the internal TLS/SCRAM-SHA-512 listener and provisions SCRAM-SHA-256
from a private temporary config file. `sandbox-admin` is the only superuser.

Strimzi owns authenticated-client, OAuth, and Registry ACLs. The Job owns only
the `ANONYMOUS` topic/group prefix `kantrip-smoke-` and cluster Describe needed
by the plaintext and server-only TLS smoke; `ANONYMOUS` is not a superuser. The
laboratory does not claim Kubernetes network isolation or production hardening.

The Kafka cluster uses a disposable persistent volume, so broker data, ACLs,
and the SCRAM-SHA-256 credential survive pod restarts but are removed with the
Kind cluster. A pre-unification laboratory containing `Kafka/auth-kantrip` is
rejected with explicit `sandbox down` and `sandbox up` guidance rather than
being deleted silently. Both Apicurio instances use KafkaSQL with
separate journal and snapshot topics configured for delete cleanup and infinite
retention. Their registry data therefore survives an Apicurio pod restart without
leaking data between the baseline and authenticated variants.
Schema Registry topics are declared as Strimzi `KafkaTopic` resources with
`cleanup.policy=compact` and the same kebab-case names used by the Kafka
clients: `schema-registry`, `schema-registry-secure`, and
`schema-registry-oauth`.

Run the adapter smoke workflow against the active services:

```bash
uv run --locked python -m scripts.smoke
```

Include the interactive shell adapters with:

```bash
uv run --locked python -m scripts.smoke \
  --shell bash --shell zsh --shell fish
```

Run the authenticated lifecycle, direct producer/consumer/admin, all-shell,
invalid-authorization, and no-ACL ping matrix with:

```bash
uv run --locked python -m scripts.auth_smoke
```

The smoke workflow remains a pre-commit hook. It creates isolated temporary
profiles and topics and cleans them up. Keep the sandbox running when committing
changes that execute the hook. See [Manual Testing](MANUAL_TESTING.md) for
provider-specific invocations and expected results.

## Build artifacts

Build the wheel and source distribution:

```bash
uv build --clear
```

Verify versions, entry points, the packaged profile schema, public examples, and
required documentation:

```bash
uv run --locked python -m scripts.verify_release dist
```

Exact `vMAJOR.MINOR.PATCH` tags, optionally suffixed with PEP 440 `aN`, `bN`, or
`rcN`, define releases. Untagged builds use hatch-vcs development metadata; its
fallback only bootstraps empty or exported checkouts.

## Architecture and security

- Stable design decisions: [Architecture](ARCHITECTURE.md)
- Planned MVP work: [MVP roadmap](MVP.md)
- Assets, threats, controls, and limitations: [Threat model](THREAT_MODEL.md)
- Private vulnerability reporting: [Security policy](SECURITY.md)

## Release

Git tags define versions and GitHub Releases hold history; do not maintain a
static version or changelog. Record each candidate with the
[release checklist](RELEASE_CHECKLIST.md).

Before releasing, ensure `main` is current, clean, and passing:

```bash
git switch main
git pull --ff-only origin main
git status --short
uv lock --check
uv run --locked python -m scripts.analyze
uv run --locked python -m scripts.tests
uv build --clear
uv run --locked python -m scripts.verify_release dist
```

Push an annotated stable tag (`vMAJOR.MINOR.PATCH`) or PEP 440 pre-release tag
(`aN`, `bN`, or `rcN`). The protected workflow validates it against `main`,
builds and verifies once, installs the wheel, generates Conventional Commit
notes, attests artifacts, awaits approval, publishes through PyPI trusted
publishing, and creates the GitHub Release. Kantrip has no Docker release job.
