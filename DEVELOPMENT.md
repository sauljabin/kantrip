# Development Instructions

This guide describes the development environment and repository workflows. Run
user-facing exploratory checks from [Manual Testing](MANUAL_TESTING.md).

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
workflows. Keep test utilities with their suite and manual environment utilities
in `sandbox`. Automated tests must not import `sandbox`.

## Schema and application environment

Keep the profile schema in `schemas/`, synthetic examples in `examples/`, and
private-data-free fixtures with their tests.

When an application variable changes, update `USAGE.md`, architecture guidance,
and tests together.

## Database migrations

Keep database evolution independent from product releases. The transaction and
history engine lives in `kantrip/migrations/engine.py`. Migration commands and
their explicit `MigrationChain` registry live in `kantrip/migrations/versions.py`.
Each `SqlMigration` subclass has one positive integer `sequence`, an immutable
descriptive name, and an immutable SQL tuple. The sequence is its only identity
and order; the product version that applies it is history metadata, not part of
the migration name. The engine derives a checksum from that complete identity
and payload.

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

## Sandbox services and smoke workflow

The sandbox supplies a single-node plaintext Kafka cluster, Confluent Schema
Registry, and Apicurio Registry. Versions live in `sandbox/.env`.

Start and stop the services with:

```bash
docker compose --project-directory sandbox up -d
docker compose --project-directory sandbox down -v
```

The endpoints are:

- Kafka: `localhost:9092`
- Confluent Schema Registry: `http://localhost:8081`
- Apicurio compatibility API: `http://localhost:8082/apis/ccompat/v7`
- Apicurio Core API: `http://localhost:8082/apis/registry/v3`

Run the adapter smoke workflow against the active services:

```bash
uv run --locked python -m sandbox
```

Include the interactive shell adapters with:

```bash
uv run --locked python -m sandbox \
  --shell bash --shell zsh --shell fish
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
