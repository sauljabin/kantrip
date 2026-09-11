# Development Instructions

## Setup

Install uv:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# or on macOS
brew install uv
```

Create the project environment and install locked development dependencies:

```bash
uv sync --locked
```

The project is installed in editable mode. Run the CLI with:

```bash
uv run kantrip --help
uv run kantrip --version
```

Install pre-commit hooks with:

```bash
uv run pre-commit install
```

## Scripts

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

Generate the deterministic Rich README banner:

```bash
uv run python -m scripts.banner
```

Reusable script code belongs in `scripts/__init__.py`; individual modules are
executable workflows. Tests and fixture utilities remain under their owning test
suite, and manual-environment utilities remain under `sandbox`.

## Schema and application environment

The profile JSON Schema lives in `schemas/`. Synthetic user-facing examples live
in `examples/`. Test-owned fixture copies live under `tests/unit` and must never
contain real credentials or infrastructure details.

Application environment variables are documented in `USAGE.md`. When a variable
changes, update the usage and architecture documentation and the relevant tests
together.

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

An exact `vMAJOR.MINOR.PATCH` tag produces a release version. Untagged builds use
hatch-vcs development metadata; the configured fallback exists only so an empty
or exported pre-release checkout can bootstrap before the first commit.

## Manual sandbox

The sandbox is a manual environment, not a test-fixture provider. Automated tests
must not import it.

Start its three-node Kafka cluster, Kafka-backed Apicurio Registry, and Confluent
Schema Registry:

```bash
docker compose --project-directory sandbox up -d
```

Stop it and remove its volumes:

```bash
docker compose --project-directory sandbox down -v
```

Kafka is available at `localhost:19092`, `localhost:29092`, and
`localhost:39092`. Confluent Schema Registry is at `http://localhost:18081` and
Apicurio's compatibility API is at `http://localhost:18082/apis/ccompat/v7`.
Its native v3 API is at `http://localhost:18082/apis/registry/v3`. Pinned image
versions live in `sandbox/.env`.

Apicurio uses its KafkaSQL storage backend. The one-shot `apicurio-topics`
service creates its journal and snapshot topics with three replicas before the
registry starts, so `docker compose up -d` is the complete startup sequence.

The initial topology is plaintext infrastructure only. Authentication work
extends this one authoritative topology with synthetic TLS, SASL, OAuth, and
identity-provider material instead of introducing unrelated Compose files.

Create a profile for the sandbox and try the supported Kafka clients:

```bash
uv run kantrip add sandbox --bootstrap-server localhost:19092

uv run kantrip exec sandbox -- kcat -L
uv run kantrip exec sandbox -- kafka-topics --list
uv run kantrip exec sandbox -- kafka-topics.sh --list
```

The final command is useful with Apache Kafka distributions that retain the
`.sh` executable suffix. Kantrip supplies the selected bootstrap servers and
temporary client configuration, so do not repeat `--bootstrap-server`, `-F`, or
`--command-config` in these commands.

## Architecture and security

- Stable design decisions: [`ARCHITECTURE.md`](ARCHITECTURE.md)
- Assets, threats, controls, and limitations: [`THREAT_MODEL.md`](THREAT_MODEL.md)
- Private vulnerability reporting: [`SECURITY.md`](SECURITY.md)

## Release

Git tags are the only release-version source. GitHub Releases are the canonical
release history; never edit a static package version or maintained changelog.

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

Create and push an annotated semantic-version tag. The protected release
workflow validates the tag against `main`, builds once, verifies and installs the
wheel, generates Conventional Commit notes, attests the distributions, waits for
approval, publishes through PyPI trusted publishing, and creates the GitHub
Release from the same artifacts. Kantrip has no Docker release job.
