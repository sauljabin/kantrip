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
suite, and manual-environment utilities remain under `sandbox`. The sandbox smoke
command is intentionally separate from the offline test suite.

## Schema and application environment

The profile JSON Schema lives in `schemas/`. Synthetic user-facing examples live
in `examples/`. Test-owned fixture copies live under `tests` and must never
contain private infrastructure details.

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

An exact `vMAJOR.MINOR.PATCH` tag, optionally suffixed with PEP 440 `aN`, `bN`,
or `rcN`, produces a release version. Untagged builds use
hatch-vcs development metadata; the configured fallback exists only so an empty
or exported pre-release checkout can bootstrap before the first commit.

## Manual sandbox

The sandbox is a manual environment, not a test-fixture provider. Automated tests
must not import it.

Start its single-node plaintext Kafka cluster, Confluent Schema Registry, and
Apicurio Registry:

```bash
docker compose --project-directory sandbox up -d
```

Stop it and remove its volumes:

```bash
docker compose --project-directory sandbox down -v
```

Kafka is available at `localhost:9092`; Confluent Schema Registry is available
at `http://localhost:8081`; Apicurio's Confluent-compatible API is available at
`http://localhost:8082/apis/ccompat/v7`, and its Core API is available at
`http://localhost:8082/apis/registry/v3`. Pinned image versions live in
`sandbox/.env`.

With the sandbox running and the supported clients installed locally, run the
adapter smoke checks:

```bash
uv run --locked python -m sandbox
uv run --locked python -m sandbox \
  --shell bash --shell zsh --shell fish
uv run --locked python -m sandbox my-topic \
  --profile sandbox --bootstrap-servers localhost:9092 --keep-topic
uv run --locked python -m sandbox my-apicurio-topic \
  --profile sandbox-apicurio \
  --schema-registry-url http://localhost:8082/apis/ccompat/v7
```

By default, the script checks Kafka and Confluent Schema Registry connectivity, creates
and lists a randomized topic, produces and consumes a record, and exercises the
groups, configs, ACLs, broker API, and all six Schema Registry console adapters.
It also lists the topic with kcat, validates the Kaskade adapter, and deletes the
topic. At least one executable variant for every Apache Kafka command and all
six Confluent Schema Registry console commands must be installed. The check uses
an animated spinner for running steps and styled emoji for results in a colored
terminal. It uses stable text status labels when styling is disabled, including
in CI or when `--no-color` is passed.

Pass Apicurio's Confluent-compatible URL as shown above to run the same adapter
workflow against Apicurio instead.

Repeat `--shell` to add real interactive-subshell checks after the explicit
command pass. A requested shell is required to be installed; the three-shell
command above verifies Bash, Zsh, and Fish with PTYs, including profile
visibility, path restoration, and every installed adapter executable.

The smoke script is also a pre-commit hook. Keep the sandbox running when making
commits; this remains a local integration check rather than part of the offline
unit-test suite.

GitHub Actions separately runs a lightweight shell contract on Bash, Zsh, and
Fish, including history persistence. It uses generated fake client executables instead of installing Kafka,
kcat, Kaskade, Java, or Docker. The Python test owns the assertions and reads a
temporary JSON-lines event log containing command names, safe arguments, config
file modes, and session metadata. Logs are deleted with the test directory and
sanitized output is shown only when a contract fails.

Run that contract independently from the normal unit suite with:

```bash
KANTRIP_REQUIRED_SHELLS=bash,zsh,fish \
  uv run --locked python -m scripts.verify_shell_contract
```

### End-to-end adapter workflow

Create an isolated profile for the running sandbox, then create a topic, produce
two records, and consume exactly those records:

```bash
uv run kantrip add sandbox \
  --bootstrap-servers localhost:9092 \
  --schema-registry-url http://localhost:8081

uv run kantrip exec sandbox -- kafka-topics --create \
  --topic kantrip-development --partitions 1 --replication-factor 1

printf 'first record\nsecond record\n' | \
  uv run kantrip exec sandbox -- kafka-console-producer \
    --topic kantrip-development

uv run kantrip exec sandbox -- kafka-console-consumer \
  --topic kantrip-development --from-beginning --max-messages 2
```

The examples above use Confluent Platform's unsuffixed command names. When using
an Apache Kafka Unix archive, use the corresponding `.sh` executable, such as
`kafka-topics.sh` or `kafka-console-consumer.sh`.

### Schema Registry adapter workflow

The sandbox profile above also supports the Confluent Avro, JSON Schema, and
Protobuf console clients. Create one topic per wire format so each consumer sees
only records encoded with its expected serializer:

```bash
for topic in kantrip-avro kantrip-json-schema kantrip-protobuf; do
  uv run kantrip exec sandbox -- kafka-topics --create \
    --topic "$topic" --partitions 1 --replication-factor 1
done

printf '{"message":"hello from avro"}\n' | \
  uv run kantrip exec sandbox -- kafka-avro-console-producer \
    --topic kantrip-avro \
    --property value.schema='{"type":"record","name":"Event","fields":[{"name":"message","type":"string"}]}'
uv run kantrip exec sandbox -- kafka-avro-console-consumer \
  --topic kantrip-avro --from-beginning --max-messages 1

printf '{"message":"hello from json schema"}\n' | \
  uv run kantrip exec sandbox -- kafka-json-schema-console-producer \
    --topic kantrip-json-schema \
    --property value.schema='{"type":"object","properties":{"message":{"type":"string"}},"required":["message"]}'
uv run kantrip exec sandbox -- kafka-json-schema-console-consumer \
  --topic kantrip-json-schema --from-beginning --max-messages 1

printf '{"message":"hello from protobuf"}\n' | \
  uv run kantrip exec sandbox -- kafka-protobuf-console-producer \
    --topic kantrip-protobuf \
    --property value.schema='syntax = "proto3"; message Event { string message = 1; }'
uv run kantrip exec sandbox -- kafka-protobuf-console-consumer \
  --topic kantrip-protobuf --from-beginning --max-messages 1
```

kcat 1.7+ can decode the Avro topic without an explicit `-r`; Kantrip injects
the profile's Schema Registry URL when an Avro `-s` deserializer is selected:

```bash
uv run kantrip exec sandbox -- kcat \
  -C -t kantrip-avro -o beginning -e -s value=avro
```

Kaskade 5+ reads the same URL from the generated private `[registry]` section.
Select its registry deserializer for Avro, JSON Schema, or Protobuf records:

```bash
uv run kantrip exec sandbox -- kaskade consumer \
  --topic kantrip-avro --earliest -v registry
```

Use the same command with `--topic kantrip-json-schema` or
`--topic kantrip-protobuf` to inspect the other registered formats.

Additional quick checks for the other adapters are:

```bash
uv run kantrip exec sandbox -- kafka-consumer-groups --list
uv run kantrip exec sandbox -- kafka-configs \
  --describe --entity-type topics --entity-name kantrip-development
uv run kantrip exec sandbox -- kafka-acls --version
uv run kantrip exec sandbox -- kafka-broker-api-versions
uv run kantrip exec sandbox -- kcat -L
uv run kantrip exec sandbox -- kaskade admin

uv run kantrip exec sandbox -- kafka-topics --delete \
  --topic kantrip-development
```

The sandbox does not configure a Kafka authorizer, so use `--version` to validate
the ACL adapter there. Listing or changing ACLs requires a cluster with an
authorizer and a suitably authorized principal.

## Architecture and security

- Stable design decisions: [`ARCHITECTURE.md`](ARCHITECTURE.md)
- Planned MVP work: [`MVP.md`](MVP.md)
- Assets, threats, controls, and limitations: [`THREAT_MODEL.md`](THREAT_MODEL.md)
- Private vulnerability reporting: [`SECURITY.md`](SECURITY.md)

## Release

Git tags are the only release-version source. GitHub Releases are the canonical
release history; never edit a static package version or maintained changelog.
Use the reusable [release checklist](RELEASE_CHECKLIST.md) to record preparation
and post-release evidence for each candidate.

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

Create and push an annotated stable (`vMAJOR.MINOR.PATCH`) or PEP 440
pre-release tag (with an `aN`, `bN`, or `rcN` suffix). The protected release
workflow validates the tag against `main`, builds once, verifies and installs the
wheel, generates Conventional Commit notes, attests the distributions, waits for
approval, publishes through PyPI trusted publishing, and creates the GitHub
Release from the same artifacts. Kantrip has no Docker release job.
