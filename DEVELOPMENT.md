# Development Instructions

## Setup

Install uv:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
# or on macOS
brew install uv
```

Install locked development dependencies:

```bash
uv sync --locked
```

Run the editable CLI with:

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

Shared script code belongs in `scripts/__init__.py`; modules are executable
workflows. Keep test utilities with their suite and manual utilities in
`sandbox`, outside the offline tests.

## Schema and application environment

Keep the profile schema in `schemas/`, synthetic examples in `examples/`, and
private-data-free fixtures with their tests.

When an application variable changes, update `USAGE.md`, architecture guidance,
and tests together.

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

Endpoints are Kafka `localhost:9092`, Confluent Schema Registry
`http://localhost:8081`, and Apicurio's compatibility and Core APIs at
`http://localhost:8082/apis/ccompat/v7` and
`http://localhost:8082/apis/registry/v3`. Versions live in `sandbox/.env`.

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

The smoke run checks Kafka and registry connectivity; topic creation, listing,
production, consumption, groups, configs, ACLs, and broker APIs; all six registry
console adapters; kcat; Kaskade; and cleanup. It requires each Kafka command
group and all six Confluent registry commands. Colored terminals animate running
steps; plain output uses stable status labels.

Pass Apicurio's Confluent-compatible URL as shown above to run the same adapter
workflow against Apicurio instead.

Repeat `--shell` to test installed interactive shells after direct commands. The
three-shell example verifies Bash, Zsh, and Fish with PTYs, profile visibility,
path restoration, and installed adapters.

The smoke script is also a pre-commit hook. Keep the sandbox running when making
commits; this remains a local integration check rather than part of the offline
unit-test suite.

GitHub Actions tests Bash, Zsh, Fish, and history persistence with generated fake
clients—no Kafka, kcat, Kaskade, Java, or Docker. Python assertions read a
temporary safe-metadata log, deleted afterward and shown only on failure.

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

Examples use Confluent's unsuffixed names; Apache Kafka archives use `.sh`, such
as `kafka-topics.sh`.

### Schema Registry adapter workflow

For Confluent Avro, JSON Schema, and Protobuf clients, use one topic per wire
format:

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

kcat 1.7+ receives the registry URL when an Avro `-s` deserializer is selected:

```bash
uv run kantrip exec sandbox -- kcat \
  -C -t kantrip-avro -o beginning -e -s value=avro
```

Kaskade 5+ reads that URL from its private `[registry]` section:

```bash
uv run kantrip exec sandbox -- kaskade consumer \
  --topic kantrip-avro --earliest -v registry
```

Change `--topic` to inspect the JSON Schema or Protobuf topics.

Other adapter checks:

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

The sandbox has no authorizer, so validate ACLs with `--version`; ACL operations
require an authorized principal on a configured cluster.

## Architecture and security

- Stable design decisions: [`ARCHITECTURE.md`](ARCHITECTURE.md)
- Planned MVP work: [`MVP.md`](MVP.md)
- Assets, threats, controls, and limitations: [`THREAT_MODEL.md`](THREAT_MODEL.md)
- Private vulnerability reporting: [`SECURITY.md`](SECURITY.md)

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
