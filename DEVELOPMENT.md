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

## Database migrations

Keep database evolution independent from product releases. Each bundled
migration file has one positive integer `sequence`, an immutable descriptive
name, and a checksum. The sequence is its only identity and order; the product
version that applies it is history metadata, not part of the migration name.

Before merging a database change:

- Choose the next sequence on `main`; resolve branch collisions before merge.
- Keep each schema change in its own bundled migration file. Never edit or
  renumber a migration that has appeared in a release; add a new forward file
  instead. One product release may include several migration files.
- Update the schema, `schema_migrations`, and `PRAGMA user_version` in the same
  bounded transaction.
- Test a fresh database, an idempotent reopen, every supported upgrade path,
  rollback, uniquely timestamped backups, checksum tampering, missing or future
  sequences, and concurrent initialization.
- Keep migration definitions inside the package. Do not require users to run a
  SQL file or expose separate migration commands.
- Do not implement compatibility for an unreleased database shape. The first
  published release establishes the oldest supported migration state; one
  product release may still bundle several ordered migration sequences.
- Keep normal `doctor` execution read-only. Exercise explicit maintenance only
  through `doctor --repair`.

Tests should create a repository at a selected migration sequence and then run
the current chain. A published package release is not required to establish an
upgrade fixture.

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
basic Confluent adapter smoke check:

```bash
uv run --locked python -m sandbox
```

This creates an isolated temporary `sandbox` profile for Kafka at
`localhost:9092` and Confluent Schema Registry at `http://localhost:8081`. It
creates a randomly named topic; checks Kafka and registry connectivity; tests
topic creation and listing, production, consumption, groups, configs, ACLs,
broker APIs, kcat, Kaskade, and all six Confluent registry console adapters; and
then deletes the topic and temporary profile.

Add interactive-shell checks to the same Confluent workflow with:

```bash
uv run --locked python -m sandbox \
  --shell bash --shell zsh --shell fish
```

After the direct adapter checks, this opens an isolated Kantrip session in each
requested shell and repeats the installed adapter probes through the generated
session shims. Repeating `--shell` verifies Bash, Zsh, and Fish independently,
including profile visibility and connection-override protection.

Use an explicit topic and connection target when diagnosing a sandbox or when
you want to inspect the smoke topic afterward:

```bash
uv run --locked python -m sandbox my-topic \
  --profile sandbox --bootstrap-servers localhost:9092 --keep-topic
```

`my-topic` replaces the random topic name. `--profile` controls the temporary
Kantrip profile name, `--bootstrap-servers` selects the Kafka brokers, and
`--keep-topic` skips topic deletion. This command still uses the default
Confluent provider and `http://localhost:8081` registry URL.

Test Apicurio's Confluent compatibility API with:

```bash
uv run --locked python -m sandbox my-apicurio-topic \
  --profile sandbox-apicurio-ccompat \
  --registry-provider confluent \
  --registry-url http://localhost:8082/apis/ccompat/v7
```

This deliberately keeps `provider=confluent`, so Kantrip treats the Apicurio
endpoint as a Confluent-compatible Schema Registry. The smoke check exercises
the Confluent console adapters, kcat, and Kaskade with Confluent framing. The
topic is deleted because `--keep-topic` is not present.

Test Apicurio's native Core Registry API v3 integration with:

```bash
uv run --locked python -m sandbox my-apicurio-native-topic \
  --profile sandbox-apicurio-native \
  --registry-provider apicurio \
  --registry-url http://localhost:8082/apis/registry/v3
```

This pings the native `/search/artifacts` API and verifies that Kaskade receives
the Apicurio provider and URL. It skips the six Confluent registry console
probes, which cannot read native Apicurio framing, while retaining the generic
Kafka and kcat metadata checks. This command validates the native integration
configuration; use the native Apicurio workflow below to decode records produced
with Apicurio's default `contentId` framing. Only Kaskade 5+ supports this native
profile.

All commands animate running steps in colored terminals and use stable status
labels in plain output. Unless `--keep-topic` is supplied, each command deletes
its smoke topic even when a later check fails.

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
  --registry-url http://localhost:8081

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

### Native Apicurio adapter workflow

Create a separate profile for the native Core Registry API v3 endpoint:

```bash
uv run kantrip add sandbox-apicurio \
  --bootstrap-servers localhost:9092 \
  --registry-provider apicurio \
  --registry-url http://localhost:8082/apis/registry/v3

uv run kantrip ping sandbox-apicurio
```

After producing records with an Apicurio serializer in a native-format topic:

```bash
uv run kantrip exec sandbox-apicurio -- kaskade consumer \
  --topic your-native-apicurio-topic --earliest -v registry
```

Kaskade 5+ can decode native Apicurio Avro, JSON Schema, and Protobuf records.
The producing serializers must use Apicurio's default `contentId` framing.
Confluent console clients and kcat do not accept this profile; configure a
second profile with provider `confluent` and the `/apis/ccompat/v7` endpoint for
those clients.

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
