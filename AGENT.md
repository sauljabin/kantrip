# Agent Instructions

## Engineering Contract

- Write all source code, identifiers, comments, tests, fixtures, CLI text, and
  documentation in English, regardless of the language used in conversations.
- When a durable convention changes, update this file and every affected guide,
  schema, fixture, example, template, and command. Replace obsolete or duplicate
  guidance.
- Do not add empty modules or speculative adapters. Keep cyclomatic complexity
  at or below 10; repository-wide Ruff `C901` runs in `scripts.analyze`, so use
  focused helpers instead of suppressions.
- Importing `kantrip` must have no filesystem, logging, console, or network side
  effects. Classify and redact values before presentation; keep behavior
  independent from Rich.
- Support Linux and macOS on Python 3.10 through 3.14. Keep paths, permissions,
  signals, terminals, and shell documentation portable.
- Keep unimplemented product work and exact future CLI contracts in `MVP.md`,
  not in current feature docs, schemas, examples, commands, or implementation
  comments.
- Preserve a resource-oriented CLI. Extend the lifecycle, inspection,
  diagnostic, and execution verbs defined in `MVP.md` instead of adding
  one-off command families for input formats or credential operations. Never
  expose secret retrieval or a round-trip profile export. Update current user
  documentation only when the corresponding behavior lands.

## Profiles and Sessions

- Keep Kantrip scoped to profile storage, secret resolution, temporary client
  configuration, and active-execution supervision. Do not turn it into a
  persistent process manager, global context selector, or Kafka client
  replacement.
- Keep the implemented profile schema in `schemas/`, examples in `examples/`, and
  synthetic data with its tests. Update examples, tests, and documentation with
  schema changes. Release-bundled profile schemas are authoritative; profile
  documents omit application versions. The SQLite schema uses a separate
  internal version.
- Treat the environment documented in `USAGE.md` as public API. Add variables
  compatibly; renames or semantic breaks require release and migration guidance.
  Use `KAFKA_*` for application values and reserve `KANTRIP_*` for
  Kantrip-owned profile/session metadata.
- Store profiles in the private SQLite database resolved through
  `KANTRIP_DATABASE`, `XDG_DATA_HOME`, then the user's default data directory.
  Keep its directory mode `0700`, database and sidecar modes `0600`, WAL enabled,
  and writes bounded by `BEGIN IMMEDIATE` transactions. Reject symlinks, unsafe
  ownership or permissions, corrupt contents, and unsupported schema versions.
- Evolve the database through one bundled linear chain in
  `kantrip/migrations.py`. Keep one immutable `SqlMigration` subclass per change
  in the explicit `MigrationChain` registry; do not use filesystem or import
  discovery. Use a positive integer `sequence` as each migration's only identity
  and order; store its immutable name, checksum,
  applied timestamp, and applying Kantrip version in `schema_migrations`. Never
  derive migration identity from product SemVer or profile document fields.
- Treat released migrations as immutable and forward-only. Mirror the highest
  applied sequence in `PRAGMA user_version`, update schema and history in one
  transaction under the maintenance lock, create a uniquely timestamped backup
  before pending work, and fail closed on gaps, unknown entries, checksum
  changes, or version disagreement. Never overwrite an earlier migration backup.
- Do not add compatibility or baseline adoption for database formats that were
  never published in a release. Before the first release, reject every non-empty
  history-less database and all former YAML. The first published database starts
  the supported migration boundary.
- Profile add/edit/remove is validated and transactional; add creates the
  missing database and never overwrites a profile, edit preserves its immutable
  ID and changes only explicit fields, and list returns empty when the database
  is absent. Registry removal requires `--remove-registry`. Profile-dependent
  commands may migrate an existing supported database, but reads never create a
  missing repository. Normal `doctor` inspection never creates or modifies
  filesystem state.
- Access credentials only through the narrow `SecretStore` protocol. Approve
  only macOS Keychain on macOS and Secret Service-compatible keyring backends on
  Linux; reject null, plaintext, encrypted-file, chained, and unknown backends.
  Use service `kantrip` and canonical immutable
  `profile/<profile-uuid>/<credential-uuid>/<field>` keys. Fully qualify fields
  by owner, including `kafka/oauth/client-secret` and
  `registry/oauth/client-secret`.
- Route every secret-bearing profile change through `credential_mutations`.
  Journal each new immutable reference before its credential-store write,
  switch the validated profile with an expected revision, and retire only exact
  superseded references after the database commit. A failed switch leaves the
  old profile usable and the staged reference recoverable by reconciliation.
- Keep exact pending credential deletions in `credential_reconciliation`.
  Validate every record and reference, delete only that exact credential, and
  remove its journal row only after deletion succeeds. Normal doctor reports
  pending work; `doctor --repair` retries it under the maintenance lock.
- Inject the documented environment only into supervised children; never mutate
  the caller's environment or add a separate JSON schema for environment values.
- Reject `kantrip exec` when `KANTRIP_SESSION_ID` identifies an active parent
  session. The schema accepts PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and mTLS only
  over verified TLS. Resolve their exact profile-owned references through
  `SecretStore`, validate mTLS certificate/key correspondence, construct Java
  JAAS internally, and keep Java and librdkafka rendering independent. Until
  authenticated adapters and ping land, reject those profiles before launch.
- Copy user-selected Kafka CA bundles into the profile as validated public PEM
  material. TLS always verifies certificates and hostnames. Materialize a
  custom CA only inside the private session and use canonical Java and
  librdkafka properties; never accept arbitrary client-property passthroughs.
  Version-gate Java custom-CA sessions at Apache Kafka 2.7 or Confluent Platform
  6.1 and fail before the Kafka operation when support cannot be verified.
- Supervise one-off commands in their own POSIX process boundary and interactive
  shells through a PTY. Forward SIGINT, SIGTERM, and SIGHUP, preserve child exit
  status, and escalate after five seconds or a repeated signal.
- Keep session roots and directories private and user-owned. Determine liveness
  with `fcntl.flock`, treat validated unlocked sessions as stale after five
  minutes, and inspect no more than 256 entries automatically before `exec`.
- Cleanup must be descriptor-relative, refuse symlinks and unsafe metadata, and
  never remove active sessions or paths outside the validated runtime root.
- Keep `doctor` read-only unless the user supplies `--repair`. Unified repair is
  non-interactive and idempotent: migrate, validate, reconcile exact journal
  entries, clean validated stale sessions, then diagnose again under one lock.
  Do not add public task-specific migration or cleanup commands or flags.

## Client Adapters and Shells

- Sessions expose private librdkafka properties through `KCAT_CONFIG`; shims
  preserve it and reject `-F`. kcat Avro deserializers receive the plain registry
  URL through `-r`; explicit `-r` is rejected. Never put secrets in arguments.
- Adapters recognize Apache Kafka's `.sh` commands and Confluent's unsuffixed
  equivalents. All receive `--bootstrap-server`; consumers/producers receive
  their config option and admin tools receive `--command-config`, using private
  Java properties.
- Profiles use one optional `registry` object whose stored `provider` is always
  explicit. `kantrip add --registry-url` selects and persists `confluent` when
  `--registry-provider` is omitted. Confluent uses the official
  `schema.registry.url` serializer/deserializer property and native Apicurio
  uses `apicurio.registry.url`. The providers are mutually exclusive and only
  plain `http://` URLs are supported.
- Confluent's Avro, JSON Schema, and Protobuf console producers and consumers
  are unsuffixed. They receive the matching Kafka producer/consumer config file
  and a Confluent-compatible `schema.registry.url`; connection overrides and
  native Apicurio profiles are rejected.
- Kaskade `admin` and `consumer` receive a private INI file through
  `--config-file`; registry deserializers select a second private file with a
  provider-specific `[registry]` section. Kaskade alone supports native
  Apicurio Avro, JSON Schema, and Protobuf decoding. Do not assume a Kaskade
  environment variable until Kaskade implements that contract.
- Bash, Zsh, and Fish sessions preserve startup files and history, neutralize
  adapter shadows, and restore the private temporary shim path. Never install
  persistent aliases.
- Adapters must reject connection arguments that override the selected profile.
- `kantrip ping` uses Confluent Kafka's `AdminClient` to request cluster metadata.
  It checks `/subjects` for Confluent-compatible registries and
  `/search/artifacts` for native Apicurio. All checks use a bounded timeout and
  do not depend on an installed external Kafka CLI.

## Sensitive Values and Output

- Never expose sensitive values in arguments, fixtures, logs, output,
  diagnostics, tracebacks, or snapshots. Examples use conspicuously synthetic
  values and infrastructure.
- Send results to stdout and diagnostics to stderr. Animate progress only on
  colored TTYs; `NO_COLOR`, `TERM=dumb`, `--no-color`, and non-TTY output use
  stable text labels. Accept `--no-color` before or after a subcommand through
  the shared local-option decorator. Styling carries no essential information.
- Syntax-highlight JSON and YAML only on colored TTYs. `--no-color`, `NO_COLOR`,
  `TERM=dumb`, and non-TTY streams must contain no ANSI escapes and remain valid
  structured documents.
- Include a bounded, sanitized underlying cause in normal `ping` failures,
  never a traceback. `ping --quiet` emits nothing and communicates only through
  status `0` or `1`.

## Tests, Scripts, and Sandbox

- Tests and their fixtures live in `tests` and remain offline. Shared workflow
  helpers belong in `scripts/__init__.py`; other script modules are executable
  workflows.
- Keep Kind configuration, Kubernetes manifests, pinned versions, synthetic
  bootstrap data, and lifecycle tooling in `sandbox`. Bind every host endpoint
  to loopback. Offline tests may inspect manifests and import side-effect-free
  sandbox helpers; sandbox code must never import tests.
- Generate sandbox credentials at deployment time. Store exported credentials,
  client properties, and certificate material only below ignored
  `sandbox/.state`, with directory mode `0700` and file mode `0600`. Never print
  credential values from sandbox lifecycle commands.
- `python -m sandbox up` reconciles the Kind laboratory; `status`, `credentials`,
  and `down` inspect or remove it. Keep plaintext, verified TLS, SCRAM-SHA-512,
  mTLS, and OAuth listeners on one Strimzi cluster. SASL/PLAIN is not a sandbox
  scenario; `plaintext` means no authentication and no encryption.
- Kafka uses a disposable persistent volume so data survives broker pod restarts.
  Both Apicurio variants use KafkaSQL with isolated journal and snapshot topics,
  delete cleanup policy, and infinite retention so their data survives Apicurio
  pod restarts. Destroying the Kind cluster intentionally removes sandbox data.
- `python -m scripts.smoke` runs the adapter smoke workflow against the active
  plaintext listener on `localhost:9092`, with locally installed clients and
  optional shells. It is a pre-commit hook, not an offline or packaged E2E test.
- `python -m scripts.verify_shell_contract` tests Bash, Zsh, and Fish through PTYs
  and fake clients. Keep assertions in Python and delete safe-metadata event logs
  with their temporary directory.
- Keep reproducible, high-value exploratory scenarios in `MANUAL_TESTING.md`.
  Every scenario needs explicit setup, actions, and expected results.
  `DEVELOPMENT.md` contains environment and contributor workflows, not manual
  test cases. Manual checks complement rather than replace offline tests and the
  sandbox smoke workflow.

## Verification

Run these checks after code, environment, schema, tooling, or documentation work:

```text
uv run --locked python -m scripts.analyze
uv run --locked python -m scripts.tests
uv build --clear
uv run --locked python -m scripts.verify_release dist
```

Regenerate `images/banner.svg` with `uv run --locked python -m scripts.banner`
when the banner, console theme, or SVG helper changes.

## Releases and Contributions

- Annotated stable and PEP 440 pre-release tags (`vMAJOR.MINOR.PATCH`, plus
  `aN`, `bN`, or `rcN` suffixes) on `main` are the only release version source;
  Hatchling and hatch-vcs derive package metadata from Git. GitHub Releases are
  the canonical changelog, so do not add maintained changelogs or version-bump
  commits.
- Never hard-code the current release version in documentation, templates,
  examples, or commands. Use `kantrip --version`, `MAJOR.MINOR.PATCH`, or Git
  metadata so releases need no follow-up edits.
- Keep database migration sequences independent from releases. A release may
  contain zero, one, or several migrations; record its resolved version only as
  `applied_by` metadata.
- Commits and pull-request titles use Conventional Commits:
  `<type>(<optional scope>): <imperative summary>`. Keep the summary short and do
  not use it as a change list.
- End commit messages and pull-request descriptions with a blank line followed by
  `Assisted-by: <AI model> <version>`, using the actual model and version.
