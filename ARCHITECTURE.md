# Architecture

This document describes the implemented architecture. Shared credential and
renderer foundations do not imply authenticated CLI execution. Remaining
implementation decisions and first-release gates belong to [MVP.md](MVP.md).
The diagrams illustrate component boundaries; the capability limits in this
text and [Compatibility](COMPATIBILITY.md) govern current behavior.

Kantrip is not a persistent process manager, a global context selector, or a
replacement for Kafka clients. Its responsibility ends at storing profiles,
resolving secrets, generating temporary client configuration, and supervising
the active execution.

## Product boundary

For each command or subshell, the user selects a profile. Kantrip then:

1. Validates the profile and rejects currently unsupported authentication.
2. Builds the plaintext or verified-TLS connection model.
3. Renders the native connection properties required by the client.
4. Supervises the client in a bounded session.
5. Removes session-owned connection material.

The shared secret resolver exists for authenticated profile validation and
rendering, but the execution path does not enable those profiles yet.

Kantrip prepares the connection for one execution; it does not perform the
Kafka or Registry operation itself. It does not install or replace clients,
manage topics, groups, or schemas, or keep a globally active profile. The
selected client performs the requested operation using the connection material
Kantrip supplies for that session.

## Command surface

The CLI follows one resource-oriented lifecycle. `add` creates a profile,
`edit` changes it, `remove` deletes it, `list` selects profiles, and `describe`
inspects one safely. `doctor`, `ping`, `exec`, and `current` retain their
diagnostic, connectivity, execution, and session roles.

The CLI currently accepts explicit non-secret profile fields; it has no file
input or prompted credential editor. Kantrip has no separate `import`, `secret`,
`export`, `clone`, or `configure` command family. Human, JSON, and YAML
inspection are observations rather than round-trip profile documents; they omit
secret values and internal credential references. The exact planned CLI
contract remains centralized in [MVP.md](MVP.md).

## Data flow

![Kantrip connection data flow](images/data-flow.svg)

Explicit profile fields enter a validated connection model. Each renderer
translates that model into native client configuration. External input sources
and unified authenticated diagnostics are not implemented yet.

## Profiles and connection material

Profiles are schema-validated JSON documents stored one per row in a private
SQLite database. Each has an immutable UUID, Kafka connection metadata, and at
most one independent Registry connection. Documents contain non-secret values
and opaque secret references, never credentials, bearer tokens, raw JAAS, or
private keys.

Database lookup follows `KANTRIP_DATABASE`, `XDG_DATA_HOME`, then
`~/.local/share/kantrip/profiles.db`. Its directory is user-owned mode `0700`;
the database and SQLite sidecars are mode `0600`. Symlinks, unsafe ownership or
permissions, corrupt schemas, and unsupported internal versions fail closed.
WAL permits readers while writes are serialized with bounded
`BEGIN IMMEDIATE` transactions. The profile document schema is independent of
the internal database schema version.

Long-lived secrets use immutable
`profile/<profile-uuid>/<credential-uuid>/<field>` keys in macOS Keychain or a
Linux Secret Service-compatible backend. The credential UUID changes on every
replacement, so staging never overwrites the value referenced by the usable
profile. Kantrip rejects unavailable, plaintext, encrypted-file, null, and
unknown backends instead of weakening storage.

The implemented connection model supports plaintext transport,
server-authenticated TLS, SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and mutual
TLS. Authentication always requires verified TLS. Passwords, private keys, and
optional private-key passwords resolve from exact profile-owned references;
public client certificate chains remain in the profile. Java and librdkafka
render independently from the same resolved model, including internally escaped
JAAS for Java password mechanisms. OAuth 2.0 client credentials remain planned.
A Registry remains an independent connection; Kafka and Registry credentials
are never inherited across those boundaries.

Authenticated execution is a separate capability boundary. Until adapter and
authenticated-ping integration lands, `exec` and `ping` reject these otherwise
valid authenticated profiles before creating a session or client.

## Schema evolution and maintenance

![Kantrip database migration flow](images/database-migration.svg)

Kantrip owns a linear migration history inside the profile database. The engine
executes immutable `SqlMigration` commands registered explicitly in one
`MigrationChain`; it does not discover modules or files dynamically. Each
command has one positive integer `sequence`, which is both its identity and
order, plus an immutable name and SQL payload. Its checksum covers all three.
The history records when it ran and which Kantrip version applied it, but
product SemVer does not identify or order migrations. A release may contain
zero, one, or several migration commands.

The initial SQLite profile store is migration sequence `1`; sequence `2` adds
the credential reconciliation journal. New databases apply the complete bundled
chain. Databases from published releases apply each pending migration in
ascending order before a profile-dependent command continues. An unreleased
database shape receives no compatibility path: every non-empty database without
migration history fails closed. There is no YAML migration path and no
user-facing migration command or script.

`schema_migrations` is authoritative and `PRAGMA user_version` mirrors its
highest applied sequence. Missing or duplicate sequences, an altered checksum,
an unknown applied migration, a newer sequence, or disagreement with the pragma
fails closed. Applied migrations are never edited or renumbered; corrections
roll forward through a new sequence.

Pending work acquires the cross-process maintenance lock and creates one private
consistent backup before applying the pending chain. Its name is
`profiles.db.pre-migration-<UTC timestamp>-<unique id>`, so a later migration
never overwrites an earlier recovery point. Schema changes run inside a bounded
`BEGIN IMMEDIATE` transaction. Kantrip updates the history and pragma only with
the schema change, validates the result before commit, retains every backup, and
never downgrades a database.

A normal `doctor` run opens storage read-only and reports pending work without
creating files. `doctor --repair` is the single explicit operational workflow:
under one lock it migrates, validates, reconciles exact credential-journal
records, removes validated stale sessions, and diagnoses the resulting state.
It never guesses how to repair corrupt, ambiguous, active, recent, or externally
owned objects.

## Connection-only configuration

Profiles describe connections: endpoints, transport security, server
verification, client identity, credential acquisition, and authentication
routing. They exclude producer, consumer, topic, group, serializer, retry,
cache, telemetry, schema-selection, and other application behavior.

Every stored Registry connection names its provider explicitly. The CLI may
select Confluent as a convenience default, but it persists that choice rather
than relying on schema normalization or read-time inference.

Profiles reject arbitrary property maps. Current renderers produce Java and
librdkafka connection configuration and provider-specific HTTP Registry URL
settings. Import normalization, Registry security, and OAuth rendering remain
in the roadmap.

## Profile lifecycle and input sources

Profile mutations share validation, cross-store locking, secret staging,
transactional database updates, and reconciliation. `edit` may add or update a
Registry; only an explicit removal deletes it. Internal authentication mutation
helpers preserve omitted secrets and replace immutable references. The CLI does
not yet expose those helpers or an interactive secret editor.

The reusable cross-store transaction engine lives in
`kantrip/credential_mutations.py`. Authentication-specific profile fields and
commands build on this engine; they do not implement their own keyring or
journal sequence.

SQLite and the credential store cannot participate in one atomic transaction.
Kantrip therefore stores non-secret exact-delete records in
`credential_reconciliation` and uses immutable secret references to make
partial failures recoverable. The journal never stores a value and never asks a
backend to enumerate credentials:

- Commit cleanup intent before a credential-store write can create an orphan.
- Stage new secrets under new references.
- Atomically switch the profile document and advance its journal record in one
  SQLite transaction.
- On a failed database update, remove staged secrets and retain the old profile.
- Delete superseded secrets only after the profile switch, retaining failed
  cleanup work for idempotent retry by a mutation or `doctor --repair`; a normal
  `doctor` run only reports the pending record.

Each profile row has a stable UUID, a unique name, and a monotonically
increasing revision. The revision is a generation and concurrency token, not
retained history. The internal mutation API accepts an expected revision and
rejects a concurrent change instead of overwriting it. Prompt collection and
external file normalization have not been wired into the CLI; their transaction
boundaries and format contracts are specified in the roadmap.

## Session resolution and rendering

The shared resolver can build an authenticated in-memory model. The current
session path rejects authenticated profiles, then renders its supported model
without independent keyring queries from adapters.

Generated properties, custom CA bundles, adapter files, and shims live only in
the private session directory. A selected Kafka CA bundle is validated and copied into the profile as public material, then
materialized privately for Java and librdkafka clients. Secrets do not appear
in child arguments, diagnostics, snapshots, or normal output.

Custom CA profiles use native PEM properties in librdkafka. Java adapters first
verify Apache Kafka 2.7+ or Confluent Platform 6.1+, the releases that introduced
PEM trust-store support. Older or unidentifiable Java clients fail before the
Kafka operation instead of attempting an incompatible or weaker configuration.

OAuth acquisition and refresh are not implemented in the current profile or
session path. Their native-client integration is scoped in the roadmap.

## Client adapters

An adapter recognizes the executable, rejects connection overrides, and injects
native configuration. Java custom-CA execution also checks the installed client
version. Authentication is currently blocked before adapter execution.

Current adapters cover Apache and Confluent Kafka commands, Confluent Schema
Registry consoles, `kcat`/`kafkacat`, and Kaskade. The exact version and feature
matrix lives in [COMPATIBILITY.md](COMPATIBILITY.md).

Interactive Bash, Zsh, and Fish sessions use temporary shims. The shell keeps
its startup files and history, while Kantrip neutralizes aliases, functions,
and abbreviations that would bypass an adapter. No persistent shell or Kafka
installation changes are made.

## Process supervision and cleanup

### Execution sequence

![Kantrip exec sequence](images/exec-sequence.svg)

The profile is validated before session files exist. Only the selected child
receives rendered connection material, and the runtime is removed after
supervision ends. Authenticated credential resolution is currently blocked in
this path.

### Session lifecycle

![Kantrip supervised session lifecycle](images/session-lifecycle.svg)

Before starting a child, `exec` validates the request, runs the bounded janitor,
creates and locks a private runtime session, renders configuration, and prepares
the adapter. The marker changes from `preparing` to `running` only after
preparation succeeds. Every exit path closes the lock and removes the owned
session when possible.

### Process and terminal boundary

Direct commands run in a new POSIX session and process group, allowing Kantrip
to signal descendants that remain within the managed boundary. Interactive
Bash, Zsh, and Fish shells run in a PTY-owned child session; Kantrip bridges I/O
and window-size changes while the PTY preserves job control.

The supervisor forwards SIGINT, SIGTERM, and SIGHUP. The first signal starts a
five-second grace period; another signal or expiration sends SIGKILL. Normal
exit codes are preserved and signal `N` maps to `128 + N`.

When control returns to Kantrip, it restores signal handlers, terminal
attributes, and foreground ownership. A supervisor SIGKILL, host failure, or
power loss can prevent restoration and leave the runtime session for recovery.

The child inherits the parent environment with Kantrip's documented
Kafka/config/session variables overwritten and both providers' Registry
URL/config variables cleared before the selected pair is set. Other exported
values, including sandbox credential variables, are currently retained. The
caller environment is unchanged. Shell startup restores shims/PATH, not all
connection variables; comprehensive precedence is pending in the roadmap.

### Runtime identity and liveness

Kantrip uses `$XDG_RUNTIME_DIR/kantrip/sessions` when the base directory is
absolute, real, user-owned, and mode `0700`; otherwise it uses
`<system-temp>/kantrip-<uid>/sessions`. An existing unsafe managed root blocks
execution.

Managed directories use mode `0700`. Each direct child is named
`session-<32-lowercase-hex-id>` and contains owner-only connection material,
`session.lock`, and `session.json`. The marker contains only the session ID,
owner UID, supervisor PID, creation time, and lifecycle state. It does not yet
record profile ID or revision. Generated files are snapshots, but profile-scoped
session attribution cannot yet be established from these markers.

Kantrip holds an exclusive `fcntl.flock` for the session lifetime. The kernel
lock, not the recorded PID, establishes liveness. A crash releases the lock even
when it leaves the directory behind.

### Recovery

![Kantrip session cleanup decision](images/session-cleanup-decision.svg)

Runtime entries are classified as follows:

| State | Meaning | Cleanup action |
| --- | --- | --- |
| `active` | Valid and locked | Keep |
| `recent` | Valid, unlocked, younger than five minutes | Keep |
| `stale` | Valid, unlocked, at least five minutes old | Eligible |
| `removed` | Stale and deleted | None |
| `invalid` | Failed structural or security validation | Report, never delete |
| `failed` | Safe candidate could not be deleted | Report |

The lock is authoritative; age only protects a newly unlocked or partially
initialized session from immediate deletion. Invalid entries remain untouched.

The automatic janitor scans at most 256 direct entries before `exec`. A normal
`doctor` run previews a bounded classification and reports truncation;
`doctor --repair` removes every valid stale session it can safely process.
Repair returns nonzero for unsafe roots, invalid entries, incomplete scans,
deletion failures, or any other remaining error. Active and recent sessions are
not errors.

Deletion is descriptor-relative. Before removing a direct child, Kantrip
validates its name, owner, modes, marker, lock, age, contents, and inode; it
rejects symlinks and path replacement. The same lock prevents concurrent
cleaners from deleting an active session.

The doctor scanner treats active sessions as valid, recent or stale entries as
warnings, and unsafe state as errors. Runtime paths appear only with
`--verbose`.

Nested sessions and processes that escape through `setsid` or daemonization are
unsupported. Kantrip exposes no persistent background-session API.

## Diagnostics and output

`doctor` currently checks global migration/profile state, permissions,
credential-backend availability, reconciliation counts, aggregate runtime
sessions, and installed command presence. It does not yet inspect each stored
credential's availability or certificate expiry, or attribute sessions to a
profile revision. Its default mode is read-only; `--repair` explicitly enables
only the deterministic maintenance sequence described above. Scoped diagnostics
and detailed session observations remain in the roadmap.

`ping` reuses normal connection construction for bounded Kafka metadata and
provider-specific Registry requests. Its result is limited to the operation it
performed; it can depend on resource permissions and does not imply broader
topic, group, schema, or administrative authorization. It does not yet implement
a connection/authentication-only probe.

Kantrip writes results to stdout and diagnostics to stderr. Sensitive values
are classified and redacted before presentation, and color never carries
meaning.

## Architectural strengths

- Explicit profile selection prevents ambient context drift.
- Long-lived secrets remain outside the profile database and are resolved only
  for the selected execution.
- One typed model and native renderers prevent dialect mixing and arbitrary
  configuration passthrough.
- Capability checks fail before unsupported authentication can degrade.
- Immutable references and reconciliation preserve a usable profile across
  partial credential-store failures.
- Ordered migrations, immutable checksums, backups, and fail-closed history
  validation make local schema evolution auditable and recoverable.
- Process boundaries, liveness locks, and recovery give temporary secrets a
  bounded lifecycle.
- Native clients retain protocol handling; Kantrip does not implement Kafka
  wire protocols.

## Architectural limitations and tradeoffs

- Kantrip is a credential-delivery boundary, not a sandbox. The selected client,
  descendants, and shell startup code can read material supplied to them.
- Same-user compromise, malicious executables, compromised credential stores,
  kernel compromise, or administrator access defeat local controls.
- Secrets temporarily exist in memory and sometimes private files; deletion is
  not forensic erasure.
- SQLite and credential-store updates are recoverable, not atomic. Loss of both
  journal and referenced state can leave undiscoverable orphans.
- Schema migrations are forward-only. An older Kantrip binary cannot open a
  database containing migrations it does not recognize.
- Compatibility depends on external client interfaces and tested versions.
- Profiles intentionally exclude application behavior and topic-dependent
  schema configuration.
- Linux and macOS are supported. Windows, Amazon MSK IAM, the Strimzi OAuth
  module, HTTP proxy profiles, Kubernetes discovery, and managed background
  sessions are outside the supported product boundary.
