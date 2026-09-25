# Architecture

This developer reference records the implemented architecture and its technical
decisions. Planned work is tracked in the
[v0.1](https://github.com/sauljabin/kantrip/milestone/1) and
[v0.2](https://github.com/sauljabin/kantrip/milestone/2) milestones.
The diagrams illustrate component boundaries; the capability limits in this
text and [Compatibility](COMPATIBILITY.md) govern current behavior.

Kantrip is not a persistent process manager, a global context selector, or a
replacement for Kafka clients. Its responsibility ends at storing profiles,
resolving secrets, generating temporary client configuration, and supervising
the active execution.

## Product boundary

For each command or subshell, the user selects a profile. Kantrip then:

1. Loads one UUID/revision generation and resolves both Kafka and Registry
   credentials through one store under the mutation lock.
2. Builds the plaintext or verified-TLS connection model.
3. Renders the native connection properties required by the client.
4. Supervises the client in a bounded session.
5. Removes session-owned connection material.

The immutable resolved snapshot is released from the mutation lock before
rendering or networking. Profile rotation or removal after that point cannot
mix generations or invalidate credentials already held in memory. Adapters and
shims never query SQLite or the credential store again during that session.

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

The CLI accepts explicit profile fields and no-echo credential prompts; `edit`
requires at least one option. It has no file import yet. Kantrip has no separate `import`, `secret`,
`export`, `clone`, or `configure` command family. Human, JSON, and YAML
inspection are observations rather than round-trip profile documents; they omit
secret values and internal credential references. Planned CLI changes are
tracked in milestone issues.

`cli.py` defines commands and presents results. `cli_inputs.py` turns options into
typed inputs: each command receives one frozen options dataclass whose fields
match Click's parameter names, validates conflicting flags, collects no-echo
secrets from the controlling terminal, and builds the Kafka and Registry
authentication inputs passed to the profile lifecycle.

## Design rules

These rules bound every feature and are enforced before an operation starts:

- **Explicit selection.** Every network command names `PROFILE`; there is no
  ambient or globally active profile.
- **No secret values in arguments.** Secrets enter only through no-echo
  prompts or bounded private-key files. No flag accepts a literal password,
  token, client secret, private key, or JAAS value.
- **Verified TLS for authentication.** Authenticated Kafka, Registry, and
  OAuth token endpoints require verified TLS, including on localhost; there is
  no insecure test bypass. Plaintext is allowed only without authentication.
- **Independent trust and credentials.** Kafka, Registry, and token-endpoint
  trust and credentials are configured separately and never inherited from
  one another.
- **Capability checks name the gap.** An unsupported client/mechanism
  combination fails before the operation with a message naming both. Support
  in a library does not imply that a CLI built on it exposes the setting.

## Outside the product scope

Database downgrade; profile history or rollback; secret retrieval or
round-trip export; detached or background sessions; Kubernetes discovery;
arbitrary secret providers; Kafka fixed-token or refresh-token OAuth;
client-side Strimzi OAuth callbacks; Windows; automatic JKS/PKCS12 conversion;
HTTP proxy profiles; adapters without a versioned, tested safe contract; a
hosted service, daemon, programmatic profile API, or interactive TUI. Amazon
MSK IAM is tracked separately as a research item.

## Current capability boundaries

The bundled [profile schema](schemas/profile.schema.json) and
[synthetic document](examples/profile.json) define internal validated storage,
not a file-import interface. Profile documents omit application versions;
SQLite has a separate schema version. Keep these internal capabilities separate
from the executable user contract in `COMPATIBILITY.md`.

| Capability | Internal model / rendering | Executable CLI |
| --- | --- | --- |
| Plaintext and verified Kafka TLS | Validated and rendered for supported clients | `add`, `edit`, sessions, and connection-state ping |
| PLAIN, both SCRAM mechanisms, mTLS | TLS-only schema; exact secret references; mTLS key/certificate validation; Java/librdkafka renderers | Supported by `add`, `edit`, `exec`, and `ping` |
| Kafka OAuth | TLS-only client credentials with independent token trust | Native Java 4.0+ and librdkafka OIDC rendering, sessions, and ping |
| Registry | Independent TLS, Basic, token, mTLS, and OAuth | Provider-aware private client configuration and authenticated probes |
| External profile/file import | Bundled JSON schema validates stored documents only | No JSON/YAML, properties, Strimzi, or JKS/PKCS12 import |

Each stored Registry declares `provider: confluent` with
`schema.registry.url`, or `provider: apicurio` with `apicurio.registry.url`.
The CLI selects and persists Confluent when a Registry URL is supplied without
an explicit provider. Native Apicurio uses `/apis/registry/v3`; its
`/apis/ccompat/v7` API requires a Confluent profile. Registry TLS,
authentication, and token-endpoint trust are independent from Kafka. No arbitrary Java or librdkafka
property maps are accepted; only typed connection fields reach renderers.
Registry property namespaces follow the official
[Confluent](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)
and [Apicurio](https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-configuring-kafka-client-serdes.html)
serializer/deserializer contracts.

## Data flow

![Kantrip connection data flow](images/data-flow.svg)

Explicit profile fields enter a validated connection model. Each renderer
translates that model into native client configuration. External input sources
are not implemented yet.

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
the internal database schema version. Every writable connection verifies WAL
and FULL synchronization; supported macOS builds also verify `fullfsync`.
Database and backup directory entries are synchronized before durable creation
is acknowledged. This durability contract assumes a local filesystem, not a
network or cloud-synchronized database directory.

Migration backups use SQLite's online backup API to create one consistent,
private, immutable database file before migration. They are recovery snapshots,
not live databases: they have no active WAL/sidecar policy of their own, and an
out-of-band restore still requires explicit operator handling. Newly created
parent directories are created one level at a time and each parent entry is
`fsync`ed before success is acknowledged.

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
JAAS for Java password mechanisms and native OAuth client-credentials settings.
A Registry remains an independent connection; Kafka and Registry credentials
are never inherited across those boundaries.

The adapter capability table covers Apache/Confluent Java commands, kcat, and
Kaskade for PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and mTLS. Java PEM profiles
retain their installed-version gate; unsupported combinations fail before the
requested client operation.

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
settings. Registry TLS/authentication and Kafka/Registry OAuth render into
client-specific private configuration. Property-file and Strimzi Secret
import are planned for v0.2.

## Profile lifecycle and input sources

Profile mutations share validation, cross-store locking, secret staging,
transactional database updates, and reconciliation. `edit` may add or update a
Registry; only an explicit removal deletes it. Authentication mutation helpers
preserve omitted secrets and replace immutable references. The CLI exposes them
through typed options; stored fields carry over only while the authentication
type is unchanged, and a Registry provider change keeps authentication only when
the new provider supports it. Secret values are never displayed or prefilled.

The reusable cross-store transaction engine lives in
`kantrip/credential_mutations.py`. Authentication-specific profile fields and
commands build on this engine; they do not implement their own keyring or
journal sequence.

The profile layer is split by responsibility:

| Module | Responsibility | I/O |
| --- | --- | --- |
| `profiles.py` | `add`, `edit`, `remove` orchestration and coherent snapshot resolution; owns lock and transaction sequencing | Through the modules below |
| `profile_storage.py` | Database path, private file and lock checks, connections, migration access, schema validation, loading | SQLite and filesystem |
| `profile_auth.py` | Typed authentication input, Kafka and Registry authentication plans, stored authentication documents | None |
| `profile_documents.py` | New profile documents and requested edits applied to a copy | None |
| `mutation_outcomes.py` | Commit evidence and lost-acknowledgement classification | Read-only SQLite reopen |
| `credential_mutations.py` | Cross-store staging, commit, and exact retirement engine | SQLite and credential store |

Other modules call storage functions through the module (`storage.connect(...)`)
rather than importing them, so fault-injection tests replace a seam in one place.

SQLite and the credential store cannot participate in one atomic transaction.
Kantrip therefore stores non-secret exact-delete records in
`credential_reconciliation` and uses immutable secret references to make
partial failures recoverable. The journal never stores a value and never asks a
backend to enumerate credentials:

- Commit cleanup intent before a credential-store write can create an orphan.
- Stage new secrets under new references.
- Atomically switch the profile document and advance its journal record in one
  SQLite transaction.
- On a failed database update, retain the old profile and durable cleanup intent
  for every possibly written staged secret.
- Delete superseded secrets only after the profile switch, retaining failed
  cleanup work for idempotent retry by a mutation or `doctor --repair`; a normal
  `doctor` run only reports the pending record.

Each mutation tracks one explicit durable state: not committed, committed, or
unknown. If `COMMIT` raises, Kantrip rolls back any still-open transaction and,
while retaining the maintenance lock, opens an independent read-only connection
to compare the exact profile name, UUID, revision, canonical document, and
operation-owned journal records. The same committed state governs later reload,
connection-close, file-hardening, and CLI-output failures. Cleanup after a
successful mutation processes only records created by that operation, while
first validating the complete journal against all live references; older debt
remains for `doctor --repair` and cannot change the new mutation's result.

Mutation exit statuses distinguish definitely uncommitted (`1`), committed with
cleanup or post-commit verification pending (`3`), and indeterminate commit
outcomes (`4`). An exception is not treated as proof of rollback. Database
backups contain references rather than credentials, so independently restoring
SQLite cannot restore retired keyring values and is outside the supported
recovery model.

Each profile row has a stable UUID, a unique name, and a monotonically
increasing revision. The revision is a generation and concurrency token, not
retained history. The internal mutation API accepts an expected revision and
rejects a concurrent change instead of overwriting it. Prompt and file
collection occur before the maintenance lock. External file import is planned
for v0.2 and will feed this same mutation path.

## Session resolution and rendering

The shared resolver builds one authenticated in-memory model from a single
profile generation. The session path renders that model without independent
keyring queries from adapters.

Generated properties, custom CA bundles, adapter files, and shims live only in
the private session directory. A selected Kafka CA bundle is validated and copied into the profile as public material, then
materialized privately for Java and librdkafka clients. Secrets do not appear
in child arguments, diagnostics, snapshots, or normal output.

Custom CA profiles use native PEM properties in librdkafka. Java adapters first
verify Apache Kafka 2.7+ or Confluent Platform 6.1+, the releases that introduced
PEM trust-store support. Older or unidentifiable Java clients fail before the
Kafka operation instead of attempting an incompatible or weaker configuration.

Kafka OAuth acquisition and refresh are delegated to the verified Apache Java
callback or librdkafka OIDC support. Registry OAuth remains native to three
distinct implementations: the Confluent Java client used by all Registry
console wrappers, the Confluent Python `SchemaRegistryClient` used by Kaskade,
and Kaskade's native `ApicurioClient`. Acceptance repeats expiry and revocation
once per implementation and trust path, not once per equivalent shell or data
format. A bounded Registry ping obtains one in-memory client-credentials token
and discards it after the provider probe; it is initial acquisition evidence,
not native refresh evidence.

Kaskade's native Apicurio mapping uses the official shared
`apicurio.registry.tls.certificates` bundle for Registry and token endpoint, but
keeps separate HTTP/TLS contexts so Registry client identity never reaches the
IdP. Native Apicurio OAuth scopes require Kaskade 5.0.1 or newer, the first
stable release that implements the official scope property. Profiles without
scopes retain compatibility with earlier Kaskade releases.
Confluent Java likewise uses one official `ssl.*` trust configuration for both
destinations. Distinct CA profiles are rejected for those shared contracts.

Confluent Python 2.15.1 applies `ssl.ca.location` only to Registry while its
Authlib token client uses HTTPX environment trust. For Kaskade Registry OAuth,
Kantrip supplies `SSL_CERT_FILE` only to that direct child or shim process. The
mode-0600 session bundle contains platform default roots plus the profile IdP CA;
inherited `SSL_CERT_FILE` and `SSL_CERT_DIR` cannot override it. This variable is
process-scoped, not hostname-scoped, so every environment-aware HTTP client in
that Kaskade process sees the same bundle. The parent shell, unrelated clients,
system stores, and later sessions remain unchanged.

## Client adapters

An adapter recognizes the executable, rejects connection overrides, and injects
native configuration. One capability table drives direct commands and shell
shims. Both paths invoke the same argument guard before launching the native
client; shell quoting and process supervision remain separate. Java custom-CA
and mTLS execution also checks the installed client version.

| Client family | Profile-owned native inputs | Runtime inputs retained |
| --- | --- | --- |
| Apache/Confluent Java consoles and admin tools | Bootstrap and config files, alternate connection files, and client-property overrides | Consumer `group.id`; topic, group, ACL, and broker resource operations such as `kafka-configs --add-config` |
| Confluent Registry consoles | Kafka and Registry files/URLs, alternate command/formatter/reader files, and profile-owned format properties | Consumer `group.id` and presentation properties that cannot replace a connection |
| `kcat`/`kafkacat` | `-b`, `-F`, `-r`, and arbitrary `-X` configuration | `-X group.id` and `-X broker.address.family` only |
| Kaskade | Bootstrap/Registry/config-file options and arbitrary `--kafka` properties | `--kafka group.id` and `--kafka broker.address.family` only |

These are native-client grammars, not a generic passthrough policy. A future
client option is admitted only after checking the released tool's semantics,
adding direct and Bash/Zsh/Fish regressions, and showing that it cannot select
another profile connection or disclose private configuration. Do not pre-create
adapters for clients Kantrip does not currently support. A user-selected
arbitrary executable remains trusted with its child environment and is not
confined by these adapter guards.

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

The profile and its credentials are validated before session files exist. Only
the selected child receives rendered connection material, and the runtime is
removed after supervision ends.

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

The child starts from the parent environment after removing `KAFKA_*`,
`SCHEMA_REGISTRY_*`, `APICURIO_*`, `KANTRIP_SANDBOX_*`, and the four JVM option
injection variables. Kantrip then injects only the selected snapshot's public
values and private paths. Bash, Zsh, and Fish repeat this cleanup after user
startup files and restore the owned environment and shims. The caller
environment is unchanged.

### Runtime identity and liveness

Kantrip uses `$XDG_RUNTIME_DIR/kantrip/sessions` when the base directory is
absolute, real, user-owned, and mode `0700`; otherwise it uses
`<system-temp>/kantrip-<uid>/sessions`. An existing unsafe managed root blocks
execution.

Managed directories use mode `0700`. Each direct child is named
`session-<32-lowercase-hex-id>` and contains owner-only connection material,
`session.lock`, and `session.json`. The marker contains the session ID, owner
UID, supervisor PID, creation time, lifecycle state, profile UUID, and captured
profile revision. Generated files remain immutable snapshots for that session.

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

## Sandbox verification topology

The local laboratory uses one Kind cluster with one operator-managed Strimzi
Kafka cluster and one disposable persistent node pool. The cluster exposes
plaintext, verified TLS, SCRAM-SHA-512, mTLS, OAuth, PLAIN over TLS, and
SCRAM-SHA-256 over TLS on loopback ports 9092 through 9098. Registry processes
and provisioning use a separate internal listener on port 9099 with TLS and
SCRAM-SHA-512. `StandardAuthorizer` applies to the whole broker;
`sandbox-admin` is its only superuser.

PLAIN identities come from the private runtime-generated
`kafka-custom-users` Secret mounted through Kafka's file configuration
provider. An idempotent in-cluster Job authenticates to the internal listener
as `sandbox-admin` and provisions both SCRAM-SHA-256 credentials. It writes the
credential update to a mode-restricted temporary properties file, passes only
that path to `kafka-configs.sh --add-config-file`, and removes the file on exit;
the password does not enter the container argument vector. There is no
unauthenticated provisioning listener.

The Strimzi User Operator owns ACLs for its authenticated clients, the OAuth
service account, and the five Registry Kafka identities. PLAIN and OAuth
principals also have unused SCRAM-SHA-512 `KafkaUser` credentials so the
operator can reconcile their real ACL principals; their tested listeners still
use their named mechanisms. The two SCRAM-SHA-256 identities are excluded from
User Operator reconciliation, because the Job owns their 256-only credentials
and the allowed identity's `kantrip-auth-` ACLs. Registry
identities receive only their exact topic and consumer-group permissions.
Allowed client fixtures receive the `kantrip-auth-` prefix, while matching
authenticated no-ACL identities prove that ping does not imply resource
authorization.

The provisioning Job is also the sole owner of the unauthenticated smoke ACL:
`ANONYMOUS` receives only topic and group prefixes `kantrip-smoke-` plus cluster
Describe. It is never a superuser. The User Operator explicitly ignores these
Job-owned principals so periodic reconciliation does not erase their credentials
or rules. The
OAuth service account is limited to `kantrip-oauth-`. This is a loopback-only,
disposable test fixture, not a claim of Kubernetes network isolation or a
production authorization design.

The cluster retains broker data, ACLs, and SCRAM-SHA-256 credentials across
broker pod restarts through its PVC and loses them when the Kind cluster is
destroyed. `sandbox up` rejects the retired `Kafka/auth-kantrip` topology and
requires an explicit `down` followed by `up`; it never silently deletes the old
cluster. Acceptance forces a User Operator reconciliation, restarts the broker
without rerunning provisioning, and performs a second idempotent `sandbox up`.

Infrastructure acceptance is deliberately separate from sandbox lifecycle.
`python -m scripts.tests --suite e2e` validates an already-running laboratory
and never creates or destroys it. The same suite runs locally and in CI against
an installed candidate wheel plus separately installed, pinned released clients.
Setup failures (missing executable, wrong version, workload/certificate/topic
readiness, host endpoint, private state, or native credential store) are distinct
from product assertion failures. A private non-blocking lock serializes mutation
of shared OAuth clients. Long-lived Registry consumers force distinct schema
cache misses before and after token expiry, confirm new IdP issuance, then revoke
the client and require token-acquisition failure without decoding the final
record. TUI clients are observed through parsed terminal state.

## Diagnostics and output

`doctor` checks global or profile-scoped migration/profile state, exact
credential availability, certificate/key validity and expiry, reconciliation,
runtime sessions, and installed commands. `doctor PROFILE --sessions` attributes
validated runtime markers to the immutable profile UUID and captured revision.
Its default mode is read-only; `--repair` explicitly enables only the
deterministic maintenance sequence described above.

Kafka `ping` polls librdkafka's public statistics and error callbacks until a
configured or learned, addressable broker reaches `UP` after the required
TLS/SASL exchange. It does not call resource or cluster-description APIs.
Plaintext proves reachability, server-only TLS proves server identity, SASL
proves its configured exchange, and mTLS proves the configured client exchange;
none proves application authorization. Registry profiles independently support
HTTP or verified HTTPS plus Basic, fixed bearer, mTLS, or OAuth credentials.
Their provider-specific read probe uses Confluent-compatible
`/subjects?limit=1` or native Apicurio v3 `/search/versions?limit=1`, validates
the provider response shape, and accepts an empty collection. Authenticated
profiles repeat that exact URL without credentials to prove the gate rejects
anonymous access. The result proves only that listing/search is permitted, not
access to a particular schema or write authorization.

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
