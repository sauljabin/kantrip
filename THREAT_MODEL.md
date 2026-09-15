# MVP 1 Threat Model

This document models Kantrip after MVP 1 is complete. It covers local profile
storage, secret resolution, configuration rendering, supervised execution,
imports, diagnostics, and direct connectivity checks on a developer workstation.

Kantrip reduces accidental disclosure, profile confusion, unsafe connection
overrides, and abandoned secret-bearing artifacts. It does not make an
untrusted command safe and does not defend against a compromised operating
system or user account.

## Security objectives

Kantrip aims to preserve these properties:

- Every Kafka connection is associated with an explicitly selected profile.
- Long-lived secrets are absent from the profile database, argv, logs,
  tracebacks, diagnostics, snapshots, and normal terminal output.
- Only the selected child receives the minimum connection material required by
  its verified adapter.
- Imported configuration cannot bypass the typed connection model or weaken TLS
  verification silently.
- Kafka and Registry identities remain independent.
- Session-owned files have a bounded lifecycle and recover safely after a crash.
- Database upgrades apply only known immutable migrations and cannot leave a
  silently partial schema.
- Profile mutations leave either the previous usable profile or the new usable
  profile, with incomplete cleanup recorded for retry.

These objectives protect credential handling and connection selection. They do
not guarantee the behavior, integrity, authorization, or data handling of the
external client or remote service.

## Protected assets

- Kafka PLAIN and SCRAM passwords.
- OAuth client secrets and fixed Confluent-compatible Registry bearer tokens.
- OAuth access tokens acquired and refreshed inside selected client processes.
- mTLS private keys and private-key passwords.
- Registry basic-auth credentials.
- Profile-to-broker, Registry, certificate, and secret-reference associations.
- Migration history, checksums, internal sequence, and private pre-migration
  backups.
- Generated Java, librdkafka, Registry, TOML, YAML, and INI client files.
- Session environment variables, runtime paths, locks, markers, and shims.
- The integrity of the executable and adapter selected for a profile.
- Command arguments, shell history, diagnostics, errors, and terminal
  transcripts that could accidentally expose connection material.

CA certificates and public certificate chains are not confidential, but their
integrity matters because replacing them can redirect trust.

## Actors and trust assumptions

Kantrip trusts:

- The operating-system kernel, filesystem permission model, and approved native
  credential store.
- The logged-in user who selects the profile and command.
- The selected child executable and its descendants with all connection
  material intentionally provided to them.
- User shell startup files executed inside an interactive session.
- Installed Python dependencies and supported Kafka or Registry client
  libraries.

Kantrip treats as untrusted until validated:

- Stored profile documents, imported files, stdin, paths, profile names, labels,
  and URLs.
- Environment variables inherited from the caller.
- Client arguments that may override a profile connection.
- Executable lookup and shell aliases, functions, abbreviations, and `PATH`
  changes.
- Session directories discovered after a previous abnormal termination.
- Kafka brokers, Registry servers, and OAuth identity providers until TLS and
  authentication checks succeed.

Remote authorization policy remains outside Kantrip. A valid identity can still
be denied access by broker ACLs, Registry permissions, or identity-provider
policy.

## Trust boundaries and entry points

### Profile storage boundary

Each JSON profile document crosses from the private SQLite database through
schema validation and typed parsing. Secret references become usable only after
the configured credential backend is approved and each exact reference
resolves.

### Import boundary

Java properties, librdkafka properties, Confluent-generated properties, and
Strimzi KafkaUser Secret documents are attacker-controlled input.
Format-specific parsers normalize only allowlisted connection shapes; source
files never become runtime configuration directly.

### Execution boundary

Kantrip crosses from protected state into a trusted child when it creates the
child-only environment and private generated files. After that handoff, the
child can read, copy, print, transmit, or retain the supplied values.

### Network boundary

Kafka, Registry, and OAuth endpoints are external. Server identity depends on
TLS certificate and hostname verification using the client's default trust
store or the selected profile CA. Application authorization is evaluated by
those remote services.

### Recovery boundary

Database state and runtime directories found during startup or explicit repair
are untrusted local inputs. Migration history must match the bundled immutable
chain before schema changes. Runtime ownership, permissions, shape, marker,
lock state, age, and path containment must all validate before deletion.

## Threats and controls

### Profile tampering and connection confusion

Threats include changing brokers or Registry URLs, swapping secret references,
using a misleading profile name, relying on an ambient selection, or combining
Kafka credentials with the wrong Registry identity.

Controls:

- Validate every load and mutation against the bundled schema.
- Require an explicit profile for every connecting command.
- Do not implement a persistent current-profile selector.
- Give every profile an immutable UUID and key secrets by that identity rather
  than by a renameable display name.
- Require every secret reference to match the owning profile UUID and expected
  credential field.
- Keep the optional Registry connection structurally independent from Kafka.
- Keep the user-owned database directory at mode `0700` and SQLite files at
  mode `0600`; reject symlinks and unsafe metadata.
- Serialize profile writes with bounded `BEGIN IMMEDIATE` transactions and
  validate every document read from the database.
- Redact profile values before presentation.

Residual risk: filesystem permissions provide confidentiality and accidental
integrity protection, not cryptographic authenticity. Malware running as the
same user can alter profile endpoints or references.

### Schema migration tampering and partial upgrades

Threats include changing an applied migration, forging its history, skipping or
reordering sequences, opening a newer database with an older binary, concurrent
upgrades, disk exhaustion, and interruption between schema and metadata writes.

Controls:

- Use one positive integer sequence as each migration's immutable identity and
  order; do not derive it from product SemVer.
- Store the applied sequence, name, checksum, time, and applying Kantrip version
  in the private profile database.
- Require contiguous known history and matching checksums; mirror the highest
  sequence in `PRAGMA user_version` and fail closed on disagreement.
- Reject every non-empty database without migration history. Compatibility
  begins with the first published database state, not unreleased development
  schemas.
- Acquire one cross-process maintenance lock, create a private consistent
  uniquely timestamped pre-migration backup without overwriting earlier
  recovery points, and use a bounded `BEGIN IMMEDIATE` transaction.
- Update schema, history, and `user_version` together, validate before commit,
  and roll back on failure.
- Apply only bundled forward migrations in ascending order. Never downgrade,
  execute a user-provided migration script, or edit a released migration.
- Keep normal `doctor` inspection read-only and require the explicit
  `doctor --repair` mode for a complete maintenance pass.

Residual risk: transactions and backups protect against interruption and known
failure paths, not a logically incorrect migration that passes its validation.
A same-user attacker can alter both the database and local application files;
checksum validation is integrity evidence against accidental drift, not a
cryptographic trust anchor.

### Secret disclosure at rest

Threats include credentials committed to profile documents, insecure keyring
fallback, world-readable files, orphaned values after failed updates, and
unintended copies of imported secrets.

Controls:

- Store long-lived secrets only in macOS Keychain or an approved Linux Secret
  Service-compatible backend.
- Reject null, fail, plaintext, encrypted-file, unavailable, locked, and unknown
  backends rather than degrading silently.
- Store only opaque immutable references in profile documents. Each reference
  includes independent profile and credential UUIDs so replacement never
  overwrites the value used by the current profile.
- Stage new references before switching the profile transaction and reconcile
  exact superseded references afterward.
- Write cleanup intent to a transactional non-secret database journal before a
  cross-store mutation can create an orphan.
- Never modify, delete, or duplicate a user-owned import source.

Residual risk: standard keyring APIs cannot enumerate arbitrary entries. If the
reconciliation journal is destroyed, Kantrip cannot prove that no orphaned
credential remains. A compromised or unlocked native store also exposes all
secrets accessible to the user.

### Secret disclosure during execution

Threats include credentials in argv, inherited parent state, logs, tracebacks,
terminal output, generated files, or environment variables visible to unrelated
processes.

Controls:

- Resolve secrets into one in-memory session model.
- Pass them only through a private generated file or a child-only environment
  variable documented by the selected client.
- Execute children with argument arrays and never place secrets in arguments.
- Do not mutate the caller's environment.
- Create session roots and directories with mode `0700` and files through
  exclusive, restrictive creation.
- Redact before presentation, independently of output styling.
- Keep Registry credentials out of Kafka configuration unless a verified client
  requires one combined file.

Residual risk: the selected child, its descendants, shell startup files,
debuggers running as the same user, and sufficiently privileged processes can
read the supplied material. Kantrip deliberately trusts this boundary and is
not a sandbox.

### Adapter bypass and command injection

Threats include shell interpolation, executable-name tricks, user-provided
connection flags, startup aliases, functions, abbreviations, and `PATH` changes
that bypass the selected profile.

Controls:

- Match supported clients by validated executable basename and use argument
  arrays rather than constructed shell commands.
- Reject bootstrap, config-path, TLS, authentication, Registry, and other
  connection overrides covered by each adapter contract.
- Check the client version and authentication capability before launch.
- Load normal interactive-shell startup files, then remove supported-client
  shadows and restore private shims at the front of `PATH`.
- Reject nested Kantrip sessions.

Residual risk: a malicious executable already present at the resolved path or
malicious startup code can exfiltrate connection material. Shim restoration
prevents accidental bypass; it cannot make hostile user-controlled shell code
safe.

### Unsafe imported configuration

Threats include parser confusion, mixed Java and librdkafka dialects, malicious
escaping, arbitrary JAAS modules, misspelled security properties, TLS-disabling
settings, secret-bearing unknown keys, and topic-dependent behavior entering a
global profile.

Controls:

- Parse Java properties, librdkafka properties, and Kubernetes Secret documents
  with explicit format-aware logic.
- Tokenize supported JAAS syntax instead of extracting secrets with regular
  expressions.
- Normalize only PLAIN, SCRAM, mTLS, and OAuth client-credentials shapes.
- Ignore known non-connection and unknown non-security properties while
  reporting names only.
- Reject unknown security-like keys, mixed or ambiguous dialects, arbitrary
  login modules, unverified callbacks, and disabled certificate or hostname
  validation.
- Extract supported secrets into the credential store before committing the
  profile.
- Reject implicit JKS or PKCS12 conversion and do not execute vendor CLIs with
  secrets in argv.

Residual risk: an allowlist can lag a new client release. The safe failure mode
is loss of compatibility, not silent acceptance; users must wait for a tested
mapping. A user-owned source file can remain as a separate plaintext copy after
import; Kantrip does not own or erase it, so stdin is safer for generated files
that contain credentials.

### Network interception and endpoint substitution

Threats include plaintext credentials, malicious brokers or registries,
hostname mismatch, replaced CA material, and interception of the OAuth token
request.

Controls:

- Require TLS for every credential-bearing Kafka, Registry, or OAuth connection.
  SASL without TLS is allowed only for documented loopback test infrastructure.
- Keep certificate and hostname verification enabled for Kafka, Registry, and
  token endpoints.
- Reject client properties that disable those checks.
- Keep the token endpoint and its CA separate from broker TLS configuration.
- Use only documented Java, librdkafka, Confluent Schema Registry, and native
  Apicurio connection properties.
- Delegate OAuth token acquisition and refresh to verified native clients.

Residual risk: Kantrip cannot protect against a malicious endpoint trusted by
the selected CA, a compromised identity provider, bad remote authorization
policy, or secrets copied by the client after connection.

### Abandoned runtime artifacts and process escape

Threats include normal cleanup being skipped, PID reuse, symlink or traversal
attacks during cleanup, concurrent janitors, descendants surviving the parent,
and terminal corruption after signals.

Controls:

- Supervise one-off commands in a POSIX process group and interactive shells in
  a PTY-owned session.
- Forward SIGINT, SIGTERM, and SIGHUP to the managed boundary and escalate after
  five seconds or a repeated signal.
- Restore terminal attributes and foreground ownership in a `finally` path.
- Hold a `fcntl.flock` for liveness and use the PID only as metadata.
- Validate ownership, permissions, marker, lock, age, direct-child shape, and
  path containment before stale cleanup.
- Reject symlinks and use descriptor-relative or equivalently symlink-safe
  deletion.
- Treat only validated, unlocked sessions older than five minutes as stale,
  limit automatic cleanup to 256 entries, preview the complete state through
  read-only `doctor`, and require `doctor --repair` for complete removal.

Residual risk: a process that deliberately daemonizes or creates a new session
can escape supervision and retain copied material. `SIGKILL`, host crashes, and
power loss can leave private files until the next cleanup pass. Deletion does
not guarantee forensic erasure on SSD, copy-on-write, journaled, or snapshotting
filesystems.

### Diagnostic and output leakage

Threats include exception wrapping that reveals URLs or credentials, Rich
markup bypassing classification, verbose output exposing paths or environment,
and diagnostics overstating successful authorization.

Controls:

- Classify and redact values before formatting or styling.
- Keep command output on stdout and diagnostics on stderr.
- Bound and redact underlying exception messages before diagnostic output.
- Never print resolved child environments or generated file contents.
- Make color optional and semantically irrelevant.
- Map stable librdkafka and HTTP outcomes into configuration, transport, TLS,
  authentication, and authorization stages.
- Treat HTTP 401 as authentication failure and HTTP 403 as authenticated but
  unauthorized only when the provider follows those semantics.
- Describe a ping as proof of its bounded metadata or Registry request, not as a
  general authorization test.

Residual risk: upstream clients control their own stdout and stderr. A trusted
child can print credentials or sensitive Kafka records, and Kantrip cannot
reliably redact arbitrary child output without corrupting it.

### Availability and resource exhaustion

Threats include locked credential stores, hanging clients, unavailable identity
providers, malformed import files, large private keys, many stale session
directories, and a migration blocked by another writer or insufficient disk.

Controls:

- Bound network diagnostics, startup scans, shutdown grace periods, and parser
  inputs.
- Validate realistic credential sizes against supported backends.
- Fail before launch when required secrets, clients, or mappings are missing.
- Scan only direct children of the validated runtime root automatically.
- Bound maintenance lock acquisition and preserve every uniquely named private
  pre-migration backup.

Residual risk: Kantrip does not provide high availability. A locked keyring,
unavailable external service, exhausted filesystem, or hostile same-user process
can prevent operation.

## Security strengths

- Explicit profile selection materially reduces accidental cross-environment
  operations.
- Long-lived secrets are isolated from portable profile metadata.
- Typed normalization and allowlisted rendering reduce configuration injection
  and prevent topic-specific behavior from leaking between applications.
- Fail-closed capability checks favor confidentiality and integrity over broad
  client compatibility.
- Native TLS, SASL, and OAuth implementations avoid a custom Kafka or token
  protocol stack.
- Recoverable cross-store updates and lock-based crash cleanup address failure
  modes commonly omitted from local credential wrappers.
- Ordered migration history and fail-closed checksum validation make schema
  evolution explicit without coupling it to product releases.
- Kafka and Registry security are modeled independently, reducing credential
  reuse and accidental identity inheritance.

## Residual weaknesses

- The user-selected child is inside the trust boundary and receives usable
  credentials. Kantrip cannot constrain what it does with them.
- Shell startup files execute with the child environment before Kantrip can make
  the interactive session useful; a hostile startup file can capture secrets.
- The same local user can inspect process memory, replace executables, modify
  profile metadata, or interfere with runtime files subject to OS controls.
- Temporary private files reduce accidental exposure but do not eliminate
  on-disk secret material or provide forensic deletion.
- Native keyring operations and SQLite updates are not one atomic transaction;
  reconciliation narrows but cannot eliminate every orphan scenario.
- Security support is only as complete as the tested client/version matrix. A
  client upgrade can require a new mapping before Kantrip can safely launch it.
- OAuth client secrets remain long-lived credentials. Native refresh limits
  access-token handling but does not provide automatic secret rotation or
  revocation.
- Successful connectivity does not establish authorization beyond the exact
  probe performed.
- Profile metadata such as broker names and Registry URLs remains in the local
  SQLite database. Kantrip treats it as sensitive-looking operational data,
  not as a secret-store asset.

## Out of scope

- Database downgrade support and executing user-provided migration code.
- Compromise of the user account, kernel, administrator, credential-store
  implementation, Python runtime, dependency, or selected client executable.
- Sandboxing, malware prevention, record-level confidentiality, broker ACL
  administration, Registry authorization policy, and identity-provider policy.
- Detached or managed background sessions and processes that deliberately
  escape the Kantrip-owned POSIX boundary.
- Windows support, Amazon MSK IAM authentication, and the Strimzi OAuth module.
- Refresh-token or fixed-access-token Kafka OAuth profiles.
- HTTP proxy profiles, automatic Kubernetes discovery, automatic certificate
  conversion, and arbitrary secret-provider automation.
- Guarantees of physical or forensic erasure.

Report suspected failures of the stated controls according to `SECURITY.md`.
