# MVP 1 Architecture

This document describes Kantrip's architecture after MVP 1: its boundaries,
security-relevant data flow, major components, strengths, and limitations.

Kantrip is not a persistent process manager, a global context selector, or a
replacement for Kafka clients. Its responsibility ends at storing profiles,
resolving secrets, generating temporary client configuration, and supervising
the active execution.

## Product boundary

For each command or subshell, the user selects a profile. Kantrip then:

1. Validates the profile and requested client.
2. Resolves referenced secrets from an approved operating-system store.
3. Builds a provider-neutral connection model.
4. Renders the native connection properties required by the client.
5. Supervises the client in a bounded session.
6. Removes session-owned connection material.

Kantrip does not implement Kafka or Registry protocols, install clients, manage
Kafka resources, select schemas, or keep an ambient active profile. Those
responsibilities remain with the selected client and remote services.

## Data flow

![Kantrip connection data flow](images/data-flow.svg)

Profiles and imports converge on the same validated connection model. Each
renderer translates that model into one client's native configuration;
diagnostics reuse the same resolution path.

## Profiles and connection material

Profiles are schema-validated YAML documents with an immutable UUID, Kafka
connection metadata, and at most one independent Registry connection. YAML
contains non-secret values and opaque secret references, never credentials,
bearer tokens, raw JAAS, or private keys.

Configuration follows `KANTRIP_CONFIG`, `XDG_CONFIG_HOME`, then
`~/.config/kantrip/config.yaml`. Mutations are locked and replace the mode
`0600` file atomically.

Long-lived secrets use immutable profile-ID-based keys in macOS Keychain or a
Linux Secret Service-compatible backend. Kantrip rejects unavailable,
plaintext, encrypted-file, null, and unknown backends instead of weakening
storage.

The connection model supports plaintext development profiles, TLS with system
or profile trust, SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, mutual TLS, and
OAuth 2.0 client credentials. A Registry may independently use TLS and
authentication through Confluent-compatible or native Apicurio properties.
Kafka and Registry credentials are never inherited across those boundaries.

## Connection-only configuration

Profiles describe connections: endpoints, transport security, server
verification, client identity, credential acquisition, and authentication
routing. They exclude producer, consumer, topic, group, serializer, retry,
cache, telemetry, schema-selection, and other application behavior.

Importers allowlist connection properties and report ignored property names
without their values. Unknown security-like settings, conflicting aliases, and
options that disable certificate or hostname verification fail closed.

Renderers produce native Java, librdkafka, Confluent Schema Registry, and
Apicurio configurations. They use documented properties such as
`sasl.oauthbearer.token.endpoint.url`, `bearer.auth.issuer.endpoint.url`, and
`apicurio.registry.auth.service.token.endpoint`; imported files are never
replayed as unvalidated configuration.

## Profile lifecycle and imports

Profile mutations share validation, locking, secret staging, atomic YAML
replacement, and reconciliation. `edit` may add or update a Registry; only an
explicit removal deletes it. Existing secrets have explicit keep, replace, and
remove semantics and are never displayed or prefilled.

YAML and the credential store cannot form one atomic transaction. Kantrip uses
immutable secret references and a non-secret reconciliation journal to make
partial failures recoverable:

- Record planned reference changes before a keyring write can create an orphan.
- Stage new secrets before switching the YAML reference.
- On a failed YAML update, remove staged secrets and retain the old profile.
- Delete superseded secrets only after the YAML switch.
- Retain failed cleanup work for idempotent retry by a mutation or `doctor`.

Java, librdkafka, Confluent-generated, and Strimzi KafkaUser TLS or SCRAM files
import into the same profile model. Importers use format-specific parsers,
extract supported secrets before commit, and never modify the source. Private
vendor CLI files and Kubernetes discovery are outside this boundary.

## Session resolution and rendering

Secret resolution creates one in-memory session model. Renderers derive all
client files from it; adapters cannot query the credential store independently.

Generated properties, certificates, private keys, adapter files, and shims live
only in the private session directory. Secrets do not appear in child
arguments, diagnostics, snapshots, or normal output.

Java and librdkafka OAuth use their verified native client-credentials flows.
Kantrip supplies native connection properties but does not implement token
refresh.

## Client adapters

An adapter recognizes the executable, checks its version and required profile
capabilities, rejects connection overrides, and injects native configuration.
Unsupported or ambiguous combinations fail before execution.

MVP 1 adapters cover Apache and Confluent Kafka commands, Confluent Schema
Registry consoles, `kcat`/`kafkacat`, Kaskade, `kcl`, and `kafkactl`. The exact
version and feature matrix lives in [COMPATIBILITY.md](COMPATIBILITY.md).

Interactive Bash, Zsh, and Fish sessions use temporary shims. The shell keeps
its startup files and history, while Kantrip neutralizes aliases, functions,
and abbreviations that would bypass an adapter. No persistent shell or Kafka
installation changes are made.

## Process supervision and cleanup

### Execution sequence

![Kantrip exec sequence](images/exec-sequence.svg)

The profile store and credential store are read before session files exist.
Only the selected client receives the rendered connection material, and the
runtime is removed after supervision ends.

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

The child receives only Kantrip session metadata and adapter-specific
connection variables. Conflicting inherited Kafka and Registry values are
removed from the child environment; the caller's environment is unchanged.

### Runtime identity and liveness

Kantrip uses `$XDG_RUNTIME_DIR/kantrip/sessions` when the base directory is
absolute, real, user-owned, and mode `0700`; otherwise it uses
`<system-temp>/kantrip-<uid>/sessions`. An existing unsafe managed root blocks
execution.

Managed directories use mode `0700`. Each direct child is named
`session-<32-lowercase-hex-id>` and contains owner-only connection material,
`session.lock`, and `session.json`. The marker contains only the session ID,
owner UID, supervisor PID, creation time, and lifecycle state.

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

The automatic janitor scans at most 256 direct entries before `exec`.
`kantrip cleanup` performs a complete scan, while `--dry-run` neither creates
nor deletes runtime state. Cleanup returns nonzero for unsafe roots, invalid
entries, incomplete scans, or deletion failures; active and recent sessions are
not errors.

Deletion is descriptor-relative. Before removing a direct child, Kantrip
validates its name, owner, modes, marker, lock, age, contents, and inode; it
rejects symlinks and path replacement. The same lock prevents concurrent
cleaners from deleting an active session.

`doctor` reuses the scanner without mutation. It treats active sessions as
valid, recent or stale entries as warnings, and unsafe state as errors. Runtime
paths appear only with `--verbose`.

Nested sessions and processes that escape through `setsid` or daemonization are
unsupported. Kantrip exposes no persistent background-session API.

## Diagnostics and output

`doctor` validates profiles, permissions, credential-store availability,
secret references, reconciliation state, certificates, runtime sessions, and
installed client capabilities without resolving values for display.

`ping` reuses normal connection construction for bounded Kafka metadata and
provider-specific Registry requests. Its result is limited to the operation it
performed; it does not imply broader topic, group, schema, or administrative
authorization.

Kantrip writes results to stdout and diagnostics to stderr. Sensitive values
are classified and redacted before presentation, and color never carries
meaning.

## Architectural strengths

- Explicit profile selection prevents ambient context drift.
- Long-lived secrets remain outside YAML and are resolved only for the selected
  execution.
- One typed model and native renderers prevent dialect mixing and arbitrary
  configuration passthrough.
- Capability checks fail before unsupported authentication can degrade.
- Immutable references and reconciliation preserve a usable profile across
  partial credential-store failures.
- Process boundaries, liveness locks, and recovery give temporary secrets a
  bounded lifecycle.
- Native clients retain protocol handling and OAuth refresh.

## Architectural limitations and tradeoffs

- Kantrip is a credential-delivery boundary, not a sandbox. The selected client,
  descendants, and shell startup code can read material supplied to them.
- Same-user compromise, malicious executables, compromised credential stores,
  kernel compromise, or administrator access defeat local controls.
- Secrets temporarily exist in memory and sometimes private files; deletion is
  not forensic erasure.
- YAML and credential-store updates are recoverable, not atomic. Loss of both
  journal and referenced state can leave undiscoverable orphans.
- Compatibility depends on external client interfaces and tested versions.
- Profiles intentionally exclude application behavior and topic-dependent
  schema configuration.
- Linux and macOS are supported. Windows, Amazon MSK IAM, the Strimzi OAuth
  module, HTTP proxy profiles, Kubernetes discovery, and managed background
  sessions are outside MVP 1.
