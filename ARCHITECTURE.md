# MVP 1 Architecture

This document defines Kantrip's architecture after MVP 1 is complete. It
describes the product boundary, security-relevant data flow, component
responsibilities, strengths, and deliberate limitations.

Kantrip is not a persistent process manager, a global context selector, or a
replacement for Kafka clients. Its responsibility ends at storing the profile,
resolving its secrets, generating the correct temporary configuration, and
supervising the active execution.

## Product boundary

Kantrip is a local connection-profile broker for Kafka command-line tools and
compatible applications. A user explicitly chooses a profile for every command
or subshell. Kantrip then:

1. Loads and validates the profile.
2. Resolves referenced secrets from an approved operating-system store.
3. Builds one provider-neutral, in-memory connection model.
4. Renders only the product-native properties required by the selected client.
5. Starts and supervises that client inside a bounded session.
6. Removes session-owned connection material when execution ends.

Kantrip does not implement the Kafka protocol, serialize records, manage
topics, choose schemas, install clients, or keep an ambient active profile.
Kafka clients, Schema Registry clients, brokers, registries, and identity
providers retain their own protocol, authorization, and update responsibilities.

## Data flow

```text
profile YAML ───────────────┐
                           ├─> validation ─> secret resolution
OS credential store ───────┘                        │
                                                   v
                                      resolved session model
                                                   │
                         ┌─────────────────────────┼─────────────────────────┐
                         v                         v                         v
                  Java renderer          librdkafka renderer       Registry renderer
                         └─────────────────────────┼─────────────────────────┘
                                                   v
                                  private session files/environment
                                                   │
                                                   v
                                      adapter ─> selected client
                                                   │
                                      Kafka / Registry / IdP
```

Diagnostics and imports use the same validation, normalization, secret-store,
and rendering boundaries. They do not maintain alternate connection models.

## Profiles and connection material

Profiles use one schema-validated YAML document. Each profile has an immutable
UUID, Kafka connection metadata, and at most one independent Registry
connection. The YAML file contains non-secret values and opaque secret
references; it never contains passwords, bearer tokens, OAuth client secrets,
raw JAAS, or private keys.

The configuration path follows `KANTRIP_CONFIG`, then `XDG_CONFIG_HOME`, then
`~/.config/kantrip/config.yaml`. The file is written atomically with mode
`0600`, and concurrent mutations are serialized with a lock.

Long-lived secrets are stored under immutable profile-ID-based keys in either:

- macOS Keychain.
- A Linux Secret Service-compatible backend.

Null, fail, plaintext, encrypted-file, and unknown keyring backends are
rejected. There is no fallback that weakens storage when an approved backend is
unavailable or locked.

The profile model supports:

- Plaintext connections without authentication for local development.
- TLS server authentication using system trust or a profile CA.
- SASL/PLAIN over TLS.
- SCRAM-SHA-256 and SCRAM-SHA-512 over TLS.
- Mutual TLS with a client certificate and private key.
- OAuth 2.0 client credentials through verified native client mechanisms.
- Independent Confluent-compatible or native Apicurio Registry TLS and
  authentication.

Kafka and Registry credentials remain separate even when a client eventually
requires a combined properties file. Registry authentication is never inherited
implicitly from Kafka authentication.

## Connection-only configuration

Kantrip models secure connections, not arbitrary Kafka application behavior.
Typed profile fields cover endpoint discovery, transport security, server
verification, client identity, credential acquisition, and product-required
authentication routing.

Producer, consumer, topic, group, schema-selection, serializer, retry, cache,
telemetry, and application behavior do not belong in a profile. Importers ignore
those properties and report their names without their values. Unknown
security-like properties, conflicting aliases, and settings that disable
certificate or hostname verification fail closed.

The resolved model is rendered into distinct Java, librdkafka, Confluent Schema
Registry, and native Apicurio configurations. Renderers use documented native
properties such as `sasl.oauthbearer.token.endpoint.url`,
`bearer.auth.issuer.endpoint.url`, and
`apicurio.registry.auth.service.token.endpoint`. Imported files are never
replayed as unvalidated passthrough configuration.

## Profile lifecycle and imports

`add`, `edit`, `secret set`, and `remove` share schema validation, profile
locking, secret staging, atomic YAML replacement, and reconciliation logic.
`edit` can add or update a Registry connection; only an explicit removal action
deletes it. Existing secrets have explicit keep, replace, and remove semantics
and are never displayed or prefilled.

The YAML file and OS credential store cannot participate in one atomic
transaction. Kantrip therefore uses immutable secret references and a
non-secret reconciliation journal:

- The journal records the exact planned old and new references before the first
  keyring write that could create an orphan.
- New secrets are staged before the YAML reference changes.
- A failed YAML update removes staged secrets and preserves the previous
  profile.
- Superseded or removed references are deleted only after the YAML switch.
- Failed cleanup remains in the journal for an idempotent retry by a later
  mutation or `doctor`.

Imports from Java properties, librdkafka properties, Confluent-generated client
properties, and Strimzi KafkaUser TLS or SCRAM Secrets normalize into the same
profile model. Importers use format-specific parsers and a connection-property
allowlist, extract supported secrets before committing the profile, and never
modify the source. They do not inspect private vendor CLI files or perform
Kubernetes discovery.

## Session resolution and rendering

Secret resolution produces one in-memory session object. Renderers derive all
client-specific files from that object; adapters cannot query the secret store
independently. This keeps secret access centralized and makes adapter capability
checks deterministic.

Generated properties, certificate chains, CA bundles, private keys, adapter
configuration, and shims live only in the session directory. The directory is
mode `0700`; files are created exclusively with restrictive permissions.
Secrets never appear in child arguments, logs, diagnostics, snapshots, or
normal output.

Java and librdkafka OAuth use their verified native client-credentials flows so
the client owns token acquisition and refresh. Kantrip stores the client secret
and supplies the documented connection properties; it does not implement a
parallel token-refresh service.

## Client adapters

An adapter recognizes a client by executable basename, verifies its supported
version and profile capabilities, rejects connection overrides, and injects the
resolved connection through that client's documented interface. Unsupported or
ambiguous combinations fail before the child starts.

The adapter set covers:

- Apache Kafka and Confluent Kafka commands using Java properties files.
- Confluent Schema Registry console clients.
- `kcat` and `kafkacat` using a private librdkafka configuration.
- Kaskade using private Kafka and provider-specific Registry files.
- `kcl` using a private TOML profile selected with `KCL_CONFIG_PATH`.
- `kafkactl` using private read-only and writable YAML paths while disabling its
  independent keyring integration.

Official Kafka commands receive their native bootstrap and properties-file
arguments. Confluent schema-aware console clients accept only a
Confluent-compatible Registry. `kcat` receives a Registry URL only when its
selected deserializer requires one. Kaskade is the only MVP adapter that uses a
native Apicurio Registry profile. Every adapter rejects caller-supplied options
that would replace a Kantrip-owned connection value.

Interactive Bash, Zsh, and Fish sessions use temporary adapter shims. The shell
loads its normal startup files and history, after which Kantrip removes
supported-client aliases, functions, and abbreviations and restores the shim
directory at the front of `PATH`. Kantrip installs no persistent aliases and
does not modify the user's Kafka installation.

## Process supervision and cleanup

One-off commands run in a Kantrip-owned POSIX process group. Interactive shells
run in a PTY-owned child session so terminal job control, resizing, and Ctrl-C
behave normally. The supervisor forwards SIGINT, SIGTERM, and SIGHUP, preserves
the child's exit status, escalates after five seconds or a repeated signal, and
restores terminal state in all normal error paths.

The child receives `KANTRIP_PROFILE`, `KANTRIP_SESSION_ID`, and
`KANTRIP_SESSION_DIR`, plus the documented client-specific connection variables.
Kantrip constructs this environment for the child only and clears inherited
Kafka and Registry values that could conflict with the selected profile. The
caller's parent environment is never modified.

Session directories live below a validated user-owned runtime root. Each
session has a non-secret marker and a held `fcntl.flock`; the lock, not the PID
alone, establishes liveness. Startup cleanup and `kantrip cleanup` remove only
validated, unlocked direct children that are at least five minutes old. The
automatic scan is limited to 256 entries. Cleanup rejects symlinks, unsafe
permissions, wrong owners, malformed markers, and paths outside the root.

Nested sessions are rejected. Processes that deliberately daemonize, create a
new session, or otherwise escape the Kantrip-owned process boundary are
unsupported. Kantrip provides no persistent background-session API.

## Diagnostics and output

`kantrip doctor` performs local validation of profiles, permissions, credential
backend availability, missing references, journaled orphan references,
certificates, session locks, stale artifacts, and installed client capabilities.
It does not print resolved values or generated file contents.

`kantrip ping` resolves the secure profile and uses the same librdkafka and
Registry connection construction as a normal session. It performs bounded
broker metadata and provider-specific Registry requests: `/subjects` for a
Confluent-compatible Registry and `/search/artifacts` for native Apicurio. It
then distinguishes configuration, DNS, transport, TLS, authentication, and
authorization outcomes without claiming more than the underlying operation
proves.

Output and diagnostics stay on separate streams. Redaction occurs before
presentation and does not depend on Rich. `NO_COLOR`, `TERM=dumb`, non-TTY
output, and either supported position of `--no-color` produce stable unstyled
output.

## Architectural strengths

- Explicit profile selection prevents ambient context drift and makes the
  connection boundary visible in every invocation.
- Long-lived secrets remain outside YAML and are resolved only for the selected
  child.
- One typed model and product-native renderers avoid unsafe configuration
  passthrough and dialect mixing.
- Fail-closed adapter capability checks prevent unsupported authentication from
  degrading silently.
- Immutable references plus reconciliation preserve a usable old or new profile
  across partial keyring failures.
- Process groups, PTY ownership, liveness locks, and stale-session cleanup give
  secret-bearing temporary artifacts a defined lifecycle.
- Reusing native Kafka and Registry clients avoids a second protocol stack and
  delegates OAuth refresh to code designed for that client.

## Architectural limitations and tradeoffs

- Kantrip is a credential delivery boundary, not a sandbox. The selected child,
  its descendants, and shell startup code can read material intentionally made
  available to them.
- Same-user compromise, a malicious client executable, a compromised keyring,
  kernel compromise, or administrator access defeats the local controls.
- Secret material exists temporarily in process memory and, for clients that
  require files, in a private runtime directory. Deletion is not forensic
  erasure.
- YAML and keyring updates are recoverable but not atomic. If both the journal
  and its referenced state are lost, generic keyring APIs cannot discover every
  orphan.
- Compatibility depends on external client versions and their documented
  configuration interfaces. Kantrip intentionally rejects combinations that
  have not been integration-tested.
- `ping` proves only the operations it performs. Successful authentication does
  not imply authorization for topics, groups, schemas, or administrative APIs.
- Profiles contain connection configuration only. Application tuning and
  topic-dependent schema behavior must remain with the calling application.
- The MVP supports Linux and macOS only. Windows, Amazon MSK IAM, the Strimzi
  OAuth module, HTTP proxy profiles, automatic Kubernetes discovery, and
  managed background sessions are outside the architecture.
