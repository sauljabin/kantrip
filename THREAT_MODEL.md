# Threat Model

What Kantrip protects, what it trusts, and what it can't defend against. It
builds on the design in [Architecture](ARCHITECTURE.md) and covers what exists
today: the profile store, the credential lifecycle, running authenticated Kafka
and Registry clients, and local diagnostics. A profile that passes schema
validation isn't automatically safe end to end: whether a client can use a
given Registry or OAuth setup safely depends on the per-client checks listed in
[Compatibility](COMPATIBILITY.md).

Kantrip makes it harder to leak a secret by accident, to run a command against
the wrong profile, to override a profile's connection unsafely, or to leave
secret-bearing files behind. It doesn't make an untrusted command safe, and it
doesn't defend against a compromised operating system or user account.

## Contents

- [Security objectives](#security-objectives)
- [Protected assets](#protected-assets)
- [Actors and trust assumptions](#actors-and-trust-assumptions)
- [Trust boundaries and entry points](#trust-boundaries-and-entry-points)
  - [Profile storage boundary](#profile-storage-boundary)
  - [Imported document boundary](#imported-document-boundary)
  - [Credential vault boundary](#credential-vault-boundary)
  - [Execution boundary](#execution-boundary)
  - [Network boundary](#network-boundary)
  - [Sandbox laboratory boundary](#sandbox-laboratory-boundary)
  - [Recovery boundary](#recovery-boundary)
- [Threats and controls](#threats-and-controls)
  - [Profile tampering and connection confusion](#profile-tampering-and-connection-confusion)
  - [Schema migration tampering and partial upgrades](#schema-migration-tampering-and-partial-upgrades)
  - [Secret disclosure at rest](#secret-disclosure-at-rest)
  - [Secret disclosure during execution](#secret-disclosure-during-execution)
  - [Adapter bypass and command injection](#adapter-bypass-and-command-injection)
  - [Network interception and endpoint substitution](#network-interception-and-endpoint-substitution)
  - [Abandoned runtime artifacts and process escape](#abandoned-runtime-artifacts-and-process-escape)
  - [Diagnostic and output leakage](#diagnostic-and-output-leakage)
  - [Availability and resource exhaustion](#availability-and-resource-exhaustion)
- [Security strengths](#security-strengths)
- [Residual weaknesses](#residual-weaknesses)
- [Out of scope](#out-of-scope)

## Security objectives

Kantrip tries to keep these true:

- Every Kafka connection is associated with an explicitly selected profile.
- Long-lived secrets are absent from the profile database, argv, logs,
  tracebacks, diagnostics, snapshots, and normal terminal output.
- Only the selected child receives the minimum connection material required by
  its verified adapter.
- Kafka and Registry identities remain independent.
- Session-owned files have a bounded lifecycle and recover safely after a crash.
- Database upgrades apply only known immutable migrations and cannot leave a
  silently partial schema.
- Profile mutations leave either the previous usable profile or the new usable
  profile, with incomplete cleanup recorded for retry.

They cover how credentials are handled and which connection is used. They say
nothing about how the client or the remote service behaves, what it's
authorized to do, or what it does with your data.

## Protected assets

- Kafka PLAIN and SCRAM passwords and OAuth client secrets.
- Registry Basic passwords, fixed bearer tokens, and OAuth client secrets.
- Kafka and Registry mTLS private keys and private-key passwords.
- OAuth access tokens obtained by `ping` or by the client.
- Profile-to-broker, Registry, certificate, and secret-reference associations.
- Migration history, checksums, internal sequence, and private pre-migration
  backups.
- Generated Java, librdkafka, HTTP Registry, and INI client files.
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

- Stored profile documents, certificate files, paths, profile names, labels,
  and URLs.
- Environment variables inherited from the caller.
- Client arguments that may override a profile connection.
- Executable lookup and shell aliases, functions, abbreviations, and `PATH`
  changes.
- Session directories discovered after a previous abnormal termination.
- Kafka brokers until configured server TLS verification succeeds; plaintext
  Kafka and HTTP Registry connections do not verify server identity.

Authorization happens on the server, outside Kantrip. A valid identity can
still be denied by broker ACLs, Registry permissions, or identity-provider
policy.

## Trust boundaries and entry points

### Profile storage boundary

Each JSON profile document crosses from the private SQLite database through
schema validation and typed parsing. Secret references become usable only after
the configured credential backend is approved and each exact reference
resolves. Kafka and Registry secret references must belong to that profile and
the expected fully qualified field. Passwords, fixed tokens, OAuth client
secrets, mTLS private keys, and optional key passwords never enter the profile
document; public client certificate chains do and are checked against the
resolved key before use.

### Imported document boundary

`add --from-strimzi` reads a Kubernetes Secret that the user exported, from a
regular file or stdin. Kantrip treats it as untrusted input: at most 1 MiB of
UTF-8, exactly one document, no YAML anchors or aliases, no duplicate keys, and
bounded nesting. It accepts only the `v1` Secret shape the Strimzi user
operator generates for one SCRAM-SHA-512 or TLS `KafkaUser`, reads the
password or the certificate and key, and rejects unknown keys. Parser errors
name keys and rules, never values or source lines. The imported values go
through the same validation, vault staging, and journal as values typed at a
prompt; the document itself is neither stored nor modified.

The Secret's `ca.crt` is the clients CA that signed the user certificate.
Kantrip ignores it rather than trusting it for the listener, which a
compromised or misconfigured clients CA could otherwise impersonate. The
exported file is itself a plaintext credential that Kantrip doesn't manage;
piping `kubectl` output to `--from-strimzi -` avoids writing it to disk.

`add --from-properties` reads Java or librdkafka client properties under the
same 1 MiB, UTF-8, regular-file-or-stdin rules. It rejects duplicate keys, more
than 4096 properties, malformed escapes, and files whose dialect it can't
decide, instead of guessing which value a client would use. `sasl.jaas.config`
goes through a JAAS tokenizer, not pattern matching, so a crafted quote or
comment can't make Kantrip read a different credential than the client would.
The mapping is closed: only the Kafka connection keys it names are read, and
unknown keys in the `sasl.`, `ssl.`, `security.`, and `https.` namespaces fail
rather than being dropped, because each could change what the connection
trusts. It never imports a weaker connection than the file describes:
`SASL_PLAINTEXT`, disabled certificate or hostname verification, unsecured
JWTs, custom login or callback classes, and HTTP token endpoints all fail.
Application keys are ignored and reported by name only, and only names made of
letters, digits, dots, hyphens, and underscores are printed.

PEM paths in the file are read once, through the same bounded regular-file
readers as the CA and key options, and copied into the profile or the vault;
Kantrip keeps no path, so later changes to those files don't reach a profile.
Relative paths resolve against the file's directory; stdin has no directory,
so its paths must be absolute. After importing secrets from a file, from either
option, `add` notes on stderr that the file still holds them. Kantrip never
deletes or changes it, since other clients may still read it.

### Credential vault boundary

On macOS, credentials cross into the dedicated Kantrip keychain through
`/usr/bin/security`, with values on stdin and never in argv. On Linux, they
cross the D-Bus session bus, in a DH-encrypted Secret Service session, into the
dedicated `kantrip` collection of GNOME Keyring or KDE Wallet. On both, the
operating system collects the vault password and Kantrip never handles it, and
the vault doesn't unlock with the login session. When it locks again is up to
the platform; see [Credential vault](ARCHITECTURE.md#credential-vault).

### Execution boundary

Secrets leave Kantrip's control when it builds the child's environment and
writes the private generated files. From then on, the child can read, copy,
print, send, or keep them.

### Network boundary

Kafka and Registry endpoints are external. Current Kafka TLS server identity
depends on certificate/hostname verification with default trust or the selected
CA. Current HTTP Registry connections provide no transport confidentiality or
server identity. Remote services evaluate application authorization.

### Sandbox laboratory boundary

The contributor sandbox is disposable infrastructure, not a production
security boundary. It binds host endpoints to loopback and uses one persistent
Strimzi Kafka cluster with authorization enabled, but it does not claim pod
network isolation. `sandbox-admin` is the only broker superuser and is used only
by the in-cluster provisioning Job over TLS/SCRAM-SHA-512. Runtime-generated
credentials remain below private ignored state and mounted Secrets. Its layout
is in [Verification](ARCHITECTURE.md#verification).

`ANONYMOUS` is never a superuser and is limited to the `kantrip-smoke-` topic and
group prefix. SCRAM-SHA-256 provisioning reads passwords from mounted files,
writes a mode-restricted temporary config, passes its path rather than the
password to Kafka tooling, and deletes it on exit. A privileged cluster
administrator, compromised node, or process inside that short-lived container
can still read the mounted or temporary value.

The E2E runner never creates or removes the caller's sandbox; CI removes only
the sandbox it created, after capturing sanitized diagnostics. CI vault jobs use
weak vaults on purpose: macOS gets a random password passed on the command line
(masked in logs), and Ubuntu an empty-password keyring that GNOME Keyring stores
in plain text. That's acceptable only because those throwaway runner vaults
hold nothing but synthetic sandbox credentials.

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

Threats include credentials committed to profile documents, a fallback to a
store that unlocks with the session or has no password, world-readable files,
orphaned values after failed updates, and unintended copies of credential
input.

Controls:

- Store long-lived secrets only in the dedicated Kantrip vault of each
  platform. Never fall back to the macOS login keychain or the default Secret
  Service collection, which unlock with the session.
- Accept only GNOME Keyring and KDE Wallet as Linux providers, and fail with
  guidance when no Secret Service runs rather than degrading silently.
- Refuse an empty vault password at creation and report one in `doctor`, since
  it lets anyone at the account unlock the vault without a prompt; on GNOME it
  also stores the keyring file in plain text.
- Warn when a Linux vault opens without asking for its password, which reveals
  GNOME's "Automatically unlock this keyring whenever I'm logged in" or an empty
  password, and warn in `doctor` when the vault is the desktop's default
  collection. Restore KDE's `default` alias after creating or opening the
  vault, because KDE's first use would otherwise make the vault the default.
- Find the Linux vault only by its exact identity (the GNOME object path and
  label, or the KDE alias and wallet file), refuse to guess between same-label
  collections, and reject empty secrets, which KDE returns for items of a
  replaced wallet.
- Create macOS vault items through `/usr/bin/security`, whose stable partition
  keeps Python upgrades from prompting, and keep labels, accounts, and
  attributes free of secrets: they name only the field and the profile and
  credential UUIDs. KDE stores Linux labels and attributes in plain JSON.
- Store only opaque immutable references in profile documents. Each reference
  includes independent profile and credential UUIDs so replacement never
  overwrites the value used by the current profile.
- Stage new references before switching the profile transaction and reconcile
  exact superseded references afterward.
- Write cleanup intent to a transactional non-secret database journal before a
  cross-store mutation can create an orphan.
- Classify a failed commit acknowledgement by reopening the database under the
  same maintenance lock and matching exact profile-generation and journal
  evidence. Unknown outcomes retain evidence and prohibit automatic retry.
- Validate the complete journal against live references before deletion, but
  process only cleanup records owned by the current successful mutation; older
  debt remains explicit for repair.

Residual risk: Kantrip does not enumerate the vault's entries. If the
reconciliation journal is destroyed, Kantrip cannot prove that no orphaned
credential remains. A compromised or unlocked native store also exposes all
secrets accessible to the user. While the macOS vault is unlocked, any process
of the same user can read its items through `/usr/bin/security` without a
prompt; Kantrip runs as Python, not as a signed binary, so it claims no
per-application isolation inside the vault. Any process of the same user can
delete the vault file, even while it is locked; reads then fail with recovery
guidance, and a replaced vault reads as missing secrets. A user can lengthen or
remove the vault's lock timeout in Keychain Access; `doctor` reports a vault
that never locks.

On Linux the vault is readable by any same-user process for as long as it is
unlocked, which is the whole login session by default: GNOME Keyring does not
lock it on idle, screen lock, or suspend, and a secret was read at the lock
screen during testing. Any same-user process can also delete the locked
collection without a password, lock or unlock it through the API, and move the
desktop's `default` alias without a prompt; doctor reports a moved default but
cannot prevent it. A user who ticks GNOME's automatic unlock makes every lock
cosmetic; Kantrip detects it only the next time it unlocks the vault, never by
probing. Password windows do not name the requesting application on either
provider, so a same-user process can ask for the vault password in a window
that looks like Kantrip's.

### Secret disclosure during execution

Threats include credentials in argv, inherited parent state, logs, tracebacks,
terminal output, generated files, or environment variables visible to unrelated
processes.

Controls:

- Resolve one coherent profile generation under the mutation lock and retain
  the resulting credentials only in memory and private session files.
- Pass them only through a private generated file or a child-only environment
  variable documented by the selected client.
- Execute children with argument arrays and never place secrets in arguments.
- Do not mutate the caller's environment.
- Create session roots and directories with mode `0700` and files through
  exclusive, restrictive creation.
- Write only what a one-off supported client reads: the configuration its
  adapter passes and the key and CA files that configuration names. Java
  clients carry client keys inline, so they get no key file, and an encrypted
  key is decrypted to disk only for kaf or kcl. Custom commands and interactive
  shells still get every generated configuration, because they rely on the
  documented file variables, but a decrypted key only when a kaf or kcl
  configuration that names it is written.
- Redact before presentation, independently of output styling.
- Hold in-memory secrets in an opaque `Secret` type whose `repr()` is masked
  and whose text conversion and pickling fail, so tracebacks, debuggers,
  dataclass representations, and test failure messages cannot print them.
  This does not protect process memory or a deliberate `reveal()` call.
- Keep Registry credentials out of Kafka configuration unless a verified client
  requires one combined file.

Kantrip removes reserved Kafka/Registry namespaces, sandbox credential variables,
and JVM option injection before launch, then restores owned values after
supported shell startup. The selected child, its descendants, shell startup files,
debuggers running as the same user, and sufficiently privileged processes can
read the supplied material. Kantrip deliberately trusts this boundary and is
not a sandbox.

### Adapter bypass and command injection

Threats include shell interpolation, executable-name tricks, user-provided
connection flags, startup aliases, functions, abbreviations, and `PATH` changes
that bypass the selected profile.

Controls:

- Match supported clients by validated executable basename, following a symlink
  under another name to the first target with a supported basename, so a linked
  name gets the same adapter instead of bypassing it. Use argument arrays rather
  than constructed shell commands.
- Reject bootstrap, config-path, TLS, authentication, Registry, and other
  connection overrides covered by each adapter contract.
- Version-gate Java custom PEM trust and client identities, and reject unsupported
  adapter/mechanism combinations before launch.
- Load normal interactive-shell startup files, then remove supported-client
  shadows and restore private shims at the front of `PATH`.
- Reject nested Kantrip sessions.

Residual risk: a malicious executable already present at the resolved path or
malicious startup code can exfiltrate connection material. Shim restoration
prevents accidental bypass; it cannot make hostile user-controlled shell code
safe.

### Network interception and endpoint substitution

Threats include plaintext credentials, malicious endpoints, hostname mismatch,
and replaced CA material.

Controls:

- Require verified TLS for every authenticated Kafka profile in the schema;
  there is no localhost exception.
- Keep Kafka certificate and hostname verification enabled.
- Copy validated public CA material into the profile instead of depending on an
  externally mutable CA path during later sessions.
- Reject non-HTTP(S), credential-bearing URL, and unverified authenticated
  Registry configurations. Typed HTTPS trust and credentials use only reviewed
  provider/client mappings.
- Probe registries only through the fixed, non-mutating read query for their
  provider: Confluent-compatible `/subjects?limit=1` or native Apicurio v3
  `/search/versions?limit=1`. Authenticated probes repeat the same URL without
  credentials and accept only an explicit 401/403 or the expected mTLS
  client-certificate rejection; unrelated transport failures prove nothing.
- Keep Kaskade's native Apicurio Registry and token endpoint in separate HTTP/TLS
  contexts. Both use the official shared CA bundle, but Registry mTLS identity
  never enters the IdP client.
- Scope Confluent Python token trust to the Kaskade child. Its private
  `SSL_CERT_FILE` combines platform default roots with the profile IdP CA and
  overrides inherited SSL environment only in that process.

Residual risk: plaintext Kafka and HTTP Registry connections provide neither
transport confidentiality nor server authentication. A trusted CA can still
validate a malicious endpoint. Remote authorization and child handling of data
remain outside Kantrip's control. The corrected native Apicurio mapping is
available in Kaskade 5.0.1; Kantrip rejects older or development builds when a
native Apicurio OAuth scope needs that mapping. Shared CA contracts broaden
trust to both destinations. `SSL_CERT_FILE` is process-wide, not
hostname-specific, so other environment-aware HTTP clients inside the same
Kaskade process also receive that bundle.

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
  limit automatic cleanup and doctor scans to 256 entries, report truncation
  through read-only `doctor`, and use `doctor --repair` for complete removal.

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
- Bound and sanitize connection/Registry request failures, and keep quiet ping
  silent for handled outcomes.
- Treat Kafka ping as evidence of an addressable configured/learned broker
  reaching `UP` after its configured TLS/SASL exchange. Plaintext and
  server-only TLS do not claim a client identity; no result claims authorization.
- Treat Registry ping as evidence of its exact provider resource request; roles
  can reject it independently of transport reachability.

Residual risk: upstream clients control their own stdout and stderr. A trusted
child can print credentials or sensitive Kafka records, and Kantrip cannot
reliably redact arbitrary child output without corrupting it.

### Availability and resource exhaustion

Threats include locked credential stores, hanging clients, unavailable remote
services, malformed certificate files, large private keys, many stale session
directories, and a migration blocked by another writer or insufficient disk.
A locked macOS vault read without a terminal would wait on a password window,
and an unanswered Linux password window blocks its caller forever and stays on
screen.

Controls:

- Bound network diagnostics, startup scans, shutdown grace periods, and parser
  inputs.
- Bound PEM input sizes. Realistic backend-size integration evidence remains
  a first-release verification requirement.
- Fail before launch when required secrets, clients, or mappings are missing.
- Unlock a locked macOS vault only on the controlling terminal, with three
  attempts and a clean Ctrl-C cancellation; without a terminal, fail at once
  with guidance. A refused unlock is not asked again in the same process.
- Show a Linux password window only with a terminal to explain it, wait for it
  at most 60 seconds, and dismiss it on timeout or Ctrl-C. Without a terminal,
  dismiss the never-shown prompt and fail at once, unless the vault opens
  without a window. Report a window the desktop could not show (no display, a
  locked screen) as such.
- Scan only direct children of the validated runtime root automatically.
- Bound maintenance lock acquisition and preserve every uniquely named private
  pre-migration backup.

Residual risk: Kantrip does not provide high availability. A locked vault with
no desktop session to unlock it, an unavailable external service, an exhausted
filesystem, or a hostile same-user process can prevent operation.

## Security strengths

- Naming the profile on every command makes it much harder to run something
  against the wrong environment by accident.
- Long-lived secrets are kept apart from the profile metadata.
- Profiles hold typed fields and each client gets only allowlisted settings,
  which limits configuration injection and keeps topic-specific settings from
  leaking between applications.
- When Kantrip can't confirm a client supports a setting safely, it refuses to
  run, choosing confidentiality and integrity over compatibility.
- Kantrip uses the real Kafka clients rather than its own protocol stack. Shared
  SASL/TLS renderers and one descriptor per client keep each client's handling
  in one place.
- Updates that span the database and the vault can be recovered, and crashed
  sessions are cleaned up using their locks. Local credential wrappers often
  skip both.
- The migration history is ordered and checksummed and fails closed, so schema
  changes are explicit and independent of product releases.
- Kafka and Registry security are separate, so credentials aren't reused and one
  identity isn't silently inherited by the other.

## Residual weaknesses

Each threat above lists its own residual risk. A few don't belong to a single
threat:

- A macOS vault that times out between Kantrip's state check and an item read
  opens a macOS password window; the interval is milliseconds.
- Security support is only as complete as the tested client/version matrix. A
  client upgrade can require a new mapping before Kantrip can safely launch it.
- Profile metadata such as broker names and Registry URLs remains in the local
  SQLite database. Kantrip treats it as sensitive-looking operational data,
  not as a secret-store asset.

## Out of scope

Features Kantrip doesn't have are listed in
[Architecture](ARCHITECTURE.md#out-of-scope). Beyond those, this model doesn't
cover:

- Executing user-provided migration code.
- Compromise of the user account, kernel, administrator, credential-store
  implementation, Python runtime, dependency, or selected client executable.
- Sandboxing, malware prevention, record-level confidentiality, broker ACL
  administration, Registry authorization policy, and identity-provider policy.
- Amazon MSK IAM authentication.
- Guarantees of physical or forensic erasure.

If you think one of these controls fails, report it as described in
[SECURITY.md](SECURITY.md).
