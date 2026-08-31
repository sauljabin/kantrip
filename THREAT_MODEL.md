# Threat Model

Kantrip protects local Kafka connection profiles and temporary client sessions
on a developer workstation. It reduces accidental credential exposure; it does
not defend against a fully compromised user account, kernel, terminal, child
process, credential store, or administrator.

## Protected assets

- Kafka, OAuth, Schema Registry, TLS private-key, and MSK-related credentials.
- The association between credentials, brokers, and environment names.
- Generated client configuration, certificates, keys, and session metadata.
- Command arguments, shell history, logs, diagnostics, and terminal transcripts.

## Trust boundaries

- The operating-system credential store holds long-lived local secrets.
- Kantrip resolves secrets and supervises a child explicitly selected by the
  user. That child is trusted to read its injected environment and files.
- Generated session directories are trusted only after owner, mode, marker,
  random identifier, and path-boundary validation.
- Kafka brokers, registries, OAuth providers, callback JARs, and external tools
  remain separate systems with their own trust and update models.

## Primary threats

- Secrets written to profile YAML, command arguments, shell history, logs,
  tracebacks, Rich renderables, errors, or issue reports.
- Production credentials combined accidentally with development brokers.
- Symlink, path traversal, wrong-owner, unsafe-runtime-root, or recursive-cleanup
  errors deleting or exposing user files.
- Temporary files surviving normal exit or an unclean termination indefinitely.
- Malicious profile names, generated shims, executable resolution, or command
  interpolation changing the selected process.
- Unsupported client adapters silently dropping authentication or exposing a
  credential through process arguments.
- A pre-authentication Kafka `ApiVersions` response being mistaken for proof of
  valid SASL credentials.

## Required controls

- Store long-lived secrets only in approved operating-system credential stores.
- Keep literal secret flags out of the CLI and reject classified profile fields.
- Use argument arrays and direct process execution without shell interpolation.
- Create runtime directories as `0700` and secret-bearing files as `0600`
  without following symlinks.
- Validate exact cleanup targets and use random session identifiers independent
  of profile display names.
- Remove normal-session artifacts immediately and use bounded stale-session
  cleanup after crashes.
- Preserve explicit adapter capability failures instead of weakening a security
  requirement for compatibility.
- Redact before rendering and keep Rich tracebacks from displaying locals.

## Accepted limitations

- A user-selected child process and its descendants can read the environment and
  files Kantrip gives them.
- Detached descendants may retain inherited environment values; detached and
  background execution is outside the MVP.
- Unlinking files does not guarantee forensic erasure on SSD, copy-on-write,
  journaled, or snapshotting filesystems.
- A compromised user account, credential store, runtime, or dependency can
  bypass Kantrip's local controls.

Report suspected control failures privately according to `SECURITY.md`.
