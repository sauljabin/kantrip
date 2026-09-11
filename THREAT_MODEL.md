# Threat Model

Kantrip protects plaintext Kafka profile metadata and temporary client sessions
on a developer workstation. It reduces profile confusion and accidental local
exposure; it does not defend against a compromised user account, kernel,
terminal, child process, Kafka installation, or administrator.

## Protected assets

- The association between profile names and Kafka brokers.
- Generated client properties, executable shims, and session metadata.
- Command arguments, shell history, diagnostics, and terminal transcripts.

## Trust boundaries

- The configuration file and selected profile are untrusted input until schema
  validation succeeds.
- A child explicitly selected by the user is trusted to read its injected
  environment and generated files.
- Kafka brokers and installed command-line tools remain separate systems with
  their own trust and update models.

## Primary threats

- Profile values reaching logs, tracebacks, errors, or terminal output without
  redaction.
- Malicious profile names, generated shims, executable resolution, or command
  interpolation changing the selected process.
- User startup aliases, functions, abbreviations, or `PATH` changes bypassing a
  profile adapter inside an interactive session.
- Connection arguments overriding the profile selected by the user.
- Temporary generated files remaining after a normal session.

## Implemented controls

- Validate every loaded or updated profile against the bundled schema.
- Atomically write configuration with mode `0600` and generated session files
  with restrictive permissions.
- Use random session identifiers and Python-owned temporary directories.
- Execute argument arrays directly without shell interpolation.
- Reject nested sessions and adapter connection overrides.
- Load supported shell configuration, then remove supported-client shadows and
  restore session-owned shims in the child shell.
- Redact sensitive-looking fields before normal presentation.

## Accepted limitations

- A user-selected child and its descendants can read the environment and files
  Kantrip gives them.
- Detached descendants can outlive normal temporary-directory cleanup; detached
  and background execution is unsupported.
- Unlinking files does not guarantee forensic erasure on SSD, copy-on-write,
  journaled, or snapshotting filesystems.
- A compromised user account, runtime, dependency, broker, or client executable
  can bypass Kantrip's local controls.

Future threat-model extensions are tracked with their features in `MVP.md`.
Report suspected failures of implemented controls according to `SECURITY.md`.
