# Architecture Decisions

This document records Kantrip's security decisions and application design.
Implementation details belong in code and tests.

## Profiles and operating-system vaults

Profiles contain versioned, non-secret Kafka and Schema Registry metadata plus
opaque references to credentials. Long-lived passwords, tokens, private keys,
and other secret material live only in the macOS Keychain or a Linux Secret
Service-compatible vault. Kantrip does not fall back to plaintext or locally
encrypted secret files.

Profile updates are validated and atomic. Profile names and paths are treated as
untrusted input, and profile output never reveals resolved secrets or credential
references.

## Kantrip sessions

`kantrip exec` creates a session for one selected profile and one supervised
child process or interactive subshell. A session owns its private runtime
directory, generated client configuration, injected environment, child process
group, and cleanup lifecycle.

Session directories use random identifiers, restrictive permissions, ownership
checks, and validated paths. The supervising Kantrip process forwards signals,
preserves the child's exit status, and removes generated artifacts after the
child exits. A bounded janitor removes verified stale sessions left by abnormal
termination without deleting active sessions or paths outside Kantrip's runtime
root.

Foreground process supervision is the supported execution model. A child that
detaches itself can outlive session cleanup and is therefore unsupported.
Kantrip rejects nested sessions when the child environment already contains its
active session marker.

## Child environment

Kantrip injects connection settings only into the supervised child process and
its descendants; it never modifies the caller's parent shell. Application-facing
settings use documented `KAFKA_*` and `SCHEMA_REGISTRY_*` variables. Kantrip
reserves `KANTRIP_*` for session metadata.

Applications opt in to the child environment by reading the documented
variables or generated configuration files. Command-line adapters may translate
the same resolved session into native flags or configuration paths, but secrets
are never placed in command arguments.

## Generated configuration

Some clients require properties, certificates, private keys, or executable
shims. Kantrip materializes those artifacts only inside the session directory,
creates secret-bearing files with restrictive permissions from the first write,
and removes them with the session. User-provided source files are never cleanup
targets.

Interactive Bash, Zsh, and Fish sessions load the user's normal startup
configuration before applying session controls. Bash and Zsh use session-owned
startup files; Fish uses an init command after its normal configuration. The
session removes child-shell aliases, functions, and abbreviations that shadow
supported client names, restores its executable-shim directory at the front of
`PATH`, and refreshes command lookup. This prevents startup-time path management
or shell definitions from selecting an unadapted executable. Every supported
shell remains attached to its normal user history location; the temporary
session directory is never durable history storage.

## Secret handling and diagnostics

Resolved secrets remain in memory only as long as necessary to prepare and run
a session. Kantrip classifies and redacts sensitive values before producing
normal output, diagnostics, errors, tracebacks, or machine-readable results.
Normal output and diagnostics remain separate, and neither may expose secrets.

Kantrip executes children directly with argument arrays and does not interpolate
commands through a shell. Unsupported authentication or adapter combinations
fail before child launch instead of silently weakening security.
