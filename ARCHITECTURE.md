# Architecture Decisions

This document records behavior implemented by Kantrip today. Planned design is
kept in `MVP.md` until it is implemented.

## Plaintext profiles

Profiles contain non-secret Kafka broker metadata. The bundled schema accepts
only plaintext transport without authentication, plus descriptions, labels, and
Java or librdkafka client properties. Profile updates are schema-validated and
atomically replace the configuration file with mode `0600`.

The configuration path follows `KANTRIP_CONFIG`, then `XDG_CONFIG_HOME`, then
`~/.config/kantrip/config.yaml`. Profile names and paths are treated as untrusted
input, and profile display passes through the redaction layer.

## Kantrip sessions

`kantrip exec` runs one command or interactive Bash, Zsh, or Fish subshell for a
selected profile. Nested sessions are rejected. Each session uses a randomly
named temporary directory containing private generated client configuration and,
for interactive shells, adapter shims. Python's temporary-directory lifecycle
removes those artifacts when the supervised command returns.

The child receives `KANTRIP_PROFILE`, `KANTRIP_SESSION_ID`, and
`KANTRIP_SESSION_DIR`; the caller's parent environment is never modified. The
child also receives the documented plaintext `KAFKA_*` and `KCAT_CONFIG` values.

## Client adapters

Supported adapters inject the selected bootstrap servers and generated client
configuration using each tool's native interface. kcat reads `KCAT_CONFIG`,
official Kafka commands receive connection and properties-file arguments, and
Kaskade `admin` and `consumer` receive a generated INI file. Options that would
override the selected profile are rejected.

Interactive shells load the user's normal startup configuration and history.
Kantrip then removes aliases, functions, and Fish abbreviations that shadow
supported client names, restores the session shim directory at the front of
`PATH`, and refreshes command lookup.

## Diagnostics and output

`kantrip doctor` performs read-only local checks. It validates configuration,
file permissions, profile IDs, runtime support, active-session state, shim-path
precedence, and installed client commands without contacting Kafka.

`kantrip ping` performs an explicit remote connectivity check. It uses the
Confluent Kafka `AdminClient` to request cluster metadata through the profile's
bootstrap servers with a bounded timeout. This exercises broker discovery and
provides a client boundary that can grow with authenticated profiles.

Kantrip executes children directly with argument arrays. Normal command output
and diagnostics remain separate, sensitive-looking values are redacted before
presentation, and styling follows terminal capability, `NO_COLOR`, `TERM=dumb`,
and `--no-color`.
