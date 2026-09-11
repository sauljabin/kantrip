# MVP Roadmap

Kantrip's current baseline is the plaintext profile workflow documented in
`README.md` and `USAGE.md`. Items in this file are planned and are not supported
until they move into the implementation, schema, tests, and current-feature
documentation together.

## 1. Session hardening

- Supervise a child process group and forward termination signals explicitly.
- Validate session-directory ownership, permissions, markers, and path
  boundaries before use or cleanup.
- Remove verified stale session directories left by abnormal termination.
- Define and enforce behavior for background or detached descendants.

## 2. Credentials and authenticated Kafka

- Store long-lived secrets in macOS Keychain or a Linux Secret Service provider;
  do not add plaintext or locally encrypted secret-file fallbacks.
- Extend the profile schema and generated client files for TLS, SASL/PLAIN,
  SCRAM-SHA-256, SCRAM-SHA-512, and mutual TLS.
- Materialize certificates and private keys only in private session directories
  and keep secrets out of command arguments and diagnostics.

## 3. Connectivity and registry integration

- Extend `kantrip ping` to cover authenticated profiles as those transports are
  implemented.
- Extend the current plain Confluent Schema Registry console-client integration
  to authenticated connections and Apicurio endpoints.
- Add OAuth support, including the generic client-credentials flow and Strimzi
  callback integration.

## 4. Profile workflow

- Add explicit persistent profile selection without modifying a parent shell.
- Add migration tooling when a released schema needs a breaking change.
- Extend `kantrip doctor` with credential-provider, certificate-expiry, and
  stale-session diagnostics as those capabilities are implemented.

## MVP non-goals

- AWS MSK IAM authentication.
- Windows support.
- Docker-based distribution or a hosted Kantrip service.

## Alpha configuration cleanup

Early example configurations included a top-level `version: 1` field and schema
sections for capabilities that were not implemented. The current alpha schema
rejects `version`, `defaults`, and unsupported Kafka security fields. Remove
those fields and recreate affected Kafka connections as plaintext profiles with
`kantrip add` when convenient. Plain, unauthenticated `schemaRegistry` sections
now remain valid.
