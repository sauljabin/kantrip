# Kantrip MVP roadmap

This file tracks only work that has not been implemented. Current behavior
belongs in `README.md`, `USAGE.md`, `COMPATIBILITY.md`, `ARCHITECTURE.md`,
`THREAT_MODEL.md`, and the bundled schema.

An item must leave this file when its implementation, tests, schema,
compatibility notes, and user documentation land together. Existing behavior
must not be restated here as future work.

## Remaining MVP outcome

Complete Kantrip's local profile model with:

- OS-backed storage for Kafka and Registry secrets.
- SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, mTLS, and generic OAuth client
  credentials over the implemented verified TLS transport.
- Authenticated Confluent-compatible and native Apicurio Registry connections.
- Profile creation from Java/librdkafka properties, Confluent-generated client
  properties, and Strimzi KafkaUser-generated Kubernetes Secrets.
- New adapters for `kcl` and `kafkactl`.
- Authenticated connectivity diagnostics.

Every command that connects to Kafka continues to require an explicit profile.
Kantrip must not keep an ambient or persistent profile selection.

Kantrip is not a persistent process manager, a global context selector, or a
replacement for Kafka clients. Its responsibility ends at storing the profile,
resolving its secrets, generating the correct temporary configuration, and
supervising the active execution.

## Engineering boundaries

- Extend the existing per-profile document. Do not add speculative application
  `version` or `defaults` fields. The database migration sequence remains
  internal and independent from product versions and profile documents.
- Do not add compatibility paths for database formats that were never released.
  The first published release establishes the supported migration boundary,
  while any one release may contain several ordered migration commands.
- Keep resolved secrets out of the profile database, argv, logs, tracebacks,
  diagnostics, snapshots, and normal output.
- Pass secrets to a selected child only through a private generated file or a
  child-only environment variable supported by that client.
- Define connection configuration narrowly as endpoint discovery, transport
  security, server verification, client identity, credential acquisition, and
  product-required authentication routing. Topic, record, serializer,
  deserializer, schema-selection, retry, cache, telemetry, and application
  behavior are not profile concerns.
- Normalize input connection properties into typed profile fields and render a
  new canonical client configuration. Never replay an input properties
  document or retain unrecognized keys as passthrough configuration.
- Ignore known non-connection and unknown non-security properties during input
  processing and report only their property names in the summary. Reject unknown
  properties whose names suggest credentials or security, conflicting aliases,
  and settings that disable certificate or hostname verification.
- Do not add a plaintext or locally encrypted secret-store fallback.
- Do not download client plugins or modify a user's Kafka installation.
- Fail before launch when a profile/client combination has no verified safe
  mapping.
- Preserve the existing explicit `kantrip exec PROFILE` boundary and reject
  nested sessions.
- Detached and background processes remain unsupported. Do not claim that
  Kantrip can supervise a child that deliberately escapes its POSIX session.

## Command-line contract

The completed MVP keeps one compact command surface. Each verb acts at one
resource level and keeps the same meaning across interactive and scripted use:

- `add` creates one complete profile and fails when the name already exists.
- `edit` changes explicit fields on one existing profile and creates no profile.
- `remove` removes one complete profile, not one component within it.
- `list` finds and summarizes profiles.
- `describe` replaces `show` and presents one profile without exposing secrets.
- `doctor` diagnoses global or profile-scoped local state and owns explicit
  deterministic repair.
- `ping`, `exec`, and `current` retain their focused connectivity, execution,
  and active-session meanings.

Do not add separate `import`, `secret`, `export`, `clone`, or `configure`
commands. External documents are input sources for `add`; secret replacement
is part of `edit`; machine-readable profile inspection is part of `describe`. There is no
command that reveals a stored secret and no round-trip Kantrip profile export.
This keeps creation, mutation, removal, inspection, diagnosis, and execution
easy to distinguish while preserving the fail-closed credential boundary.

### Common naming and option rules

- Use `PROFILE` as the profile-name metavariable on every profile-scoped
  command.
- Keep `-b` for `--bootstrap-servers` and `-d` for `--description` on both
  `add` and `edit`.
- Add `-l` for repeatable `--label KEY=VALUE` on `add`, `edit`, and `list`.
  On `add` and `edit` it assigns a label; on `list` it requires an exact match.
  Multiple list labels are combined with logical AND.
- Add `-o` for `--output human|json|yaml` on `list` and `describe`. `human` is
  the default and is rendered with Rich. Keep `rich` as an implementation
  detail rather than a public output-format name.
- Keep `-q` for `ping --quiet`. Do not abbreviate `--force`, `--repair`,
  security options, credential options, or destructive removal options. Do not
  add `-v` for `--verbose`, because it is easily confused with version output.
- Continue accepting `--no-color` globally and after output-producing commands.
  JSON and YAML are syntax-highlighted only on colored TTYs. `--no-color`,
  `NO_COLOR`, `TERM=dumb`, and non-TTY output remain unstyled and
  machine-readable.
- Use short options only for frequent, unambiguous, non-destructive operations.
  Long security option names are deliberate safety and comprehension aids.

### `add`

```text
Usage: kantrip add [OPTIONS] PROFILE

Create a complete profile. Fail if PROFILE already exists.

General:
  -b, --bootstrap-servers HOST:PORT[,HOST:PORT]
                                  Kafka broker addresses.
  -d, --description TEXT          Profile description.
  -l, --label KEY=VALUE           Add a label; repeatable.

Input source:
      --from-properties FILE|-     Create from Java or librdkafka properties.
      --from-strimzi FILE|-        Create from a Kubernetes Secret generated
                                   for a Strimzi KafkaUser.

Kafka security:
      --transport TYPE             plaintext or tls.
      --auth TYPE                  none, plain, scram-sha-256,
                                   scram-sha-512, mtls, or oauth.
      --username TEXT              Kafka authentication username.
      --ca-file PATH               Configure the Kafka CA from a PEM file.
      --client-certificate-file PATH
                                   Configure a PEM client certificate.
      --client-key-file PATH       Read a PEM client private key.
      --oauth-token-url URL        OAuth token endpoint.
      --oauth-client-id TEXT       OAuth client ID.
      --oauth-scope TEXT           OAuth scope; repeatable.

Registry:
      --registry-provider TYPE     confluent or apicurio.
      --registry-url URL           Registry endpoint.
      --registry-auth TYPE         none, basic, oauth, token, or mtls.
      --registry-username TEXT     Registry authentication username.
      --registry-ca-file PATH      Configure the Registry CA from a PEM file.
```

The two input-source options are mutually exclusive. Manual options may fill
required non-secret data missing from an input source, but conflicting
connection or security values fail instead of silently overriding the source.
Required passwords, tokens, client secrets, and key passwords are collected
without echo. No option accepts one of those literal values.

### `edit`

```text
Usage: kantrip edit [OPTIONS] PROFILE

Edit an existing profile. With no options, open the interactive editor.

General:
  -b, --bootstrap-servers HOST:PORT[,HOST:PORT]
                                  Replace Kafka broker addresses.
  -d, --description TEXT          Replace the description.
      --clear-description         Remove the description.
  -l, --label KEY=VALUE           Add or replace a label; repeatable.
      --remove-label KEY          Remove a label; repeatable.

Kafka security:
      --transport TYPE             Replace the transport.
      --auth TYPE                  Replace the authentication method.
      --username TEXT              Replace the Kafka username.
      --ca-file PATH               Replace Kafka CA configuration.
      --system-ca                  Use the system CA store for Kafka TLS.
      --client-certificate-file PATH
                                   Replace the client certificate.
      --client-key-file PATH       Replace the private client key.
      --oauth-token-url URL        Replace the OAuth token endpoint.
      --oauth-client-id TEXT       Replace the OAuth client ID.
      --oauth-scope TEXT           Replace OAuth scopes; repeatable.

Registry:
      --registry-provider TYPE     Add or replace the provider.
      --registry-url URL           Add or replace the endpoint.
      --registry-auth TYPE         Replace Registry authentication.
      --registry-username TEXT     Replace the Registry username.
      --registry-ca-file PATH      Replace Registry CA configuration.
      --remove-registry            Remove the complete Registry connection.

Credentials:
      --replace-secret FIELD       Prompt for and replace an applicable secret;
                                   repeatable.
```

Every non-interactive option changes only the named field and preserves omitted
fields. The interactive editor offers `keep`, `replace`, or `remove` for each
existing secret without showing or prefilling its value. A removal is accepted
only when the same validated edit removes or changes the configuration that
requires that secret. Do not add `--remove-secret` or any literal secret-value
option. `--replace-secret` is also the recovery path when a referenced
credential-store entry is missing.

`FIELD` belongs to a closed, schema-derived vocabulary and is fully qualified
by its owner. Examples include `kafka/password`,
`kafka/oauth/client-secret`, `kafka/tls/private-key`, `registry/password`,
`registry/token`, and `registry/oauth/client-secret`. Reject unknown fields and
fields that do not apply to the profile's selected authentication method. Do
not keep ambiguous names such as `oauth/client-secret` or `tls/private-key`
once both Kafka and Registry can own that credential type.

### `remove`

```text
Usage: kantrip remove [OPTIONS] PROFILE

Remove a complete profile and its owned credentials.

Options:
      --force  Skip the confirmation prompt.
```

`remove` never removes only a Registry, label, certificate, or secret. Those
profile changes belong to `edit`. `--force` skips only the prompt and never
skips validation, locking, journaling, or safe credential cleanup.

### `list`

```text
Usage: kantrip list [OPTIONS]

List profiles.

Options:
  -l, --label KEY=VALUE            Require an exact label; repeatable and
                                   combined using logical AND.
  -o, --output human|json|yaml     Output representation. Default: human.
```

Human output adds a Labels column to the existing profile summary. Render each
label as `KEY=VALUE`. Assign presentation colors deterministically from the
label key using a bounded accessible palette; do not persist random label
colors or make color carry meaning. Labels remain embedded in their profiles,
so a label unused by every profile naturally disappears without a global label
registry or garbage-collection workflow. JSON and YAML represent labels as
key/value maps. An empty match is successful and produces an empty result in
the selected format.

### `describe`

```text
Usage: kantrip describe [OPTIONS] PROFILE

Describe a profile without exposing secret values or credential references.

Options:
  -o, --output human|json|yaml     Output representation. Default: human.
```

`describe` replaces `show`. Human output uses Rich sections consistent with
`doctor` for profile identity, Kafka, Registry, labels, and credential status.
JSON and YAML are stable machine-readable observations, not accepted input or
export documents. All formats omit secret values and internal credential references;
credential fields expose only safe states such as `stored`, `missing`, or
`unavailable`. Include the current profile revision in `describe` output.

### `doctor`

```text
Usage:
  kantrip doctor [OPTIONS] [PROFILE]
  kantrip doctor --repair [--verbose]

Inspect Kantrip globally or inspect one profile.

Options:
      --sessions  Include detailed sessions for PROFILE.
      --verbose   Include safe diagnostic details.
      --repair    Apply global deterministic repairs.
```

`kantrip doctor` remains the global diagnostic. `kantrip doctor PROFILE`
focuses profile, credential, certificate, and runtime checks on one profile.
`kantrip doctor PROFILE --sessions` lists that profile's validated active,
recent, and stale sessions without revealing private configuration paths or
contents. Use the plural `--sessions`; reserve singular `--session` for a future
operation that would select one exact session. Initially reject `PROFILE` with
`--repair`, because database migrations and the current unified repair contract
are global.

Associate each runtime marker with `profileId` and `profileRevision`; never
infer profile ownership by inspecting generated client configuration. A session
remains pinned to the validated profile revision captured when it started, and
an edit affects only later sessions. Show session IDs, supervisor PIDs, or
resolved runtime paths only when the existing safe verbose-output policy allows
them.

### Remaining commands

```text
Usage: kantrip ping [OPTIONS] PROFILE

Check Kafka and configured Registry connectivity.

Options:
      --timeout SECONDS  Maximum operation time. Default: 5.
  -q, --quiet            Emit no output; communicate through exit status.
```

```text
Usage: kantrip exec PROFILE [-- COMMAND...]

Run a command or interactive subshell using PROFILE.
```

```text
Usage: kantrip current

Print the profile active in the current Kantrip session.
```

The final top-level command list is therefore `add`, `edit`, `remove`, `list`,
`describe`, `doctor`, `ping`, `exec`, and `current`. It has the same number of
top-level commands as the current CLI while absorbing future input-source and
secret work into the lifecycle verbs users already understand.

### Representative lifecycle

```bash
kantrip add production \
  --bootstrap-servers kafka.example.com:9093 \
  --transport tls \
  --auth scram-sha-512 \
  --username app-production \
  --label environment=production \
  --label owner=platform

kantrip edit production --replace-secret kafka/password
kantrip edit production --registry-url https://registry.example.com
kantrip list --label environment=production --label owner=platform
kantrip describe production --output yaml
kantrip doctor production --sessions
```

This sequence uses the same resource lifecycle throughout: `add` creates,
`edit` changes, `list` selects, `describe` inspects, and `doctor` diagnoses.
Secrets enter only through no-echo prompts or approved input sources, and none
of the inspection commands can return them.

## 1. Credential store and profile lifecycle

### Remaining store work

- Store textual PEM private keys as credential values. Validate realistic PEM
  sizes against both supported store families before declaring mTLS complete.
- Treat public client certificate chains as non-secret. They may be copied into
  the profile for portability or referenced by a user-owned path; Kantrip never
  deletes a referenced source file.

### Recoverable updates

The SQLite profile database and OS credential store cannot participate in one
atomic transaction. Do not promise cross-store atomicity.

- For add or secret replacement, write new secrets under new immutable
  references, atomically switch the validated profile references, then delete
  superseded entries.
- If the database update fails, delete the newly staged entries and leave the old
  profile usable.
- If old-secret deletion fails after the profile switch, keep the new profile
  usable, report the orphan safely, and let `doctor --repair` or an idempotent
  retry remove the exact old reference.
- For profile removal, remove the profile in a SQLite transaction before deleting
  its exact credential keys. Report and reconcile any leftover orphan instead
  of restoring a profile whose secrets may already be partially deleted.
- Integrate the existing exact-reference reconciliation journal into every
  secret-bearing mutation. Commit intent before a credential-store write can
  create an orphan, advance it atomically with the profile switch, and retry it
  on later mutations. Standard `keyring` APIs cannot enumerate arbitrary
  orphaned entries, so no workflow may depend on backend listing support.
- Use SQLite write transactions for database concurrency and the existing
  maintenance lock around the journal/credential/database workflow.

### Commands

- Extend `kantrip add PROFILE` with authentication and the explicit
  `--from-properties` and `--from-strimzi` input sources.
- Extend `kantrip edit PROFILE` with every mutable profile field, including
  authentication, Registry TLS and authentication, and prompted secret
  replacement. With no options, open an interactive editor.
- For each existing secret, offer explicit keep, replace, or remove decisions.
  Never display or prefill the current value.
- Support direct prompted rotation or recovery through repeatable
  `kantrip edit PROFILE --replace-secret FIELD`; do not add a `secret` command
  group or a secret-reading interface.
- Add confirmation to secret-bearing profile removal and `--force` to skip
  only the prompt, not validation.
- Extend `doctor` with optional profile scope, detailed `--sessions`,
  locked-store, missing-reference, certificate-match, and certificate-expiry
  checks.

There must be no literal password, client-secret, token, JAAS, or private-key
value option. Generic secret-provider automation beyond the supported input
sources accepted by `add` is not part of this MVP.

### Profile revision and concurrency contract

Keep the existing stable profile ID, unique profile name, and monotonically
increasing `revision`. The revision is a generation and concurrency token, not
a retained version history: the MVP adds no history listing, rollback, or old
secret recovery.

Normal non-interactive edits remain serialized by the maintenance lock and a
SQLite `BEGIN IMMEDIATE` transaction. An edit that prompts for secret material
must not hold the lock while the user types. It instead reads the profile ID and
revision, collects and validates input, acquires the lock, reloads the profile,
and commits only with the expected generation:

```sql
UPDATE profiles
SET revision = revision + 1, document = ?
WHERE id = ? AND revision = ?
```

Exactly one row must change. A mismatch leaves the old profile usable, safely
discards or journals staged credential references, and asks the user to retry.
Every session records and remains bound to the revision from which its private
configuration was generated. The revision appears in `describe` and detailed
session diagnostics, but not in the default profile list.

## 2. Profile input sources

External documents are explicit input sources for `kantrip add`, not a separate
`import` command family. Every source creates a new Kantrip profile through the
same schema validation, credential staging, SQLite commit, and reconciliation
workflow as manual creation. A source never overwrites an existing profile.

### Client properties source

Add these creation forms:

```text
kantrip add PROFILE --from-properties FILE
kantrip add PROFILE --from-properties -
```

The input handler accepts supported Java properties, librdkafka properties, and
the client properties emitted by Confluent's public client-config generator.

- Parse only an allowlist of Kafka connection, TLS, authentication, and
  Confluent-compatible Registry properties.
- Parse Java property escaping and line continuation explicitly; do not treat a
  Java properties document as generic INI or YAML.
- Detect the Java or librdkafka dialect from recognized keys and reject mixed or
  ambiguous security configuration instead of guessing.
- Parse supported JAAS login-module syntax with a tokenizer that handles quoted
  values and escapes. Do not extract credentials with regular expressions.
- Accept only the PLAIN, SCRAM, mTLS, and OAuth client-credentials shapes
  represented by Kantrip's schema. Reject custom callback classes, arbitrary
  login modules, and unknown secret-bearing properties.
- Move passwords, API secrets, client secrets, inline private keys, and Registry
  credentials into the approved credential store before committing the profile.
- For a supported private-key path, resolve relative paths against the source
  properties file and read the key contents. Stdin input requires an absolute
  path because it has no stable source directory. Reject JKS and PKCS12 inputs
  rather than attempting an implicit conversion.
- Ignore recognized properties outside the connection allowlist, even when they
  are non-secret. Report ignored property names, never their values.
- Fail closed on unknown `ssl.*`, `sasl.*`, `*.auth.*`, token, password,
  secret, credential, certificate, or private-key properties. This prevents a
  misspelled or newer security property from being silently discarded.
- Normalize into the existing typed connection fields. Never reintroduce the
  removed generic `properties.common`, `properties.java`, or
  `properties.librdkafka` passthroughs.
- Treat an input file as user-owned and never modify or delete it. Recommend
  stdin for generated files that contain secrets so users do not need to leave
  another plaintext copy on disk.
- Do not read private implementation files belonging to another CLI and do not
  invoke a vendor CLI with secrets in argv.

A supported Confluent workflow is:

```bash
confluent kafka client-config create java |
  kantrip add production --from-properties -
```

### Strimzi credential source

Add these creation forms:

```text
kantrip add PROFILE --from-strimzi FILE [options]
kantrip add PROFILE --from-strimzi - [options]
```

- Accept exactly one Kubernetes `Secret` JSON or YAML document generated for a
  Strimzi `KafkaUser`, containing the standard `data.password`,
  `data.user.crt`, or `data.user.key` fields. The `KafkaUser` custom resource
  describes authentication and authorization but does not contain the
  generated credential values; reject `kind: KafkaUser` with guidance to read
  the same-named generated Secret instead.
- Decode and validate the base64 data, then normalize it into Kantrip's existing
  SCRAM or mTLS profile model. Do not create a separate Strimzi profile type.
- Require bootstrap servers and any cluster CA information not present in the
  KafkaUser Secret as explicit non-secret options or user-owned file paths.
- Store decoded secret values directly in the credential store and never write
  the source Secret document to a temporary file.
- Treat a source file as user-owned and never modify or delete it.
- Reject unsupported Secret shapes, conflicting TLS/SCRAM fields, missing
  connection metadata, and Kubernetes resources other than a single Secret.
- Do not call `kubectl` or discover listeners, namespaces, clusters, or CA
  Secrets automatically.

A supported file workflow is:

```bash
kantrip add production \
  --from-strimzi kafka-user-secret.yaml \
  --bootstrap-servers kafka.example.com:9093 \
  --ca-file cluster-ca.crt
```

A supported direct Kubernetes workflow keeps the credential document out of a
persistent plaintext file:

```bash
kubectl get secret kafka-user \
  --namespace kafka \
  --output yaml |
  kantrip add production \
    --from-strimzi - \
    --bootstrap-servers kafka.example.com:9093 \
    --ca-file cluster-ca.crt
```

Do not document `kubectl get kafkauser kafka-user --output yaml` as an input
source. Strimzi normally creates a credential-bearing Kubernetes Secret with
the same name as the `KafkaUser`; the generated Secret is the input Kantrip
needs. The explicit `--from-strimzi` name identifies this supported producer
and schema without implying that Kantrip accepts arbitrary Kubernetes Secrets
or invokes Kubernetes APIs itself.

## 3. Kafka TLS and authentication

Extend the current verified-TLS schema and canonical client-property
construction in this order:

1. SASL/PLAIN over TLS.
2. SCRAM-SHA-256 and SCRAM-SHA-512 over TLS.
3. Mutual TLS.
4. OAuth/OAUTHBEARER client credentials over TLS.

SASL without TLS is rejected except for explicitly documented loopback test
infrastructure.

### Standard Kafka client properties

Kantrip stores one provider-neutral connection model. Input normalization and
rendering use the following product-native property allowlist; profile documents do not
expose these names as an arbitrary property map.

| Connection concern | Java client properties | librdkafka properties |
| --- | --- | --- |
| Broker endpoints | `bootstrap.servers` | `bootstrap.servers` |
| Transport | `security.protocol` | `security.protocol` |
| Server CA | `ssl.truststore.type=PEM` plus `ssl.truststore.certificates`, or a verified PEM `ssl.truststore.location` | `ssl.ca.pem` or `ssl.ca.location` |
| Client certificate | `ssl.keystore.type=PEM` and `ssl.keystore.certificate.chain` | `ssl.certificate.pem` or `ssl.certificate.location` |
| Client private key | `ssl.keystore.key` and, when required, `ssl.key.password` | `ssl.key.pem` or `ssl.key.location` and, when required, `ssl.key.password` |
| TLS verification | `ssl.endpoint.identification.algorithm=HTTPS` | `enable.ssl.certificate.verification=true` and `ssl.endpoint.identification.algorithm=https` |
| SASL mechanism | `sasl.mechanism` | `sasl.mechanism` |
| PLAIN or SCRAM credentials | Kantrip-generated `sasl.jaas.config` | `sasl.username` and `sasl.password` |
| OAuth client credentials | `sasl.oauthbearer.token.endpoint.url`, `sasl.oauthbearer.client.credentials.client.id`, `sasl.oauthbearer.client.credentials.client.secret`, and optional `sasl.oauthbearer.scope` on verified current clients | `sasl.oauthbearer.method=oidc`, `sasl.oauthbearer.token.endpoint.url`, `sasl.oauthbearer.client.id`, `sasl.oauthbearer.client.secret`, and optional `sasl.oauthbearer.scope` |
| OAuth token-endpoint CA | The standard callback's documented SSL options when supported by the verified client version; otherwise system trust or a capability error | `https.ca.pem` or `https.ca.location` |

For Java clients that predate the direct
`sasl.oauthbearer.client.credentials.*` properties, Kantrip may generate the
standard `OAuthBearerLoginModule` JAAS options (`clientId`, `clientSecret`, and
optional `scope`) and the official `sasl.login.callback.handler.class`. That is
a version-gated renderer, not a second profile shape. Input processing may
accept either documented Java form and normalize both to the same profile fields.

`ssl.endpoint.identification.algorithm` cannot be empty, and librdkafka's
certificate verification cannot be false. Custom SSL engines, security
providers, SASL login modules, callbacks other than the verified Apache Kafka
callback, Kerberos, client assertions, unmanaged access tokens, and OAuth
metadata-provider modes are rejected.

Kafka producer, consumer, topic, group, serialization, timeout, retry,
buffering, compression, and observability properties are ignored. Examples
include `acks`, `group.id`, `auto.offset.reset`, `client.id`, serializers,
deserializers, and `request.timeout.ms`.

### Rendering

- Build one resolved in-memory session model, then render Java and librdkafka
  properties independently.
- Construct and escape Java JAAS internally. Raw JAAS input is not part of the
  profile schema.
- Materialize client certificates and private-key PEM only inside the private
  session directory when their source is the credential store.
- Prefer Java PEM properties on verified Kafka client versions. Older clients
  that require JKS or PKCS12 fail with an actionable capability error; Kantrip
  does not run conversion tools with secrets in argv.
- Use librdkafka's native TLS, SASL, and OIDC property names.

### OAuth scope

- Support the OAuth 2.0 client-credentials grant only.
- Keep the token endpoint, client ID, scopes, product-defined authentication
  routing identifiers, and token-endpoint CA separate from broker TLS
  configuration. Do not add a generic audience field unless a supported client
  has a documented native connection property for it.
- Store the client secret in the credential store.
- For librdkafka-based clients, use the built-in OIDC configuration so the
  client owns token refresh.
- For supported Java Kafka versions, render the built-in OAuth login callback
  configuration. If that callback is unavailable, fail before launch.

Refresh-token and fixed-access-token profile modes are deliberately deferred.
They add lifecycle and expiry behavior without being required for the initial
client-credentials use case.

## 4. Registry TLS and authentication

Extend the existing independent `registry` object for:

- HTTPS server verification with a system or profile CA.
- Basic authentication for Confluent-compatible and native Apicurio clients.
- OAuth client credentials for Confluent-compatible and native Apicurio clients.
- Fixed bearer-token authentication for Confluent-compatible clients only.
- Optional client certificate authentication when the selected provider and
  client version expose a verified native mapping.

Keep Confluent-compatible and native Apicurio property names separate.

### Confluent Schema Registry client properties

Use only the unprefixed standard Schema Registry client properties below.
Framework-specific prefixes are applied only by an adapter when that
framework's documented configuration format requires them.

| Connection concern | Standard properties and constraints |
| --- | --- |
| Registry endpoint | `schema.registry.url`; the MVP accepts exactly one URL to match the profile model |
| Server CA and mTLS | `ssl.truststore.type=PEM`, `ssl.truststore.certificates`, `ssl.keystore.type=PEM`, `ssl.keystore.certificate.chain`, `ssl.keystore.key`, and optional `ssl.key.password`; verified PEM file-location variants may be used when required by the client |
| TLS verification | `ssl.endpoint.identification.algorithm=HTTPS` |
| Basic authentication | `basic.auth.credentials.source=USER_INFO` and `basic.auth.user.info` |
| Fixed bearer token | `bearer.auth.credentials.source=STATIC_TOKEN` and `bearer.auth.token` |
| OAuth client credentials | `bearer.auth.credentials.source=OAUTHBEARER`, `bearer.auth.issuer.endpoint.url`, `bearer.auth.client.id`, `bearer.auth.client.secret`, and optional `bearer.auth.scope` |
| Required authorization routing | Optional `bearer.auth.logical.cluster` and `bearer.auth.identity.pool.id`, only when required by the target service |

Do not put credentials in `schema.registry.url`. Reject the `URL`,
`SASL_INHERIT`, `SASL_OAUTHBEARER_INHERIT`, and custom credential-provider
sources because they hide Registry identity inside a URL, Kafka configuration,
or executable code. Kantrip renders explicit Registry credentials from the
Registry portion of the profile.

### Native Apicurio Registry client properties

Use the standard Apicurio Registry client namespace without translating it to
Confluent property names.

| Connection concern | Standard properties and constraints |
| --- | --- |
| Registry endpoint | `apicurio.registry.url` |
| Server CA | `apicurio.registry.tls.truststore.type=PEM` with `apicurio.registry.tls.truststore.location`, or `apicurio.registry.tls.certificates` |
| TLS verification | `apicurio.registry.tls.verify-host=true`; `apicurio.registry.tls.trust-all` is never enabled |
| Basic authentication | `apicurio.registry.auth.username` and `apicurio.registry.auth.password` |
| OAuth client credentials | `apicurio.registry.auth.service.token.endpoint`, `apicurio.registry.auth.client.id`, and `apicurio.registry.auth.client.secret` |
| Client certificate | `apicurio.registry.tls.client-certificate` and `apicurio.registry.tls.client-key`, only for an Apicurio client version whose integration test proves support |

Kantrip does not invent an Apicurio property for a fixed bearer token or OAuth
scope. If a selected native client does not expose a documented standard
property required by the profile, the adapter fails before launch.

Properties that choose or mutate schema behavior are ignored and never stored
or injected. This includes `apicurio.registry.artifact.version`, artifact/group
IDs, resolver strategies, auto-registration, lookup strategy, ID/header
encoding, fallback behavior, cache, retry, and telemetry settings. The same rule
excludes Confluent serializer behavior such as subject-name strategies,
`auto.register.schemas`, `use.latest.version`, normalization, compatibility,
cache, and retry settings.

HTTP proxy properties are connection-related but remain outside this MVP. They
must be added later as explicit typed profile fields rather than admitted
through a generic property map.

- Extend Registry ping with `ssl.SSLContext` and explicit Authorization
  headers. Redact URLs and wrapped HTTP errors before presentation.
- Render Registry credentials only into a private provider-specific file or a
  child-only variable required by a verified client.
- Never merge Registry credentials into the Kafka properties file unless the
  target client requires a single combined format.
- Treat HTTP 401 as authentication failure and HTTP 403 as an authenticated but
  unauthorized response when the provider follows those semantics. Do not
  describe a resource-list request as permission-independent.

## 5. Connectivity diagnostics

Extend the existing `confluent-kafka` AdminClient probe; do not implement a
second Kafka protocol stack inside Kantrip.

- Resolve the secure profile, build the same librdkafka configuration used by a
  session, and perform a bounded metadata request.
- Classify DNS, TCP/timeout, TLS, broker authentication, and authorization
  errors using stable librdkafka error categories.
- A successful client construction alone is not a successful ping; at least one
  broker operation must complete.
- If authentication succeeds but metadata authorization is denied, report the
  connection/authentication stage as successful and authorization as untested
  or denied. Do not claim that `ping` is independent of broker ACLs.
- Continue accepting `kantrip ping PROFILE --timeout SECONDS`. Additional
  endpoint-policy or JSON interfaces require a separate demonstrated use case.

## 6. Client adapters

### Existing adapters

Extend the existing Apache Kafka, Confluent Platform, Confluent Registry console,
kcat, and Kaskade adapters only for the authentication combinations their
underlying clients actually support. Each matrix entry needs a minimum tested
client version and a fail-closed capability error.

Do not duplicate OAuth acquisition in an adapter when the underlying Java or
librdkafka client already implements client-credentials refresh.

### New adapters

Add only:

- `kcl`, using a private temporary TOML profile selected through
  `KCL_CONFIG_PATH`. Reject explicit bootstrap, profile, config-path, TLS, and
  authentication overrides.
- `kafkactl`, using a private temporary YAML configuration selected through
  `KAFKA_CTL_CONFIG`. Point `KAFKA_CTL_WRITABLE_CONFIG` inside the Kantrip
  session and disable kafkactl's own keyring integration so it cannot persist a
  second copy of Kantrip-managed secrets.

TLS, PLAIN, and SCRAM are required for both new adapters. mTLS and OAuth are
enabled only after version-pinned integration tests prove a safe native mapping.
Unsupported combinations fail before the child starts.

Add both explicit-command adaptation and temporary subshell shims. Use
documented config-path environment variables so secrets never appear in argv.

## Delivery order

1. Complete recoverable secret-bearing profile updates on top of the credential
   store, reconciliation journal, and plaintext `edit` lifecycle.
2. Add PLAIN, SCRAM, and mTLS to the schema and shared renderers.
3. Add the properties and Strimzi input sources to `add`.
4. Extend the existing adapters and authenticated `ping` for those mechanisms.
5. Add secure Registry connections.
6. Add OAuth client credentials through verified native Java and librdkafka
   mechanisms.
7. Add the `kcl` and `kafkactl` adapters.
8. Run the complete Linux/macOS integration and security matrix and synchronize
   all current-feature documentation.

Credential storage precedes authenticated profiles so secrets never need a
temporary insecure representation. Basic TLS and SASL precede OAuth so the
renderer and adapter boundaries are proven before token lifecycle is added.

## Remaining completion criteria

- Concurrent profile updates, partial keyring failures, and orphan cleanup leave
  either the old usable profile or the new usable profile, never a silently
  half-updated profile.
- `edit` can secure an existing Registry connection without displaying or
  unintentionally replacing existing secrets.
- The properties input source accepts verified Java, librdkafka, and
  Confluent-generated fixtures, extracts their secrets, and rejects ambiguous or
  unsupported security configuration.
- Input normalization and rendering cover only the documented Kafka, Confluent
  Schema Registry, and native Apicurio connection-property allowlists. Non-connection
  properties are reported by name and never persisted or injected; unknown
  security-like properties fail closed.
- The Strimzi input source accepts standard TLS and SCRAM Kubernetes Secret
  fixtures generated for a `KafkaUser`, from a file or stdin, without persisting
  the decoded source document.
- Profile-scoped doctor output can associate detailed sessions with the exact
  profile ID and revision on which each session started.
- PLAIN, both SCRAM mechanisms, and mTLS pass unit, renderer, adapter, and
  disposable-cluster integration tests.
- OAuth client credentials refresh through verified Java and librdkafka native
  mechanisms without exposing the client secret.
- Secure Confluent-compatible connections pass TLS, basic, fixed-bearer, OAuth,
  and applicable client-certificate tests. Native Apicurio connections pass
  TLS, basic, OAuth, and applicable client-certificate tests.
- Authenticated `ping` distinguishes configuration, transport, TLS,
  authentication, and authorization outcomes without claiming more than the
  underlying operation proves.
- Existing adapters preserve their current plaintext behavior and gain only
  tested authentication combinations.
- `kcl` and `kafkactl` work in explicit and interactive sessions for every
  combination marked supported.
- README, usage, compatibility, architecture, threat model, schema, examples,
  and release artifacts describe exactly the implemented matrix.

## Outside the MVP

- Database downgrades, user-authored migrations, and migration identities tied
  to product release numbers.
- Detached or managed background sessions.
- Automatic Kubernetes discovery.
- Arbitrary secret-provider automation beyond the supported `add` input sources.
- Secret retrieval, round-trip profile export, profile cloning, retained profile
  history, and rollback to old secret values.
- Refresh-token and fixed-access-token Kafka OAuth profiles.
- The Strimzi OAuth module and its custom Java login callback. Strimzi
  credential input remains in scope because it only normalizes KafkaUser TLS
  and SCRAM credentials.
- Amazon MSK IAM authentication.
- Windows support.
- Automatic certificate-store conversion.
- HTTP proxy profiles.
- Additional adapters without a versioned compatibility contract and
  integration tests.
- A hosted service, daemon, Docker distribution, programmatic profile API, or
  interactive TUI.
