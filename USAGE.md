# Kantrip Usage

Kantrip is in pre-release development. The current CLI exposes help, version,
and global presentation behavior:

```bash
kantrip --help
kantrip --version
kantrip --no-color --help
```

Profile, connectivity, execution, and cleanup commands documented below define
the planned workflow but are not implemented yet.

## Planned profile workflow

```bash
kantrip add local
kantrip list
kantrip use local
kantrip current
kantrip show local
kantrip ping local
kantrip exec local -- java -jar application.jar
```

Omitting the command after `kantrip exec PROFILE` opens an interactive supervised
subshell. Kantrip never exports a selected profile into the parent shell.

## Profile configuration

Profile metadata is YAML validated against
[`schemas/profile-v1.schema.json`](https://github.com/sauljabin/kantrip/blob/main/schemas/profile-v1.schema.json).
A synthetic example is available at
[`examples/config.yaml`](https://github.com/sauljabin/kantrip/blob/main/examples/config.yaml).

Planned configuration lookup order is:

1. `KANTRIP_CONFIG`.
2. `$XDG_CONFIG_HOME/kantrip/config.yaml`.
3. `~/.config/kantrip/config.yaml`.

Profile YAML stores only non-secret metadata and operating-system credential
references. Passwords, tokens, private keys, secret-bearing JAAS strings, and
keystore passwords are rejected as literal profile fields.

## Application environment from `kantrip exec`

Kantrip publishes connection settings only to the supervised child process and
its descendants. The variables Kantrip may set are listed below.

Kafka clients share configuration-property names but do not define one
cross-language environment-variable standard. Kantrip therefore uses generic
`KAFKA_*` and `SCHEMA_REGISTRY_*` names for settings an application may consume.
Only Kantrip-specific session metadata uses `KANTRIP_*`.

Applications must opt in to these variables. Kantrip also generates
client-specific property files and adapters may pass those files or the
appropriate flags directly to supported tools.

### Kafka variables

| Variable | Meaning |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated broker addresses |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL` |
| `KAFKA_JAVA_CONFIG_FILE` | Generated Java Kafka properties path |
| `KAFKA_LIBRDKAFKA_CONFIG_FILE` | Generated librdkafka properties path |
| `KAFKA_SASL_MECHANISM` | Present when SASL is configured |
| `KAFKA_SASL_USERNAME` | Present when the mechanism uses a username |
| `KAFKA_SASL_PASSWORD` | Present when the mechanism uses a password |
| `KAFKA_OAUTH_TOKEN_ENDPOINT` | Present when OAuth token acquisition is configured |
| `KAFKA_OAUTH_CLIENT_ID` | Present when OAuth client identity is configured |
| `KAFKA_OAUTH_CLIENT_SECRET` | Present when OAuth client credentials are used |
| `KAFKA_OAUTH_ACCESS_TOKEN` | Present when a fixed or acquired token is used |
| `KAFKA_SSL_CA_LOCATION` | Materialized or referenced CA bundle path |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | Materialized client certificate path |
| `KAFKA_SSL_KEY_LOCATION` | Materialized client private-key path |
| `KAFKA_SSL_KEY_PASSWORD` | Present when the private key is encrypted |

### Schema Registry variables

These variables are present only when the selected profile configures Schema
Registry.

| Variable | Meaning |
| --- | --- |
| `SCHEMA_REGISTRY_CONFIG_FILE` | Generated Schema Registry properties path |
| `SCHEMA_REGISTRY_URL` | Registry URL |
| `SCHEMA_REGISTRY_USERNAME` | Present for basic authentication |
| `SCHEMA_REGISTRY_PASSWORD` | Present for basic authentication |
| `SCHEMA_REGISTRY_TOKEN` | Present for bearer authentication |
| `SCHEMA_REGISTRY_SSL_CA_LOCATION` | Materialized or referenced CA bundle path |
| `SCHEMA_REGISTRY_SSL_CERTIFICATE_LOCATION` | Materialized client certificate path |
| `SCHEMA_REGISTRY_SSL_KEY_LOCATION` | Materialized client private-key path |
| `SCHEMA_REGISTRY_SSL_KEY_PASSWORD` | Present when the private key is encrypted |

### Kantrip session metadata

| Variable | Meaning |
| --- | --- |
| `KANTRIP_PROFILE` | Selected profile display name |
| `KANTRIP_SESSION_ID` | Opaque session identifier |
| `KANTRIP_SESSION_DIR` | Private temporary session directory |

Secret values are intentionally child-only. A user-selected child can read
them; Kantrip protects them at rest and from shell history, not from the process
the user explicitly launches. Applications should prefer their native generated
file where practical, fall back to the documented variables, and never log the
resolved environment or generated properties.

## Output and color

Normal command output goes to stdout and diagnostics go to stderr. Styling is
disabled when:

- `--no-color` is supplied.
- `NO_COLOR` is present in the environment.
- `TERM=dumb`.
- The destination stream is not a terminal.

Secret classification occurs before values reach Rich. Styling never changes
exit statuses or becomes necessary to interpret an error.
