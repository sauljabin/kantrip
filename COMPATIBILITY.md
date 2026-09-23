# Compatibility

Use this guide to choose clients, connection methods, and file formats supported
by the current Kantrip commands. See [Usage](USAGE.md) for command examples and
configuration instructions.

## Client commands

Kantrip recognizes commands by executable basename. [Apache Kafka's Unix binary
archives](https://kafka.apache.org/quickstart/) name their command scripts with a
`.sh` suffix. [Confluent Platform](https://docs.confluent.io/kafka/operations-tools/kafka-tools.html)
installs the corresponding Kafka commands without `.sh` in
`$CONFLUENT_HOME/bin`, alongside additional Confluent-only commands. Kantrip
recognizes both names for the seven shared Kafka commands; the six
[Schema Registry console commands](https://github.com/confluentinc/schema-registry/tree/master/bin)
are Confluent commands and are unsuffixed.

Apache Kafka 2.6 is the oldest CLI surface Kantrip guarantees; newer clients can
connect to older brokers subject to Apache Kafka's normal client/broker
compatibility.

Interactive sessions support Bash, Zsh, and Fish on Linux and macOS. Kantrip
loads normal user startup configuration and history, then restores its temporary
adapter executables after startup-time aliases, functions, abbreviations, and
`PATH` changes. When `SHELL` is unset, an installed Bash is used. Other shells
are not supported.

Direct commands run in an isolated POSIX process group, while supported
interactive shells use a PTY-owned session with terminal resizing and job
control. Signal forwarding and stale-session recovery apply identically to every
adapter. Detached processes and children that create a new POSIX session remain
outside the supported lifecycle.

| Supported executable(s) | Distribution | CLI version | Kafka behavior | Registry support | Notes |
| --- | --- | --- | --- | --- | --- |
| `kafka-console-consumer.sh` / `kafka-console-consumer` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Consume records | No | Injects `--bootstrap-server` and `--consumer.config`; Kafka 4.3 deprecates the config flag ahead of its planned Kafka 5.0 removal. |
| `kafka-console-producer.sh` / `kafka-console-producer` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Produce records | No | Injects `--bootstrap-server` and `--producer.config`; Kafka 4.3 deprecates the config flag ahead of its planned Kafka 5.0 removal. |
| `kafka-topics.sh` / `kafka-topics` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Create, list, describe, alter, and delete topics | No | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-consumer-groups.sh` / `kafka-consumer-groups` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Inspect and manage consumer groups | No | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-configs.sh` / `kafka-configs` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Inspect and alter supported dynamic configurations | No | Injects `--bootstrap-server` and `--command-config`; broker authorization still applies. |
| `kafka-acls.sh` / `kafka-acls` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | List, add, and remove ACLs | No | Injects `--bootstrap-server` and `--command-config`; requires a configured authorizer and an authorized principal. |
| `kafka-broker-api-versions.sh` / `kafka-broker-api-versions` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Inspect broker protocol versions | No | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-avro-console-consumer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Consume Avro records | Confluent-compatible | Injects the Kafka consumer connection and `schema.registry.url` from the profile. |
| `kafka-avro-console-producer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Produce Avro records | Confluent-compatible | Injects the Kafka producer connection and `schema.registry.url` from the profile. |
| `kafka-json-schema-console-consumer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Consume JSON Schema records | Confluent-compatible | Injects the Kafka consumer connection and `schema.registry.url` from the profile. |
| `kafka-json-schema-console-producer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Produce JSON Schema records | Confluent-compatible | Injects the Kafka producer connection and `schema.registry.url` from the profile. |
| `kafka-protobuf-console-consumer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Consume Protobuf records | Confluent-compatible | Injects the Kafka consumer connection and `schema.registry.url` from the profile. |
| `kafka-protobuf-console-producer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Produce Protobuf records | Confluent-compatible | Injects the Kafka producer connection and `schema.registry.url` from the profile. |
| `kcat` / `kafkacat` | kcat | kcat 1.7+; librdkafka 2.6.1+ for SCRAM against Kafka 4 | Metadata, produce, and consume | Confluent-compatible Avro | Uses a private `KCAT_CONFIG`; when `-s avro`, `-s key=avro`, or `-s value=avro` is selected, injects `-r` from the profile. Explicit `-F`, `-r`, and `-X schema.registry.url=...` overrides are rejected. |
| `kaskade` | Kaskade | Kaskade 5.0+ | Administer and consume | Confluent and native Apicurio | Uses a private INI file for `admin` and `consumer`; Avro, JSON Schema, and Protobuf registry deserializers select a provider-specific `[registry]` section. `--kafka group.id=...` and `--kafka broker.address.family=v4\|v6\|any` are allowed; other Kafka, config-file, and registry connection overrides are rejected. |

“Profile-aware” means Kantrip maps the selected profile into the command. The
Confluent console clients and kcat require `--registry-provider confluent`. This includes
Apicurio's `/apis/ccompat/v7` endpoint, which uses Confluent framing. Native
Apicurio `/apis/registry/v3` profiles work only with Kaskade registry
deserializers and use Apicurio's default `contentId` framing.

Every listed Kafka adapter supports plaintext, verified TLS, SASL/PLAIN,
SCRAM-SHA-256, SCRAM-SHA-512, and mTLS. Authentication always requires verified
TLS. TLS uses each client's default trust store
unless a validated custom PEM CA is copied from the profile into each private
session. The
librdkafka adapters (`kcat`, `kafkacat`, and Kaskade) support that PEM directly.
Java adapters
require [Apache Kafka 2.7+](https://kafka.apache.org/27/security/encryption-and-authentication-using-ssl/)
or Confluent Platform 6.1+, where native PEM trust stores became available;
Kantrip checks the installed client version and fails before the Kafka operation
when support cannot be verified. Kafka 2.6 and Confluent Platform 6.0 remain
supported with default client trust. Native OAuth requires Apache Kafka 4.0+
for Java commands and an OIDC-capable librdkafka client. Unsupported
authentication is rejected before the requested operation.

Registry connections may use unauthenticated HTTP, or verified HTTPS with
independent Basic, fixed-token, mTLS, or OAuth credentials. Each adapter admits
only mappings exposed safely by that concrete client; unsupported combinations
fail before launch. Kantrip rejects missing, provider-incompatible, and
caller-supplied Registry settings before starting the affected client mode.
Bash, Zsh, and Fish sessions
apply the same checks through temporary adapters. The session removes reserved
connection namespaces and known sandbox credentials before launch and restores
its owned values after shell startup; see [environment precedence](USAGE.md#environment-precedence).

## Kafka transport and authentication

| Mode | `add` / `edit` | `exec` and client commands | `ping` |
| --- | --- | --- | --- |
| Plaintext, no authentication | Supported | Supported | Broker reachability |
| Verified TLS, no client authentication | Supported | Supported; Java custom CA needs Kafka 2.7 / Confluent 6.1 or newer | Server identity and broker connection |
| SASL/PLAIN over TLS | Supported | Supported | SASL exchange |
| SCRAM-SHA-256 over TLS | Supported | Supported | SASL exchange |
| SCRAM-SHA-512 over TLS | Supported | Supported | SASL exchange |
| mTLS | Supported | Supported; Java PEM identity needs Kafka 2.7 / Confluent 6.1 or newer | Configured client exchange |
| OAuth / OAUTHBEARER | Supported | Java requires Kafka 4.0+; librdkafka uses OIDC | TLS, token acquisition, and SASL exchange |
| SASL without TLS / disabled TLS verification | Unsupported | Unsupported | Unsupported |

## Registry protocols and providers

| Provider / mode | Current support |
| --- | --- |
| Confluent-compatible HTTP/HTTPS, no auth | Confluent consoles, kcat Avro, Kaskade, and ping |
| Native Apicurio HTTP/HTTPS, no auth | Kaskade Registry deserializers and ping |
| Confluent Basic, OAuth, or mTLS | Private prefixed config and provider-aware ping. Java OAuth uses one `ssl.*` CA bundle for Registry and IdP. Kaskade's Confluent Python OAuth receives a process-private default-roots-plus-IdP-CA bundle through `SSL_CERT_FILE`; Registry CA remains `ssl.ca.location`. Both OAuth clients require a logical cluster identifier. |
| Native Apicurio Basic or mTLS | Private Kaskade INI and provider-aware ping. Kaskade supports private CA trust and unencrypted PEM mTLS keys; encrypted PEM keys are rejected. |
| Native Apicurio OAuth | The official `apicurio.registry.tls.certificates` bundle is shared by Registry and IdP. Distinct CA fields are accepted only when identical. Kaskade 5.0.1+ is required when OAuth scopes are configured; profiles without scopes retain compatibility with earlier releases. Kaskade keeps Registry and token HTTP/TLS contexts separate so Registry client identity does not reach the IdP. |
| Confluent fixed bearer | Profile and probe support; clients without a safe fixed-token mapping reject it |
| Kafka credential inheritance / URL credentials | Rejected |

Registry ping uses `GET /subjects?limit=1` for Confluent-compatible APIs and
`GET /search/versions?limit=1` for native Apicurio v3. Confluent authorization
classifies subject listing as `GLOBAL_READ`, not `SCHEMA_READ`; standard
Apicurio RBAC permits version search to `sr-readonly`, `sr-developer`, and
`sr-admin`. Authenticated profiles also require the same query to reject an
anonymous request with 401/403 (or reject a missing client certificate for
mTLS). A valid empty result succeeds and does not prove access to a particular
schema. Proxies must allow the selected endpoint; no fallback probes
`/users/me`, `/system/info`, `/schemas/types`, or artifact search. Kafka ping
uses broker connection state and does not request topics, groups, schemas, or
cluster descriptions. Neither probe proves write authorization. `doctor PROFILE` scopes profile checks and
`doctor PROFILE --sessions` attributes sessions by UUID and revision. `kcl` and `kafkactl` have no automatic
adapter, even though an arbitrary executable can run as a supervised child.

## File formats

| Format | Current accepted input | Current generated output / role |
| --- | --- | --- |
| JSON / YAML observations | No profile import or round-trip export | `list` / `describe` safe observations |
| Public PEM CA | Kafka, Registry, and OAuth CA file options | Validated public profile material; independent session-owned CA files |
| Client PEM certificate / private key | Kafka and Registry certificate/key options | Public certificate in the profile; private key in the credential store and private session files |
| Java Kafka `.properties` | No file import | Private Java client session configuration |
| librdkafka / kcat properties | No file import yet | Private librdkafka configuration selected through `KCAT_CONFIG` and documented file variables |
| Confluent-generated client properties | No file or stdin import yet | Not a retained vendor config/cache |
| Strimzi generated Secret JSON / YAML | No file or stdin import | No Kubernetes resource output |
| Kaskade INI | No profile import | Private `[kafka]` and optional provider-specific `[registry]` session config |
| Registry properties | No file import | Private HTTP endpoint configuration only |
| kcl TOML / kafkactl YAML | Unsupported | No generated adapter config yet |
| JKS / PKCS12 | Unsupported for Kantrip input | No automatic conversion |

## Installing supported commands

Kantrip does not install client programs. Install the relevant distribution and
put its `bin` directory on `PATH` before running `kantrip exec`.

| Commands | macOS | Linux |
| --- | --- | --- |
| The seven Apache `*.sh` commands in the compatibility table | Install an Apache Kafka binary archive from [Apache Kafka downloads](https://kafka.apache.org/downloads), then add its `bin` directory to `PATH`. Homebrew's `brew install kafka` is also suitable. | Install an Apache Kafka binary archive from [Apache Kafka downloads](https://kafka.apache.org/downloads), then add its `bin` directory to `PATH`. |
| The seven equivalent unsuffixed Kafka commands and all six unsuffixed `kafka-{avro,json-schema,protobuf}-console-{producer,consumer}` commands | Download and extract a Confluent Platform or Confluent Community ZIP/TAR package, set `CONFLUENT_HOME`, and add `$CONFLUENT_HOME/bin` to `PATH`. | Use the same ZIP/TAR method, or install Confluent's `confluent-community`/`confluent-platform` packages and add their `bin` directory to `PATH`. See [Confluent Platform installation](https://docs.confluent.io/platform/current/installation/overview.html). |
| `kcat`, `kafkacat` | `brew install kcat` | Debian/Ubuntu: `apt install kafkacat`; other distributions can use their package manager or follow the [kcat build instructions](https://github.com/edenhill/kcat#install). The installed legacy executable may be named `kafkacat`. For SCRAM against Kafka 4, check the linked library with `kcat -V`: Ubuntu 24.04's `librdkafka` 2.3.0 is too old, so install or build librdkafka 2.6.1+ as well. |
| `kaskade` | `brew install kaskade` or `pipx install kaskade` | `pipx install kaskade`; see the [Kaskade installation guide](https://github.com/sauljabin/kaskade#installation). |

The Confluent archive contains both its unsuffixed Kafka scripts and the Schema
Registry console scripts in `$CONFLUENT_HOME/bin`; installing the standalone
`confluent` management CLI does not provide these Java console clients. Keep the
client version aligned with the Confluent Platform/Schema Registry release you
use.
