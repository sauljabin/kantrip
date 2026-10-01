# Compatibility

Which clients, connection methods, and file formats the current version of
Kantrip supports, and where each one stops. [Usage](USAGE.md) has the command
examples.

## Contents

- [Client commands](#client-commands)
- [Kafka transport and authentication](#kafka-transport-and-authentication)
- [Registry protocols and providers](#registry-protocols-and-providers)
- [Credential storage](#credential-storage)
- [File formats](#file-formats)
- [Installing supported commands](#installing-supported-commands)

## Client commands

Kantrip recognizes a client by the name of its executable, following symlinks
as described in [Short command names](USAGE.md#short-command-names).
[Apache Kafka's binary archives](https://kafka.apache.org/quickstart/) name their
scripts with a `.sh` suffix.
[Confluent Platform](https://docs.confluent.io/kafka/operations-tools/kafka-tools.html)
installs the same Kafka commands without `.sh` in `$CONFLUENT_HOME/bin`, next to
its own extra commands. Kantrip accepts both names for the seven shared Kafka
commands. The six
[Schema Registry console commands](https://github.com/confluentinc/schema-registry/tree/master/bin)
come only with Confluent and have no suffix. Other programs, such as
`kafkactl`, have no adapter, but you can still run them under `kantrip exec`.

The oldest supported Kafka CLI is Apache Kafka 2.6. Newer clients can talk to
older brokers as far as Kafka's usual client/broker compatibility allows.

Before every launch, whether you run the client directly or from a subshell,
Kantrip reads the installed client's version once and refuses a release older
than the minimum. `kantrip doctor` shows the same result for each installed
client. Some profile features need a newer release than the minimum, and those
checks use the same version:

| Client | Minimum version | Also checked for some profiles |
| --- | --- | --- |
| Apache Kafka Java CLIs | Kafka 2.6 | 2.7 for a custom CA or mTLS; 4.1 for OAuth |
| Confluent Platform Java CLIs | Confluent Platform 6.0 | 6.1 for a custom CA or mTLS; 8.1 for OAuth |
| Schema Registry consoles | Confluent Platform 5.5 | As for Confluent Platform |
| `kcat` / `kafkacat` | kcat 1.7.0 | librdkafka 2.11.0 for OAuth with a custom token-endpoint CA |
| `kaskade` | Kaskade 5.0.1 | None |
| `kaf` | kaf 0.2.14 | None |
| `kcl` | kcl 0.20.0 | None |

kcat, Kaskade, kaf, and kcl have no upper version limit. The Java CLIs do:
Kantrip tells Apache Kafka from Confluent Platform by the major version, and
recognizes Apache Kafka 2 to 4 and Confluent Platform 5 to 8. A release with
any other major version, such as Apache Kafka 5 or Confluent Platform 9, fails
the check.

Java releases may carry Confluent's `-ccs` or `-ce` suffix. For the other
clients, a version with a suffix, such as a development or pre-release build,
fails the check. SCRAM against Kafka 4 brokers also needs librdkafka 2.6.1 or
newer. Kantrip doesn't check that one, because it doesn't know the broker's
version.

Subshells can be Bash, Zsh, or Fish, on Linux and macOS; other shells aren't
supported. Processes that detach or start a new POSIX session aren't
supervised.

| Supported executable(s) | Distribution | Accepted versions | Kafka behavior | Registry support | Notes |
| --- | --- | --- | --- | --- | --- |
| `kafka-console-consumer.sh` / `kafka-console-consumer` | Apache / Confluent | Kafka 2.6–4.x; Confluent Platform 6.0–8.x | Consume records | No | Injects `--bootstrap-server` and `--consumer.config`; Kafka 4.3 deprecates the config flag ahead of its planned Kafka 5.0 removal. |
| `kafka-console-producer.sh` / `kafka-console-producer` | Apache / Confluent | Kafka 2.6–4.x; Confluent Platform 6.0–8.x | Produce records | No | Injects `--bootstrap-server` and `--producer.config`; Kafka 4.3 deprecates the config flag ahead of its planned Kafka 5.0 removal. |
| `kafka-topics.sh` / `kafka-topics` | Apache / Confluent | Kafka 2.6–4.x; Confluent Platform 6.0–8.x | Create, list, describe, alter, and delete topics | No | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-consumer-groups.sh` / `kafka-consumer-groups` | Apache / Confluent | Kafka 2.6–4.x; Confluent Platform 6.0–8.x | Inspect and manage consumer groups | No | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-configs.sh` / `kafka-configs` | Apache / Confluent | Kafka 2.6–4.x; Confluent Platform 6.0–8.x | Inspect and alter supported dynamic configurations | No | Injects `--bootstrap-server` and `--command-config`; broker authorization still applies. |
| `kafka-acls.sh` / `kafka-acls` | Apache / Confluent | Kafka 2.6–4.x; Confluent Platform 6.0–8.x | List, add, and remove ACLs | No | Injects `--bootstrap-server` and `--command-config`; requires a configured authorizer and an authorized principal. |
| `kafka-broker-api-versions.sh` / `kafka-broker-api-versions` | Apache / Confluent | Kafka 2.6–4.x; Confluent Platform 6.0–8.x | Inspect broker protocol versions | No | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-avro-console-consumer` | Confluent | Confluent Platform 5.5–8.x | Consume Avro records | Confluent-compatible | Injects the Kafka consumer connection and `schema.registry.url` from the profile. |
| `kafka-avro-console-producer` | Confluent | Confluent Platform 5.5–8.x | Produce Avro records | Confluent-compatible | Injects the Kafka producer connection and `schema.registry.url` from the profile. |
| `kafka-json-schema-console-consumer` | Confluent | Confluent Platform 5.5–8.x | Consume JSON Schema records | Confluent-compatible | Injects the Kafka consumer connection and `schema.registry.url` from the profile. |
| `kafka-json-schema-console-producer` | Confluent | Confluent Platform 5.5–8.x | Produce JSON Schema records | Confluent-compatible | Injects the Kafka producer connection and `schema.registry.url` from the profile. |
| `kafka-protobuf-console-consumer` | Confluent | Confluent Platform 5.5–8.x | Consume Protobuf records | Confluent-compatible | Injects the Kafka consumer connection and `schema.registry.url` from the profile. |
| `kafka-protobuf-console-producer` | Confluent | Confluent Platform 5.5–8.x | Produce Protobuf records | Confluent-compatible | Injects the Kafka producer connection and `schema.registry.url` from the profile. |
| `kcat` / `kafkacat` | kcat | kcat 1.7.0+ | Metadata, produce, and consume | Confluent-compatible Avro | Uses a private `KCAT_CONFIG`; when `-s avro`, `-s key=avro`, or `-s value=avro` is selected, injects `-r` from the profile. Explicit `-F`, `-r`, and `-X schema.registry.url=...` overrides are rejected. |
| `kaf` | kaf | kaf 0.2.14+ | Topics, groups, produce, and consume | Confluent-compatible Avro | Uses a private one-cluster YAML through `--config`, with `HOME=/dev/null`. `--config`, `-b`/`--brokers`, `-c`/`--cluster`, `--schema-registry`, and `kaf config` commands are rejected. The Registry must use no auth or Basic with system trust; any other Registry profile fails before launch. |
| `kcl` | kcl | kcl 0.20.0+ | Topics, groups, cluster administration, produce, consume, and Registry administration | Confluent-compatible | Uses a private TOML file through `KCL_CONFIG_PATH` and removes inherited `KCL_*` variables. `-B`, `-R`, `-C`, `--config-path`, `--no-config-file`, `--config-env-prefix`, `-X` keys other than the three timeouts, and `kcl profile` commands are rejected. Registry OAuth and native Apicurio fail before launch. |
| `kaskade` | Kaskade | Kaskade 5.0.1+ | Administer and consume | Confluent and native Apicurio | Uses a private INI file for `admin` and `consumer`; Avro, JSON Schema, and Protobuf registry deserializers select a provider-specific `[registry]` section. `--kafka group.id=...` and `--kafka broker.address.family=v4\|v6\|any` are allowed; other Kafka, config-file, and registry connection overrides are rejected. |

The Confluent console clients and kcat need a profile with
`--registry-provider confluent`. That includes Apicurio's `/apis/ccompat/v7`
endpoint, which uses Confluent's framing. Native Apicurio profiles
(`/apis/registry/v3`) work only with Kaskade's Registry deserializers, using
Apicurio's default `contentId` framing.

## Kafka transport and authentication

TLS uses each client's default trust store, unless the profile has a custom PEM
CA, which Kantrip copies into each session. The librdkafka clients (`kcat`,
`kafkacat`, and Kaskade) read that PEM directly; the Java clients need the
newer releases in the version table above, the first with
[PEM trust stores](https://kafka.apache.org/27/security/encryption-and-authentication-using-ssl/).
kaf and kcl can't read encrypted mTLS keys, so they get a decrypted copy in a
private session file. A mode a client can't use fails before the operation
starts.

| Mode | `add` / `edit` | `exec` and client commands | `ping` |
| --- | --- | --- | --- |
| Plaintext, no authentication | Supported | Supported | Broker reachability |
| Verified TLS, no client authentication | Supported | Supported | Server identity and broker connection |
| SASL/PLAIN over TLS | Supported | Supported | SASL exchange |
| SCRAM-SHA-256 over TLS | Supported | Supported | SASL exchange |
| SCRAM-SHA-512 over TLS | Supported | Supported | SASL exchange |
| mTLS | Supported | Supported | Configured client exchange |
| OAuth / OAUTHBEARER | Supported | Supported, except kaf and kcl | TLS, token acquisition, and SASL exchange |
| SASL without TLS / disabled TLS verification | Unsupported | Unsupported | Unsupported |

## Registry protocols and providers

The Registry can be plain HTTP without authentication, or verified HTTPS with
its own Basic, fixed-token, mTLS, or OAuth credentials. Each client gets only
the settings it can take safely, and any other combination fails before
launch. So does a missing Registry, one from the wrong provider, or Registry
settings passed on the command line.

| Provider / mode | Current support |
| --- | --- |
| Confluent-compatible HTTP/HTTPS, no auth | Confluent consoles, kcat Avro, Kaskade, kaf Avro (system trust only), kcl, and ping |
| Native Apicurio HTTP/HTTPS, no auth | Kaskade Registry deserializers and ping |
| Confluent Basic, OAuth, or mTLS | Private prefixed config and provider-aware ping. kaf maps Basic with system trust only. kcl maps Basic and mTLS with system or custom CA trust, but not OAuth. Java OAuth uses one `ssl.*` CA bundle for Registry and IdP. Kaskade's Confluent Python OAuth receives a process-private default-roots-plus-IdP-CA bundle through `SSL_CERT_FILE`; Registry CA remains `ssl.ca.location`. Both OAuth clients require a logical cluster identifier. |
| Native Apicurio Basic or mTLS | Private Kaskade INI and provider-aware ping. Kaskade supports private CA trust and unencrypted PEM mTLS keys; encrypted PEM keys are rejected. |
| Native Apicurio OAuth | The official `apicurio.registry.tls.certificates` bundle is shared by Registry and IdP. Distinct CA fields are accepted only when identical. Kaskade keeps Registry and token HTTP/TLS contexts separate so Registry client identity does not reach the IdP. |
| Confluent fixed bearer | Profile and ping support. Confluent's Avro, JSON Schema, and Protobuf consoles receive it as `bearer.auth.credentials.source=STATIC_TOKEN` with `bearer.auth.token`, and kcl as its bearer token; `SCHEMA_REGISTRY_LIBRDKAFKA_CONFIG_FILE` carries the same `bearer.auth.*` keys. Kaskade Registry decoding, kaf, and kcat Avro reject it before launch. |
| Kafka credential inheritance / URL credentials | Rejected |

[Kafka and registry connectivity](USAGE.md#kafka-and-registry-connectivity)
explains what `ping` checks and which Registry permissions it needs.

## Credential storage

| Platform | Where credentials live | Locking |
| --- | --- | --- |
| macOS | The Kantrip vault, a dedicated keychain at `~/Library/Keychains/kantrip.keychain-db`, read and written only through `/usr/bin/security` | Locks after 15 idle minutes and on sleep unless you change it in Keychain Access; unlocks on the terminal |
| Linux with GNOME Keyring (GNOME, COSMIC, and other desktops that run it) | The Kantrip vault, a dedicated keyring `kantrip` at `~/.local/share/keyrings/kantrip.keyring` | Locks only at logout; unlocks in a desktop window |
| Linux with KDE Wallet (KDE Plasma) | The Kantrip vault, a dedicated wallet `kantrip` at `~/.local/share/kwalletd/kantrip.kwl` | Locks at logout, or when unused for the time set in KDE's settings; unlocks in a desktop window |
| Windows, and other Secret Service providers such as KeePassXC | Unsupported | Not applicable |

The Linux vault was tested with GNOME Keyring 46 and 50 and with KDE
Frameworks 6.24 (`ksecretd`). [Credential vault](USAGE.md#credential-vault)
explains creating, unlocking, and deleting it.

## File formats

| Format | Accepted as input | What Kantrip generates |
| --- | --- | --- |
| JSON / YAML | No profile import or round-trip export | `list` and `describe` output, without secrets |
| Public PEM CA | Kafka, Registry, and OAuth CA file options | Validated public profile material; independent session-owned CA files |
| Client PEM certificate / private key | Kafka and Registry certificate/key options | Public certificate in the profile; private key in the [credential store](#credential-storage) and private session files |
| Java Kafka `.properties` | The Kafka connection through `add --from-properties FILE`, or `-` for stdin: brokers, PEM trust, PLAIN, SCRAM, PEM mTLS, and OAuth client credentials; see [First profile](USAGE.md#first-profile) | Private Java client session configuration |
| librdkafka properties | The same Kafka connection through `add --from-properties FILE` or `-`, told apart from Java by its keys | Private librdkafka configuration, passed to kcat through `KCAT_CONFIG` and to other programs through `KAFKA_LIBRDKAFKA_CONFIG_FILE` |
| Confluent-generated client properties | Java and librdkafka output of `confluent kafka client-config create` through `add --from-properties`, as generated without a Schema Registry key, or with its Registry lines removed | Not a retained vendor config/cache |
| Strimzi generated Secret JSON / YAML | One SCRAM-SHA-512 or TLS `KafkaUser` Secret through `add --from-strimzi FILE`, or `-` for stdin; see [First profile](USAGE.md#first-profile) | No Kubernetes resource output |
| Kaskade INI | No profile import | Private `[kafka]` and optional provider-specific `[registry]` session config |
| Registry properties | No file import; Registry keys in `--from-properties` input fail | Private HTTP endpoint configuration only |
| kcl TOML | No file import | Private kcl session configuration selected through `KCL_CONFIG_PATH` |
| kafkactl YAML | Unsupported | No generated adapter config |
| JKS / PKCS12 | Unsupported for Kantrip input | No automatic conversion |

## Installing supported commands

Kantrip doesn't install clients. Install the ones you need and put their `bin`
directory on `PATH` before running `kantrip exec`.

| Commands | macOS | Linux |
| --- | --- | --- |
| The seven Apache `*.sh` commands in the compatibility table | Install an Apache Kafka binary archive from [Apache Kafka downloads](https://kafka.apache.org/downloads), then add its `bin` directory to `PATH`. Homebrew's `brew install kafka` is also suitable. | Install an Apache Kafka binary archive from [Apache Kafka downloads](https://kafka.apache.org/downloads), then add its `bin` directory to `PATH`. |
| The seven equivalent unsuffixed Kafka commands and all six unsuffixed `kafka-{avro,json-schema,protobuf}-console-{producer,consumer}` commands | Download and extract a Confluent Platform or Confluent Community ZIP/TAR package, set `CONFLUENT_HOME`, and add `$CONFLUENT_HOME/bin` to `PATH`. | Use the same ZIP/TAR method, or install Confluent's `confluent-community`/`confluent-platform` packages and add their `bin` directory to `PATH`. See [Confluent Platform installation](https://docs.confluent.io/platform/current/installation/overview.html). |
| `kcat`, `kafkacat` | `brew install kcat` | Debian/Ubuntu: `apt install kafkacat`; other distributions can use their package manager or follow the [kcat build instructions](https://github.com/edenhill/kcat#install). The installed legacy executable may be named `kafkacat`. Check the linked library with `kcat -V`: Ubuntu 24.04's `librdkafka` 2.3.0 is too old for SCRAM against Kafka 4 (see the version notes above). |
| `kaf` | `brew install kaf` | Download the release archive from [kaf releases](https://github.com/birdayz/kaf/releases) and put `kaf` on `PATH`. |
| `kcl` | Download `kcl_darwin_arm64.gz` or `kcl_darwin_amd64.gz` from [kcl releases](https://github.com/twmb/kcl/releases), decompress it as `kcl`, make it executable, and put it on `PATH`. | Download `kcl_linux_amd64.gz` or `kcl_linux_arm64.gz` from [kcl releases](https://github.com/twmb/kcl/releases), decompress it as `kcl`, make it executable, and put it on `PATH`. |
| `kaskade` | `brew install kaskade` or `pipx install kaskade` | `pipx install kaskade`; see the [Kaskade installation guide](https://github.com/sauljabin/kaskade#installation). |

The Confluent archive has both the unsuffixed Kafka scripts and the Schema
Registry console scripts in `$CONFLUENT_HOME/bin`. The standalone `confluent`
management CLI doesn't include these Java console clients. Use a client version
that matches your Confluent Platform or Schema Registry release.
