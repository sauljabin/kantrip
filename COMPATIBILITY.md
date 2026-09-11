# Command compatibility

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

| Supported executable(s) | Distribution | CLI version | Kafka behavior | Schema Registry support | Notes |
| --- | --- | --- | --- | --- | --- |
| `kafka-console-consumer.sh` / `kafka-console-consumer` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Consume records | No | Injects `--bootstrap-server` and `--consumer.config`; Kafka 4.3 deprecates the config flag ahead of its planned Kafka 5.0 removal. |
| `kafka-console-producer.sh` / `kafka-console-producer` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Produce records | No | Injects `--bootstrap-server` and `--producer.config`; Kafka 4.3 deprecates the config flag ahead of its planned Kafka 5.0 removal. |
| `kafka-topics.sh` / `kafka-topics` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Create, list, describe, alter, and delete topics | No | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-consumer-groups.sh` / `kafka-consumer-groups` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Inspect and manage consumer groups | No | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-configs.sh` / `kafka-configs` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Inspect and alter supported dynamic configurations | No | Injects `--bootstrap-server` and `--command-config`; broker authorization still applies. |
| `kafka-acls.sh` / `kafka-acls` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | List, add, and remove ACLs | No | Injects `--bootstrap-server` and `--command-config`; requires a configured authorizer and an authorized principal. |
| `kafka-broker-api-versions.sh` / `kafka-broker-api-versions` | Apache / Confluent | Kafka 2.6–4.3; Confluent Platform 6.0–8.3 | Inspect broker protocol versions | No | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-avro-console-consumer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Consume Avro records | Yes, profile-aware | Injects the Kafka consumer connection with `--command-config` and `schema.registry.url` from the profile. |
| `kafka-avro-console-producer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Produce Avro records | Yes, profile-aware | Injects the Kafka producer connection with `--command-config` and `schema.registry.url` from the profile. |
| `kafka-json-schema-console-consumer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Consume JSON Schema records | Yes, profile-aware | Injects the Kafka consumer connection with `--command-config` and `schema.registry.url` from the profile. |
| `kafka-json-schema-console-producer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Produce JSON Schema records | Yes, profile-aware | Injects the Kafka producer connection with `--command-config` and `schema.registry.url` from the profile. |
| `kafka-protobuf-console-consumer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Consume Protobuf records | Yes, profile-aware | Injects the Kafka consumer connection with `--command-config` and `schema.registry.url` from the profile. |
| `kafka-protobuf-console-producer` | Confluent | Confluent Platform / Schema Registry 5.5–8.3 | Produce Protobuf records | Yes, profile-aware | Injects the Kafka producer connection with `--command-config` and `schema.registry.url` from the profile. |
| `kcat` / `kafkacat` | kcat | kcat 1.7+ | Metadata, produce, and consume | Yes, profile-aware for Avro | Uses a private `KCAT_CONFIG`; when `-s avro`, `-s key=avro`, or `-s value=avro` is selected, injects `-r` from the profile. Explicit `-F`, `-r`, and `-X schema.registry.url=...` overrides are rejected. |
| `kaskade` | Kaskade | Kaskade 5.0+ | Administer and consume | Yes, profile-aware for Avro, JSON Schema, and Protobuf | Uses a private INI file for `admin` and `consumer`; registry deserializers select a second private file containing `[registry]`. Explicit Kafka, config-file, and registry connection options are rejected. |

“Profile-aware” means Kantrip maps the selected profile into the command. kcat
supports Schema Registry-backed Avro decoding. Kaskade 5 supports registry-backed
Avro, JSON Schema, and Protobuf decoding.

Schema-aware Confluent console commands, kcat Avro deserializers, and Kaskade 5
registry deserializers require a profile `schemaRegistry` connection using an
`http://` URL and `auth.type: none`. Kantrip rejects missing, authenticated, or
TLS-secured registry settings before starting those client modes. It also
rejects caller-supplied connection overrides. Bash, Zsh, and Fish sessions apply
the same checks through temporary adapters.

## Installing supported commands

Kantrip does not install client programs. Install the relevant distribution and
put its `bin` directory on `PATH` before running `kantrip exec`.

| Commands | macOS | Linux |
| --- | --- | --- |
| The seven Apache `*.sh` commands in the compatibility table | Install an Apache Kafka binary archive from [Apache Kafka downloads](https://kafka.apache.org/downloads), then add its `bin` directory to `PATH`. Homebrew's `brew install kafka` is also suitable. | Install an Apache Kafka binary archive from [Apache Kafka downloads](https://kafka.apache.org/downloads), then add its `bin` directory to `PATH`. |
| The seven equivalent unsuffixed Kafka commands and all six unsuffixed `kafka-{avro,json-schema,protobuf}-console-{producer,consumer}` commands | Download and extract a Confluent Platform or Confluent Community ZIP/TAR package, set `CONFLUENT_HOME`, and add `$CONFLUENT_HOME/bin` to `PATH`. | Use the same ZIP/TAR method, or install Confluent's `confluent-community`/`confluent-platform` packages and add their `bin` directory to `PATH`. See [Confluent Platform installation](https://docs.confluent.io/platform/current/installation/overview.html). |
| `kcat`, `kafkacat` | `brew install kcat` | Debian/Ubuntu: `apt install kafkacat`; other distributions can use their package manager or follow the [kcat build instructions](https://github.com/edenhill/kcat#install). The installed legacy executable may be named `kafkacat`. |
| `kaskade` | `brew install kaskade` or `pipx install kaskade` | `pipx install kaskade`; see the [Kaskade installation guide](https://github.com/sauljabin/kaskade#installation). |

The Confluent archive contains both its unsuffixed Kafka scripts and the Schema
Registry console scripts in `$CONFLUENT_HOME/bin`; installing the standalone
`confluent` management CLI does not provide these Java console clients. Keep the
client version aligned with the Confluent Platform/Schema Registry release you
use.
