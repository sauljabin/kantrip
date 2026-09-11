# Command compatibility

Kantrip recognizes commands by executable basename. Apache Kafka 2.6 is the
oldest CLI surface Kantrip guarantees; newer clients can connect to older brokers
subject to Apache Kafka's normal client/broker compatibility. Both unsuffixed
commands and the `.sh` variants shipped in Apache Kafka distributions are
supported.

Interactive sessions support Bash, Zsh, and Fish on Linux and macOS. Kantrip
loads normal user startup configuration and history, then restores its temporary
adapter executables after startup-time aliases, functions, abbreviations, and
`PATH` changes. When `SHELL` is unset, an installed Bash is used. Other shells
are not supported.

| Supported command | CLI version | Kafka behavior | Notes |
| --- | --- | --- | --- |
| `kafka-console-consumer[.sh]` | Apache Kafka 2.6–4.3 | Consume records | Injects `--bootstrap-server` and `--consumer.config`; Kafka 4.3 deprecates the config flag ahead of its planned Kafka 5.0 removal. |
| `kafka-console-producer[.sh]` | Apache Kafka 2.6–4.3 | Produce records | Injects `--bootstrap-server` and `--producer.config`; Kafka 4.3 deprecates the config flag ahead of its planned Kafka 5.0 removal. |
| `kafka-topics[.sh]` | Apache Kafka 2.6+ | Create, list, describe, alter, and delete topics | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-consumer-groups[.sh]` | Apache Kafka 2.6+ | Inspect and manage consumer groups | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-configs[.sh]` | Apache Kafka 2.6+ | Inspect and alter supported dynamic configurations | Injects `--bootstrap-server` and `--command-config`; broker authorization still applies. |
| `kafka-acls[.sh]` | Apache Kafka 2.6+ | List, add, and remove ACLs | Injects `--bootstrap-server` and `--command-config`; requires a configured authorizer and an authorized principal. |
| `kafka-broker-api-versions[.sh]` | Apache Kafka 2.6+ | Inspect broker protocol versions | Injects `--bootstrap-server` and `--command-config`. |
| `kcat` / `kafkacat` | kcat 1.7+ | Metadata, produce, and consume | Uses a private `KCAT_CONFIG`; explicit `-F` is rejected. |
| `kaskade` | Kaskade 4.0+ | Administer and consume | Uses a private INI file for `admin` and `consumer`. |
