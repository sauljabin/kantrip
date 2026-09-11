# Command compatibility

Kantrip recognizes commands by executable basename. Apache Kafka 2.6 is the
oldest CLI surface Kantrip guarantees; newer clients can connect to older brokers
subject to Apache Kafka's normal client/broker compatibility. Both unsuffixed
commands and the `.sh` variants shipped in Apache Kafka distributions are
supported.

| Supported command | CLI version | Kafka | Confluent Schema Registry | Apicurio Registry | Notes |
| --- | --- | --- | --- | --- | --- |
| `kafka-console-consumer[.sh]` | Apache Kafka 2.6–4.3 | Consume records | — | — | Injects `--bootstrap-server` and `--consumer.config`; Kafka 4.3 deprecates the config flag ahead of its planned Kafka 5.0 removal. |
| `kafka-console-producer[.sh]` | Apache Kafka 2.6–4.3 | Produce records | — | — | Injects `--bootstrap-server` and `--producer.config`; Kafka 4.3 deprecates the config flag ahead of its planned Kafka 5.0 removal. |
| `kafka-topics[.sh]` | Apache Kafka 2.6+ | Create, list, describe, alter, and delete topics | — | — | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-consumer-groups[.sh]` | Apache Kafka 2.6+ | Inspect and manage consumer groups | — | — | Injects `--bootstrap-server` and `--command-config`. |
| `kafka-configs[.sh]` | Apache Kafka 2.6+ | Inspect and alter supported dynamic configurations | — | — | Injects `--bootstrap-server` and `--command-config`; broker authorization still applies. |
| `kafka-acls[.sh]` | Apache Kafka 2.6+ | List, add, and remove ACLs | — | — | Injects `--bootstrap-server` and `--command-config`; requires a configured authorizer and an authorized principal. |
| `kafka-broker-api-versions[.sh]` | Apache Kafka 2.6+ | Inspect broker protocol versions | — | — | Injects `--bootstrap-server` and `--command-config`. |
| `kcat` / `kafkacat` | kcat 1.7+ | Metadata, produce, and consume | Not yet integrated | Not yet integrated | Uses a private `KCAT_CONFIG`; explicit `-F` is rejected. |
| `kaskade` | Kaskade 4.0+ | Administer and consume | Not yet integrated | Not yet integrated | Uses a private INI file; registry profile settings are not passed yet. |

An em dash means the command does not use a registry. Registry integrations in
the underlying tool do not become profile-aware until Kantrip explicitly maps
and tests them.
