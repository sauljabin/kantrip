<p align="center">
<a href="https://github.com/sauljabin/kantrip"><img alt="Kantrip" width="400" src="https://raw.githubusercontent.com/sauljabin/kantrip/main/images/banner.svg"></a>
</p>

<p align="center">
<a href="https://github.com/sauljabin/kantrip/actions/workflows/main.yml"><img alt="CI status" src="https://img.shields.io/github/actions/workflow/status/sauljabin/kantrip/main.yml?branch=main&style=flat-square&logo=githubactions&logoColor=white&label=ci"></a>
<a href="https://github.com/sauljabin/kantrip/blob/main/LICENSE"><img alt="MIT License" src="https://img.shields.io/github/license/sauljabin/kantrip?style=flat-square&logo=opensourceinitiative&logoColor=white&label=license"></a>
<a href="https://github.com/sponsors/sauljabin"><img alt="Sponsor on GitHub" src="https://img.shields.io/badge/sponsor-GitHub-EA4AAA?style=flat-square&logo=githubsponsors&logoColor=white"></a>
<br>
<a href="https://pypi.org/project/kantrip"><img alt="PyPI version" src="https://img.shields.io/pypi/v/kantrip?style=flat-square&logo=pypi&logoColor=white&label=pypi"></a>
</p>

Kantrip securely manages local Kafka profiles for command-line tools and compatible applications.

Kantrip is in active pre-release development. The current CLI loads and validates
profiles, displays their non-secret metadata, and opens plaintext profile sessions
for kcat, the official Apache Kafka CLIs, Kaskade, and compatible applications.

## Features

### Secure profile model

- Versioned, non-secret Kafka profile schema
- Operating-system credential-store references instead of plaintext secrets
- Explicit TLS, SASL, OAuth, Schema Registry, Strimzi, and MSK IAM models
- Classified values and safe diagnostic redaction

### Application environment

- Child-process `KAFKA_*` and `SCHEMA_REGISTRY_*` variables
- Java, librdkafka, and Schema Registry configuration paths
- Child-only credential exposure with no parent-shell export
- Explicit opt-in for compatible applications

### CLI sessions

- Native kcat configuration through a private, temporary `KCAT_CONFIG` file
- Profile-aware official Apache Kafka commands, with and without `.sh`
- Private client-file integration for Kaskade admin and consumer modes
- Interactive subshells and one-off command execution
- Child exit-status preservation and automatic session cleanup

### Terminal experience

- Rich-based Arcana theme with semantic colors
- Clean stdout/stderr separation
- `NO_COLOR`, `TERM=dumb`, `--no-color`, and non-TTY behavior

## Current limitations

- Persistent profile selection and secure-store integration are not implemented yet
- Execution currently supports only plaintext profiles without authentication
- `kantrip ping` and credential-backed adapters are not available yet
- Linux and macOS are the only planned MVP platforms
- No Docker distribution or hosted service is planned

## Quick start

### pipx

```bash
pipx install kantrip
```

### First profile session

```bash
kantrip add local
kantrip list
kantrip show local
kantrip exec local -- kcat -L
kantrip exec local -- kafka-topics --list
kantrip exec local -- kaskade admin
```

Omit the command to work in a profile-scoped interactive subshell:

```bash
kantrip exec local
kantrip current
kcat -L
kafka-topics --list
exit
```

`add` creates `~/.config/kantrip/config.yaml` when necessary and adds a plaintext
profile for `localhost:9092`. Pass `--bootstrap-server HOST:PORT` to choose a
different broker. Use `kantrip remove PROFILE` to remove one. `kantrip list`
prints no profile rows when the configuration is absent or empty.

## Command compatibility

Kantrip recognizes commands by executable basename. Apache Kafka 2.6 is the
oldest CLI surface Kantrip guarantees; newer clients can connect to older brokers
subject to Apache Kafka's normal client/broker compatibility. Both unsuffixed
commands and the `.sh` variants shipped in Apache Kafka distributions are
supported.

| Supported command | CLI version | Kafka | Confluent Schema Registry | Apicurio Registry | Important information |
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

## Usage

For configuration and usage examples, see the [Kantrip usage guide](https://github.com/sauljabin/kantrip/blob/main/USAGE.md).

## Development

For development instructions, see the [Kantrip development guide](https://github.com/sauljabin/kantrip/blob/main/DEVELOPMENT.md).

## Releases

See [GitHub Releases](https://github.com/sauljabin/kantrip/releases) for release
notes and downloadable artifacts.

## Questions

For Q&A, go to [GitHub Discussions](https://github.com/sauljabin/kantrip/discussions/categories/q-a).

## Security

Report suspected vulnerabilities privately by following the
[Kantrip security policy](https://github.com/sauljabin/kantrip/blob/main/SECURITY.md).

## Donations

If Kantrip is useful to you, consider
[supporting its development on GitHub Sponsors](https://github.com/sponsors/sauljabin).

## AI Assistance

This project uses AI-assisted development tools. Some code and documentation
may be generated or revised with AI assistance. All AI-assisted changes are
reviewed and tested by the maintainer before they are included.

## Acknowledgements

<p>
<a href="https://github.com/littlehorse-enterprises/littlehorse"><img alt="Sponsored by LittleHorse" src="https://raw.githubusercontent.com/sauljabin/kantrip/main/images/littlehorse-badge.svg"></a>
<a href="https://github.com/Textualize/rich"><img alt="Built with Rich" src="https://raw.githubusercontent.com/sauljabin/kantrip/main/images/rich-badge.svg"></a>
</p>
