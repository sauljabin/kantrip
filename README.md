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

Kantrip securely manages plaintext Kafka profiles for kcat, the official Kafka
CLIs, Kaskade, and compatible applications. This pre-release CLI validates and
displays profiles, opens scoped sessions, and checks Kafka and Schema Registry
connectivity.

## Features

### Plaintext profiles

- Schema-validated YAML configuration with atomic profile updates
- Multiple bootstrap servers, descriptions, labels, and client properties
- `add`, `remove`, `list`, and redacted `show` commands

### Local diagnostics

- Configuration, permissions, profile, platform, shell, and session checks
- Explicit Kafka protocol connectivity checks with `ping`
- Installed-command discovery for Kantrip's supported adapters
- Colored status output with plain-text and `NO_COLOR` support

### CLI sessions

- Native kcat configuration through a private `KCAT_CONFIG`, with Schema
  Registry injection for Avro
- Profile-aware Apache Kafka `.sh` commands and the equivalent unsuffixed
  Confluent Platform commands
- Profile-aware Avro, JSON Schema, and Protobuf console clients for plain
  Confluent Schema Registry connections
- Private Kaskade 5 configuration for admin, consumer, and registry decoding
- Consistent Bash, Zsh, and Fish subshells plus one-off command execution
- Child exit-status preservation and automatic session cleanup

### Terminal experience

- Rich-based Arcana theme with semantic colors
- Clean stdout/stderr separation
- `NO_COLOR`, `TERM=dumb`, `--no-color`, and non-TTY behavior

## Roadmap

The MVP roadmap has three stages:

1. Harden profile sessions and add local credential-store integration.
2. Add TLS and authenticated Kafka profiles and adapters.
3. Add authenticated Schema Registry, OAuth integration, and authenticated
   connectivity checks.

See the [MVP roadmap](MVP.md) for scope and non-goals.

## Quick start

### pipx

```bash
pipx install kantrip
```

### First profile session

```bash
kantrip add local
kantrip doctor
kantrip list
kantrip show local
kantrip ping local
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

`add` creates `~/.config/kantrip/config.yaml` and defaults to `localhost:9092`.
Use `-b HOST:PORT[,HOST:PORT]` for brokers, `-d` for a description, and
`--schema-registry-url http://HOST:PORT` for a plain registry. Remove profiles
with `kantrip remove PROFILE`; `list` shows both endpoints.

## Command compatibility

See the [compatibility guide](https://github.com/sauljabin/kantrip/blob/main/COMPATIBILITY.md)
for supported tools, versions, and limitations.

## Usage

See the [usage guide](https://github.com/sauljabin/kantrip/blob/main/USAGE.md).

## Development

See the [development guide](https://github.com/sauljabin/kantrip/blob/main/DEVELOPMENT.md).

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

The project uses AI-assisted development. The maintainer reviews and tests all
AI-assisted changes before inclusion.

## Acknowledgements

<p>
<a href="https://github.com/littlehorse-enterprises/littlehorse"><img alt="Sponsored by LittleHorse" src="https://raw.githubusercontent.com/sauljabin/kantrip/main/images/littlehorse-badge.svg"></a>
<a href="https://github.com/Textualize/rich"><img alt="Built with Rich" src="https://raw.githubusercontent.com/sauljabin/kantrip/main/images/rich-badge.svg"></a>
</p>
