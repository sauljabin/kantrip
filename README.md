<p align="center">
<a href="https://github.com/sauljabin/kantrip"><img alt="Kantrip" width="400" src="https://raw.githubusercontent.com/sauljabin/kantrip/main/images/banner.svg"></a>
</p>

<p align="center">
<a href="https://github.com/sauljabin/kantrip/actions/workflows/main.yml"><img alt="CI status" src="https://img.shields.io/github/actions/workflow/status/sauljabin/kantrip/main.yml?branch=main&style=flat-square&logo=githubactions&logoColor=white&label=ci"></a>
<a href="https://github.com/sauljabin/kantrip/blob/main/LICENSE"><img alt="MIT License" src="https://img.shields.io/github/license/sauljabin/kantrip?style=flat-square&logo=opensourceinitiative&logoColor=white&label=license"></a>
<a href="https://github.com/sponsors/sauljabin"><img alt="Sponsor on GitHub" src="https://img.shields.io/badge/sponsor-GitHub-EA4AAA?style=flat-square&logo=githubsponsors&logoColor=white"></a>
<br>
<a href="https://pypi.org/project/kantrip"><img alt="PyPI version" src="https://img.shields.io/pypi/v/kantrip?style=flat-square&logo=pypi&logoColor=white&label=pypi"></a>
<br>
<a href="https://pypi.org/project/kantrip"><img alt="Linux support" src="https://img.shields.io/badge/os-Linux-7C3AED?style=flat-square&logo=linux&logoColor=white"></a>
<a href="https://pypi.org/project/kantrip"><img alt="macOS support" src="https://img.shields.io/badge/os-macOS-7C3AED?style=flat-square&logo=apple&logoColor=white"></a>
</p>

Kantrip securely manages plaintext Kafka profiles for kcat, the official Kafka
CLIs, Kaskade, and compatible applications. This pre-release CLI validates and
displays profiles, opens scoped sessions, and checks Kafka and registry
connectivity.

## Features

### Plaintext profiles

- Schema-validated profiles in a private transactional SQLite database
- Multiple bootstrap servers, descriptions, and optional Registry endpoints
- `add`, `remove`, `list`, and redacted `show` commands

### Local diagnostics

- Profile database, permissions, platform, shell, and session checks
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
- Private Kaskade 5 configuration for admin, consumer, Confluent Schema
  Registry, and native Apicurio Registry decoding
- Consistent Bash, Zsh, and Fish subshells plus one-off command execution
- Process-group and PTY supervision with child exit-status preservation
- Crash-safe runtime locks, automatic stale-session recovery, and `cleanup`

### Terminal experience

- Rich-based Arcana theme with semantic colors
- Clean stdout/stderr separation
- `NO_COLOR`, `TERM=dumb`, non-TTY behavior, and global or command-local
  `--no-color`

## Roadmap

The MVP roadmap has three stages:

1. Add local credential-store integration and recoverable profile updates.
2. Add TLS and authenticated Kafka profiles and adapters.
3. Add authenticated registry connections, OAuth integration, and authenticated
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

`add` creates `~/.local/share/kantrip/profiles.db` and defaults to
`localhost:9092`. Use `-b HOST:PORT[,HOST:PORT]` for brokers, `-d` for a
description, and `--registry-url http://HOST:PORT` for a plain Confluent Schema
Registry. Add
`--registry-provider apicurio` for a native Apicurio Core Registry API v3 URL.
Remove profiles with `kantrip remove PROFILE`; `list` shows both endpoints.

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
