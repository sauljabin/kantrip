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

Kantrip securely manages plaintext and server-authenticated TLS Kafka profiles
for kcat, the official Kafka CLIs, Kaskade, and compatible applications. This
pre-release CLI validates and displays profiles, opens scoped sessions, and
checks Kafka and registry connectivity.
The typed connection core also models, validates, resolves, and renders PLAIN,
SCRAM-SHA-256, SCRAM-SHA-512, and mTLS; authenticated command sessions and
connectivity checks remain roadmap work.

## Features

### Kafka profiles

- Schema-validated profiles in a private transactional SQLite database
- Ordered automatic schema migrations with private recovery backups
- Multiple bootstrap servers, descriptions, labels, and optional Registry endpoints
- Plaintext or verified TLS transport with default client trust or a copied PEM CA bundle
- Typed PLAIN, SCRAM, and mTLS profile shapes with OS-backed secret references
- Canonical Java and librdkafka authentication renderers
- `add`, `edit`, `remove`, label-filtered `list`, and safe `describe` commands
- Human, JSON, and YAML profile observations

### Local diagnostics

- Profile database, credential backend, permissions, platform, shell, and session checks
- Read-only diagnostics with explicit deterministic repair through `doctor --repair`
- Explicit Kafka protocol connectivity checks with `ping`
- Installed-command discovery for Kantrip's supported adapters
- Colored status output with plain-text and `NO_COLOR` support

### Compatible CLI sessions

- `kcat` and its `kafkacat` alias
- Apache Kafka `.sh` commands and equivalent unsuffixed Confluent Platform commands
- Confluent Avro, JSON Schema, and Protobuf console producers and consumers
- Kaskade `admin` and `consumer`, including Confluent and native Apicurio decoding
- Private, profile-aware client configuration with connection overrides blocked
- Consistent Bash, Zsh, and Fish subshells plus one-off command execution
- Process-group and PTY supervision with child exit-status preservation
- Crash-safe runtime locks, automatic stale-session recovery, and `doctor --repair`

### Terminal experience

- Rich-based Arcana theme with semantic colors
- Clean stdout/stderr separation
- `NO_COLOR`, `TERM=dumb`, non-TTY behavior, and global or command-local
  `--no-color`

## Roadmap

The MVP roadmap has three stages:

1. Add properties and Strimzi input sources for authenticated Kafka profiles.
2. Connect authenticated profiles to existing adapters and `ping`.
3. Add secure Registry connections, OAuth, new adapters, and the full platform
   security matrix.

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
kantrip describe local
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

## Command compatibility

See the [compatibility guide](https://github.com/sauljabin/kantrip/blob/main/COMPATIBILITY.md)
for supported tools, versions, and limitations.

## Usage

See the [usage guide](https://github.com/sauljabin/kantrip/blob/main/USAGE.md).

## Development

See the [development guide](https://github.com/sauljabin/kantrip/blob/main/DEVELOPMENT.md).

Run the valuable exploratory scenarios in the
[manual testing guide](https://github.com/sauljabin/kantrip/blob/main/MANUAL_TESTING.md).

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
