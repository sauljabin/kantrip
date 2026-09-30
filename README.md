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

Kantrip stores your Kafka connection profiles and hands the right one to the
client you're about to run. You describe a cluster once (brokers, TLS,
credentials, and optionally a Schema Registry), then run your usual tools
through it:

```bash
kantrip exec prod -- kcat -L
kantrip exec prod -- kafka-topics --list
```

For each run, Kantrip writes that client's own config format to a private
temporary directory, starts the client, and deletes the files when it exits.
Passwords, private keys, and client secrets live in a separate vault in your
OS credential store: a dedicated keychain on macOS, or a dedicated GNOME
Keyring or KDE Wallet collection on Linux. They never go into the profile or
onto the command line.

Supported clients are kcat, the Apache Kafka and Confluent CLIs (including the
Schema Registry console producers and consumers), Kaskade, kaf, and kcl. Other
programs you start with `kantrip exec` can read the connection from `KAFKA_*`
variables and generated config files. Connections can be plaintext or verified
TLS, with SASL/PLAIN, SCRAM, mTLS, or OAuth where the client supports it.
Authentication always requires verified TLS. [Compatibility](COMPATIBILITY.md)
lists what works with each client.

There is no global "current profile". You name the profile on every command,
or open a subshell for it, so switching profiles in one terminal never changes
what another terminal connects to.

## What it does

- **Profiles.** `add`, `edit`, `remove`, `list`, and `describe`, with JSON and
  YAML output for scripts. A profile can have several bootstrap servers, a
  description, labels to filter by, and one Schema Registry (Confluent or
  Apicurio) with its own TLS and credentials.
- **Secrets.** Kantrip asks for passwords and client secrets at a prompt that
  doesn't echo, and reads private keys from files. All of them go into the
  vault. `describe` never opens the vault; it only says which secrets are
  configured.
- **Custom CAs.** A private CA bundle is validated and copied into the profile,
  so the original file can move or disappear later.
- **Client configuration.** Each client gets the options or config file it
  already understands. Arguments that would change the connection, such as
  `-b` or `--bootstrap-server`, are rejected. Kantrip checks each client's
  version before starting it and names the release to install when it's too
  old.
- **Subshells.** `kantrip exec prod` opens Bash, Zsh, or Fish with the
  supported clients already set up for that profile. Your startup files and
  history still load, and nothing is left behind after `exit`.
- **Supervision.** Kantrip forwards Ctrl-C and other signals to the client and
  exits with the client's exit code. Session files left by a crash are cleaned
  up on a later run.
- **Checks.** `kantrip ping` connects and authenticates to Kafka and the
  Registry without touching topics, groups, or schemas. `kantrip doctor`
  checks the profile database, the vault, stored credentials, certificate
  expiry, sessions, and installed clients without changing anything;
  `kantrip doctor --repair` fixes what it safely can.
- **Failure handling.** Credential changes are journaled, so a failure halfway
  through leaves the previous profile working. Database migrations run
  automatically and back up the database first.
- **Output.** Results go to stdout and messages to stderr. Color is optional
  and turns off with `NO_COLOR`, `--no-color`, `TERM=dumb`, or when output
  isn't a terminal.

## Quick start

Install with pipx:

```bash
pipx install kantrip
```

Add a profile, check it, and run some clients with it:

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

Leave out the command to get a subshell for the profile:

```bash
kantrip exec local
kantrip current
kcat -L
kafka-topics --list
exit
```

## Documentation

- [Usage](https://github.com/sauljabin/kantrip/blob/main/USAGE.md): every
  command, the credential vault, and prompt integration.
- [Compatibility](https://github.com/sauljabin/kantrip/blob/main/COMPATIBILITY.md):
  supported clients, versions, and limits.
- [Architecture](https://github.com/sauljabin/kantrip/blob/main/ARCHITECTURE.md)
  and [threat model](https://github.com/sauljabin/kantrip/blob/main/THREAT_MODEL.md):
  how it works and what it does and doesn't protect against.
- [Development](https://github.com/sauljabin/kantrip/blob/main/DEVELOPMENT.md)
  and [manual testing](https://github.com/sauljabin/kantrip/blob/main/MANUAL_TESTING.md):
  working on Kantrip itself.
- [GitHub Releases](https://github.com/sauljabin/kantrip/releases): release
  notes and downloads.

## Questions

Ask in [GitHub Discussions](https://github.com/sauljabin/kantrip/discussions/categories/q-a).

## Security

Please report vulnerabilities privately, as described in the
[security policy](https://github.com/sauljabin/kantrip/blob/main/SECURITY.md).

## Donations

If Kantrip saves you time, you can
[sponsor it on GitHub](https://github.com/sponsors/sauljabin).

## AI Assistance

Kantrip is developed with AI assistance. Every AI-assisted change is reviewed
and tested before it's merged.

## Acknowledgements

<p>
<a href="https://github.com/littlehorse-enterprises/littlehorse"><img alt="Sponsored by LittleHorse" src="https://raw.githubusercontent.com/sauljabin/kantrip/main/images/littlehorse-badge.svg"></a>
<a href="https://github.com/Textualize/rich"><img alt="Built with Rich" src="https://raw.githubusercontent.com/sauljabin/kantrip/main/images/rich-badge.svg"></a>
</p>
