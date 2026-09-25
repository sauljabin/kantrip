# Security Policy

## Supported versions

Kantrip has not published its first stable release. Until then, security fixes
are applied to the `main` development branch on a best-effort basis. Update this
table when the first stable version is released; afterward, support only the
latest stable release unless the project explicitly announces otherwise.

| Version | Supported |
| --- | --- |
| `main` pre-release development | Best effort |
| Stable releases | None published yet |

## Scope of current support

Use [Compatibility](COMPATIBILITY.md) to distinguish executable authentication
from file-input and Registry support. Remaining security work and
first-release verification are tracked in the
[v0.1 milestone](https://github.com/sauljabin/kantrip/milestone/1).
Kafka ping observes a completed broker connection without resource APIs; its
transport/authentication proof does not establish application authorization.
Registry ping reads only the selected provider's bounded list/search endpoint:
`GET /subjects?limit=1` for Confluent-compatible APIs or
`GET /search/versions?limit=1` for native Apicurio v3. A successful authenticated
probe also requires anonymous denial on that same route. This establishes the
configured authentication gate and permission for that exact read query, not
read/write access to every schema or a producer/consumer's full authorization.

## Reporting a vulnerability

Do not report suspected vulnerabilities in a public issue, discussion, pull
request, log, terminal transcript, or screenshot.

Use [GitHub private vulnerability reporting](https://github.com/sauljabin/kantrip/security/advisories/new)
whenever possible. If that form is unavailable, email
`sauljabin@gmail.com` with the subject `[Kantrip security]` and include only the
minimum sensitive detail needed to establish contact.

Include the following when it is safe to do so:

- The affected Kantrip version or commit.
- Operating system, Python version, installation method, Kafka distribution,
  and affected command.
- A concise description of the vulnerability and its likely impact.
- Reproduction steps or a minimal proof of concept using test credentials and
  test infrastructure.
- Whether generated files, environment variables, command arguments, logs, or
  private infrastructure details may have been exposed.
- Any known mitigations and your preferred name for advisory credit.

Never send production records, credentials from another system, or private
broker details. Replace them with synthetic values.

Security-sensitive examples include profile-value leakage through arguments or
diagnostics, unsafe temporary-file cleanup, symlink or path traversal, command
or shim injection, profile-confusion bugs, and dependency vulnerabilities with
a demonstrated impact on Kantrip.

## Handling and disclosure

The maintainer will assess the report, request missing information when needed,
and keep confirmed vulnerabilities private while a fix is prepared. Resolution
time depends on severity, complexity, and maintainer availability; reporters
will receive material status updates through the private report.

Please allow time for investigation and remediation before publishing details.
The maintainer and reporter should coordinate disclosure, release, advisory,
and credit timing. Confirmed vulnerabilities may be published as GitHub
repository security advisories after a fixed release is available.

Questions, troubleshooting, hardening suggestions without a concrete security
impact, and ordinary bugs belong in GitHub Discussions or the public issue
tracker after all sensitive information has been removed.
