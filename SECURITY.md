# Security Policy

## Supported versions

Security fixes go to the `main` branch on a best-effort basis.

## What the current checks prove

[Compatibility](COMPATIBILITY.md) lists which authentication methods each
client can actually use, and which inputs and Registry setups are supported.
Remaining security work and the checks for the first release are tracked in
the [v0.1 milestone](https://github.com/sauljabin/kantrip/milestone/1).

`kantrip ping` is not an authorization check. The Kafka check waits for a
completed broker connection and calls no topic, group, or cluster APIs, so it
shows that the transport and authentication work, not what the identity is
allowed to do. The Registry check sends one bounded read query to the selected
provider: `GET /subjects?limit=1` for Confluent-compatible APIs, or
`GET /search/versions?limit=1` for native Apicurio v3. For an authenticated
profile it also requires the same query to be refused without credentials.
That proves the Registry checks the configured credentials and allows that one
query. It doesn't prove read or write access to every schema, or everything a
producer or consumer needs.

## Reporting a vulnerability

Don't report suspected vulnerabilities in a public issue, discussion, pull
request, log, terminal transcript, or screenshot.

Use [GitHub private vulnerability reporting](https://github.com/sauljabin/kantrip/security/advisories/new)
if you can. If that form isn't available, email `sauljabin@gmail.com` with the
subject `[Kantrip security]`, and include only as much sensitive detail as it
takes to get in touch.

When it's safe to include them, these help:

- The affected Kantrip version or commit.
- Operating system, Python version, installation method, Kafka distribution,
  and the command involved.
- A short description of the vulnerability and its likely impact.
- Steps to reproduce it, or a minimal proof of concept that uses test
  credentials and test infrastructure.
- Whether generated files, environment variables, command arguments, logs, or
  private infrastructure details may have been exposed.
- Any mitigations you know of, and the name you'd like credited in the
  advisory.

Never send production records, credentials from another system, or private
broker details. Replace them with made-up values.

Examples of security bugs: profile values leaking through arguments or
diagnostics, unsafe cleanup of temporary files, symlink or path traversal,
command or shim injection, one profile's connection being used for another,
and dependency vulnerabilities with a demonstrated impact on Kantrip.

## Handling and disclosure

The maintainer will assess the report, ask for anything missing, and keep a
confirmed vulnerability private while a fix is prepared. How long that takes
depends on severity, complexity, and the maintainer's availability; you'll get
status updates through the private report when something changes.

Please allow time for investigation and a fix before publishing details. The
maintainer and the reporter should agree on the timing of disclosure, the
release, the advisory, and credit. Confirmed vulnerabilities may be published
as GitHub security advisories once a fixed release is available.

Questions, troubleshooting, hardening ideas without a concrete security impact,
and ordinary bugs go in GitHub Discussions or the issue tracker, with anything
sensitive removed.
