# Agent Instructions

## Engineering Contract

- Write all source code, identifiers, comments, tests, fixtures, CLI text, and
  documentation in English, regardless of the language used in conversations.
- When a durable convention changes, update this file and every affected guide,
  schema, fixture, example, template, and command. Replace obsolete or duplicate
  guidance.
- Do not add empty modules or speculative adapters. Keep cyclomatic complexity
  at or below 10; repository-wide Ruff `C901` runs in `scripts.analyze`, so use
  focused helpers instead of suppressions.
- Importing `kantrip` must have no filesystem, logging, console, or network side
  effects. Classify and redact values before presentation; keep behavior
  independent from Rich.
- Support Linux and macOS on Python 3.10 through 3.14. Keep paths, permissions,
  signals, terminals, and shell documentation portable.
- Keep unimplemented product work in `MVP.md`, not in current feature docs,
  schemas, examples, commands, or implementation comments.

## Profiles and Sessions

- Keep Kantrip scoped to profile storage, secret resolution, temporary client
  configuration, and active-execution supervision. Do not turn it into a
  persistent process manager, global context selector, or Kafka client
  replacement.
- Keep the implemented profile schema in `schemas/`, examples in `examples/`, and
  synthetic data with its tests. Update examples, tests, and migration notes with
  schema changes. Release-bundled schemas are authoritative; filenames and
  configuration omit application versions.
- Treat the environment documented in `USAGE.md` as public API. Add variables
  compatibly; renames or semantic breaks require release and migration guidance.
  Use `KAFKA_*` for application values and reserve `KANTRIP_*` for
  Kantrip-owned profile/session metadata.
- Profile add/remove is validated and atomic; add creates missing configuration,
  never overwrites a profile, and list returns empty when configuration is absent.
- Inject the documented environment only into supervised children; never mutate
  the caller's environment or add a separate JSON schema for environment values.
- Reject `kantrip exec` when `KANTRIP_SESSION_ID` identifies an active parent
  session. The schema and execution accept only `transport: plaintext` with
  `auth.type: none`.
- Supervise one-off commands in their own POSIX process boundary and interactive
  shells through a PTY. Forward SIGINT, SIGTERM, and SIGHUP, preserve child exit
  status, and escalate after five seconds or a repeated signal.
- Keep session roots and directories private and user-owned. Determine liveness
  with `fcntl.flock`, treat validated unlocked sessions as stale after five
  minutes, and inspect no more than 256 entries automatically before `exec`.
- Cleanup must be descriptor-relative, refuse symlinks and unsafe metadata, and
  never remove active sessions or paths outside the validated runtime root.

## Client Adapters and Shells

- Sessions expose private librdkafka properties through `KCAT_CONFIG`; shims
  preserve it and reject `-F`. kcat Avro deserializers receive the plain registry
  URL through `-r`; explicit `-r` is rejected. Never put secrets in arguments.
- Adapters recognize Apache Kafka's `.sh` commands and Confluent's unsuffixed
  equivalents. All receive `--bootstrap-server`; consumers/producers receive
  their config option and admin tools receive `--command-config`, using private
  Java properties.
- Profiles use one optional `registry` object. Its provider defaults to
  `confluent`; canonical generated profiles persist it explicitly. Confluent
  uses the official `schema.registry.url` serializer/deserializer property and native Apicurio
  uses `apicurio.registry.url`. The providers are mutually exclusive and only
  plain `http://` URLs are supported.
- Confluent's Avro, JSON Schema, and Protobuf console producers and consumers
  are unsuffixed. They receive the matching Kafka producer/consumer config file
  and a Confluent-compatible `schema.registry.url`; connection overrides and
  native Apicurio profiles are rejected.
- Kaskade 5 `admin` and `consumer` receive a private INI file through
  `--config-file`; registry deserializers select a second private file with a
  provider-specific `[registry]` section. Kaskade alone supports native
  Apicurio Avro, JSON Schema, and Protobuf decoding. Do not assume a Kaskade
  environment variable until Kaskade implements that contract.
- Bash, Zsh, and Fish sessions preserve startup files and history, neutralize
  adapter shadows, and restore the private temporary shim path. Never install
  persistent aliases.
- Adapters must reject connection arguments that override the selected profile.
- `kantrip ping` uses Confluent Kafka's `AdminClient` to request cluster metadata.
  It checks `/subjects` for Confluent-compatible registries and
  `/search/artifacts` for native Apicurio. All checks use a bounded timeout and
  do not depend on an installed external Kafka CLI.

## Sensitive Values and Output

- Never expose sensitive values in arguments, fixtures, logs, output,
  diagnostics, tracebacks, or snapshots. Examples use conspicuously synthetic
  values and infrastructure.
- Send results to stdout and diagnostics to stderr. Animate progress only on
  colored TTYs; `NO_COLOR`, `TERM=dumb`, `--no-color`, and non-TTY output use
  stable text labels. Accept `--no-color` before or after a subcommand through
  the shared local-option decorator. Styling carries no essential information.
- Keep `ping` failures normalized by default. `ping --verbose` may add only a
  bounded, sanitized message from the underlying exception, never a traceback.

## Tests, Scripts, and Sandbox

- Tests and their fixtures live in `tests` and remain offline. Shared workflow
  helpers belong in `scripts/__init__.py`; other script modules are executable
  workflows.
- Keep Compose, versions, synthetic data, and population tools in `sandbox`.
  Tests may inspect versions and Compose structure, but sandbox and test code
  must not import each other.
- `python -m sandbox` runs the adapter smoke workflow against an active sandbox
  with locally installed clients and optional shells. It is a pre-commit hook,
  not an offline or packaged E2E test.
- `python -m scripts.verify_shell_contract` tests Bash, Zsh, and Fish through PTYs
  and fake clients. Keep assertions in Python and delete safe-metadata event logs
  with their temporary directory.

## Verification

Run these checks after code, environment, schema, tooling, or documentation work:

```text
uv run --locked python -m scripts.analyze
uv run --locked python -m scripts.tests
uv build --clear
uv run --locked python -m scripts.verify_release dist
```

Regenerate `images/banner.svg` with `uv run --locked python -m scripts.banner`
when the banner, console theme, or SVG helper changes.

## Releases and Contributions

- Annotated stable and PEP 440 pre-release tags (`vMAJOR.MINOR.PATCH`, plus
  `aN`, `bN`, or `rcN` suffixes) on `main` are the only release version source;
  Hatchling and hatch-vcs derive package metadata from Git. GitHub Releases are
  the canonical changelog, so do not add maintained changelogs or version-bump
  commits.
- Never hard-code the current release version in documentation, templates,
  examples, or commands. Use `kantrip --version`, `MAJOR.MINOR.PATCH`, or Git
  metadata so releases need no follow-up edits.
- Commits and pull-request titles use Conventional Commits:
  `<type>(<optional scope>): <imperative summary>`. Keep the summary short and do
  not use it as a change list.
- End commit messages and pull-request descriptions with a blank line followed by
  `Assisted-by: <AI model> <version>`, using the actual model and version.
