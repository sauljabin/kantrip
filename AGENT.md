# Agent Instructions

## Engineering Contract

- Treat this file as living operational knowledge. When work establishes or
  changes a durable convention, update this guidance and every affected guide,
  schema, fixture, example, issue template, and command sample. Rewrite obsolete
  or duplicate guidance instead of appending contradictions.
- Do not add empty modules or speculative adapters. Keep cyclomatic complexity
  at or below 10; repository-wide Ruff `C901` runs in `scripts.analyze`, so use
  focused helpers instead of suppressions.
- Importing `kantrip` must not create directories, open files, configure logging,
  construct consoles, or contact Kafka. Classify and redact values before
  presentation, and keep command behavior independent from Rich.
- Support Linux and macOS on Python 3.10 through 3.14. Keep paths, permissions,
  signals, terminals, and shell documentation portable.
- Keep unimplemented product work in `MVP.md`, not in current feature docs,
  schemas, examples, commands, or implementation comments.

## Profiles and Sessions

- The profile schema lives in `schemas/`, examples in `examples/`, and synthetic
  test data inside its owning test module. Keep it limited to implemented
  behavior and synchronize examples, tests, and migration notes when it changes.
  Schema filenames and configuration documents do not duplicate the application
  version; the schema shipped by an application release is authoritative.
- Treat the environment documented in `USAGE.md` as public API. Add variables
  compatibly; renames or semantic breaks require release and migration guidance.
  Use `KAFKA_*` for application values and reserve `KANTRIP_*` for
  Kantrip-owned profile/session metadata.
- `kantrip add` creates missing configuration. Add/remove operations are validated
  and atomic, adding an existing profile never overwrites it, and listing missing
  configuration returns an empty collection.
- Inject the documented environment only into supervised children; never mutate
  the caller's environment or add a separate JSON schema for environment values.
- Reject `kantrip exec` when `KANTRIP_SESSION_ID` identifies an active parent
  session. The schema and execution accept only `transport: plaintext` with
  `auth.type: none`.

## Client Adapters and Shells

- kcat is the first supported client. Sessions expose private generated librdkafka
  properties through `KCAT_CONFIG`; interactive shims preserve this contract and
  reject client attempts to override it with `-F`. Avro deserializers receive
  the profile's plain, unauthenticated Schema Registry URL through kcat's native
  `-r` option; explicit `-r` is rejected. Never place secrets in command
  arguments.
- Apache Kafka Unix archives use `.sh` command names; Confluent Platform ships
  the equivalent commands without `.sh`. Adapters recognize both forms for the
  shared tools. All receive `--bootstrap-server`; consumers/producers receive
  their client config option, and administrative tools receive
  `--command-config`, pointing at the private generated Java properties file.
- Confluent's Avro, JSON Schema, and Protobuf console producers and consumers
  are unsuffixed. They receive the matching Kafka producer/consumer config file
  and the profile's plain Schema Registry URL; connection overrides are rejected.
- Kaskade 5 `admin` and `consumer` receive a private INI file through
  `--config-file`; registry deserializers select a second private file with a
  `[registry]` section. Do not assume a Kaskade environment variable until
  Kaskade implements that contract.
- Interactive sessions support Bash, Zsh, and Fish. They load normal user startup
  files, preserve normal history, neutralize aliases/functions/Fish abbreviations
  for registered adapters, and restore the session shim path. Shims are private
  and temporary; never install persistent aliases.
- Adapters must reject connection arguments that override the selected profile.
- `kantrip ping` uses Confluent Kafka's `AdminClient` to request cluster metadata
  and checks `/subjects` when Schema Registry is configured. Both use a bounded
  timeout and do not depend on an installed external Kafka CLI.

## Sensitive Values and Output

- Never expose sensitive values in arguments, fixtures, logs, output,
  diagnostics, tracebacks, or snapshots. Examples use conspicuously synthetic
  values and infrastructure.
- Send command results to stdout and diagnostics to stderr. Styling respects
  `NO_COLOR`, `TERM=dumb`, `--no-color`, and non-TTY output; use text status labels
  instead of emoji when styling is disabled.

## Tests, Scripts, and Sandbox

- Tests and their fixtures live in `tests` and remain offline. Shared workflow
  helpers belong in `scripts/__init__.py`; other script modules are executable
  workflows.
- The manual environment lives in `sandbox`, including Compose definitions,
  versions, synthetic data, and population tools. Tests may inspect pinned image
  versions and Compose structure, but sandbox code and test fixtures must not
  import each other.
- `python -m sandbox` runs the adapter smoke workflow against an active sandbox
  with locally installed clients and optional shells. It is a pre-commit hook,
  not an offline or packaged E2E test.
- `python -m scripts.verify_shell_contract` verifies Bash, Zsh, and Fish with
  PTYs and generated fake clients. Assertions stay in Python; its temporary event
  logs contain only safe metadata and are removed with their temporary directory.

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
