# Agent Instructions

## Public Interfaces

- Do not create empty modules or speculative adapters for unimplemented
  behavior.
- Keep the public profile schema versioned. Breaking schema changes require a
  new version, migration guidance, fixtures, examples, and tests.
- Treat the application environment documented in `USAGE.md` as public behavior.
  Add variables compatibly; a rename or semantic break requires release and
  migration guidance.

## Code Quality

- Keep cyclomatic complexity at or below 10. Repository-wide Ruff `C901` is part
  of `scripts.analyze`; prefer focused named helpers over lint suppressions.
- Importing `kantrip` must not create directories, open files, configure
  logging, construct terminal consoles, or contact credential stores or Kafka.
- Keep command behavior independent from Rich. Values must be classified and
  redacted before they reach presentation code.

## Living Knowledge and Documentation

- Treat this file as the project's living operational knowledge. Update it when
  work establishes a durable convention, architectural decision, workflow, or
  constraint that future agents need to follow.
- Review existing guidance while updating it. Remove or rewrite obsolete,
  redundant, or contradicted knowledge rather than only appending sections.
- When behavior changes, update every affected guide, schema, fixture, example,
  issue template, and command sample in the same change.

## Supported Platforms

- Kantrip must work consistently on Linux and macOS with Python 3.10 through
  3.14. Keep paths, permissions, signal handling, terminal behavior, and
  shell-facing documentation portable across both platforms.
- Keep interfaces portable enough for later Windows support without claiming
  Windows compatibility in the MVP.

## Client Support

- kcat is the first supported external CLI. Sessions provide its generated
  librdkafka properties through `KCAT_CONFIG`; do not create aliases or place
  configuration values in command arguments.
- `kafka-topics` and `kafka-topics.sh` inject `--bootstrap-server` and a generated
  Java `--command-config`. Interactive subshells expose session-owned executable
  shims for installed variants; never create persistent aliases.
- Until credential-backed sessions are implemented, execution accepts only
  profiles with `transport: plaintext` and `auth.type: none`.

## Secrets and Diagnostics

- Long-lived secrets belong only in an approved operating-system credential
  store. Never add a plaintext or locally encrypted file fallback.
- Never put passwords, tokens, private keys, secret-bearing JAAS strings, or
  Registry credentials in command arguments, repository fixtures, logs, normal
  output, diagnostics, tracebacks, or error snapshots.
- Fixtures and examples use conspicuously synthetic values and infrastructure.
  Redact secret references as well as resolved secret values in profile output.
- Normal command output goes to stdout and diagnostics go to stderr. Rich
  styling must respect `NO_COLOR`, `TERM=dumb`, `--no-color`, and non-TTY output.

## Environment and Schemas

- The machine-readable profile schema lives in `schemas/`; examples live in
  `examples/`; test-owned fixtures live under their owning suite. Keep them
  synchronized.
- `kantrip add` creates configuration when necessary, and profile additions and
  removals are validated and atomic. Adding an existing profile must not overwrite
  it; listing absent configuration behaves as an empty profile collection.
- Application-facing values use `KAFKA_*` and `SCHEMA_REGISTRY_*`. Reserve
  `KANTRIP_*` for profile and session metadata owned by Kantrip.
- The documented environment is injected only into supervised child processes.
  Never modify the caller's parent environment or add a separate JSON schema for
  environment variables.

## Tests, Scripts, and Sandbox

- Unit tests and their fixtures live in `tests/unit`. End-to-end tests and any
  E2E-only fixtures live in `tests/e2e` and provision their own disposable
  services. Keep unit tests offline.
- The manual environment lives entirely in `sandbox`, including Compose files,
  versions, generated synthetic material, and population tools. Tests may read
  pinned image versions and assert Compose structure, but must not import sandbox
  executable code or use sandbox data as test fixtures. Sandbox code must not
  import test fixtures.
- Keep the sandbox's Apicurio Registry on KafkaSQL storage. Its journal and
  snapshot topics must be created with three replicas before the registry starts.
- Reusable repository-script helpers belong in `scripts/__init__.py`; individual
  script modules remain focused on executable workflows.

## Verification

Run these checks for code, environment, schema, tooling, or documentation changes:

```text
uv run --locked python -m scripts.analyze
uv run --locked python -m scripts.tests
uv run --locked python -m scripts.tests --e2e
uv build --clear
uv run --locked python -m scripts.verify_release dist
```

Regenerate `images/banner.svg` with `uv run --locked python -m scripts.banner`
when the banner, console theme, or SVG helper changes.

## Releases and Versions

- Annotated tags matching `vMAJOR.MINOR.PATCH` on `main` are the only release
  version source. Hatchling and hatch-vcs derive package metadata from Git.
- GitHub Releases are the canonical changelog. Do not add a maintained changelog
  or version-bump commit.
- Never hard-code Kantrip's current release version in documentation, issue
  templates, examples, or release commands. Refer to `kantrip --version`, use a
  `MAJOR.MINOR.PATCH` placeholder, or derive the version from Git metadata so a
  release does not require follow-up file edits.

## Commits

Use the [Conventional Commits](https://www.conventionalcommits.org/) format for
every commit message:

```text
<type>(<optional scope>): <description>
```

The description must be a short, imperative summary of the feature or fix. Do
not use it as a list of changes.

End every commit message with an `Assisted-by` trailer, separated from the body
by a blank line:

```text
Assisted-by: <AI model> <version>
```

Use the actual AI model and version that generated the commit.

## Pull Requests

Pull request titles and descriptions must follow the same rules as commit
messages: use the Conventional Commits format, provide a short imperative
summary of the feature or fix rather than a list of changes, and end with the
`Assisted-by: <AI model> <version>` trailer.
