# Agent Instructions

## Engineering Contract

- Write all code, identifiers, comments, tests, fixtures, CLI text, and docs in
  English, whatever language the conversation uses.
- When a durable convention changes, update this file and every affected guide,
  schema, fixture, example, template, and command; replace obsolete or duplicate
  guidance.
- Add no empty modules or speculative adapters. Keep cyclomatic complexity at or
  below 10 (Ruff `C901` in `scripts.analyze`); never add `# noqa: C901`, extract
  a real responsibility instead.
- Importing `kantrip` has no filesystem, logging, console, or network side
  effects. Keep behavior independent from Rich.
- Support Linux and macOS on Python 3.11 through 3.15 in product, development,
  docs, and CI, with portable paths, permissions, signals, terminals, and shell
  instructions. CI tests only 3.11 and 3.15; never add an intermediate version,
  and single-version jobs, E2E included, use one of the two. Releases also run
  the Compatibility workflow: E2E at each Java floor in `kantrip/adapters.py` and
  on Python 3.11; a unit test keeps its rows on those floors.
- Kantrip is scoped to profile storage, secret resolution, temporary client
  configuration, and supervising the active execution. It is not a process
  manager, global context selector, or Kafka client replacement.
- Keep the CLI resource-oriented: extend `add`, `edit`, `remove`, `list`,
  `describe`, `current`, `doctor`, `ping`, and `exec` rather than adding command
  families. Never expose secret retrieval or round-trip profile export.
- Unimplemented work and future CLI contracts live only in milestone issues,
  never in docs, schemas, examples, commands, or code comments. Document
  behavior when it lands.
- No backward compatibility before v0.1.0; published alphas and betas are not a
  boundary. Replace obsolete contracts directly, without aliases or upgrade
  paths, and reject incompatible state with explicit reset guidance; never
  silently delete user data. CLI, structured-output, and migration guarantees
  begin with v0.1.0.

## Documentation

| Audience | Documents | Responsibility |
| --- | --- | --- |
| End users | `USAGE.md`, `COMPATIBILITY.md` | Installed commands, supported clients, protocols, formats, and actionable limits |
| Developers | `DEVELOPMENT.md`, `ARCHITECTURE.md`, `THREAT_MODEL.md`, `MANUAL_TESTING.md` | Setup and workflows, decisions and rationale, security analysis, human smoke tests |
| AI agents | `AGENT.md`, `RELEASE_CHECKLIST.md` | Engineering rules and release gates; `CLAUDE.md` only imports `AGENT.md` |

- User guides cover only what the installed version runs: no sandbox, `uv run`,
  repository workflows, internal capabilities, or future flags.
- `ARCHITECTURE.md` records decisions, rationale, invariants, and limits; link to
  it instead of repeating it. `THREAT_MODEL.md` matches implemented controls and
  residual risks. `DEVELOPMENT.md` stays a short human guide; agent workflow
  rules and edge cases belong here.
- `MANUAL_TESTING.md` holds short smoke tests only a person can run (real vault,
  terminals, desktops, readability), each with why, commands, and expected
  result, plus a platform table. Anything a program can check belongs in the
  unit and E2E suites.
- `USAGE.md`, `COMPATIBILITY.md`, `ARCHITECTURE.md`, `DEVELOPMENT.md`, and
  `THREAT_MODEL.md` open with a `## Contents` list of their `##` and `###`
  headings, linked by GitHub anchor; update it with every heading change.
- Architecture diagrams in `images/` are hand-written SVGs sharing one `<style>`
  block and marker (a unit test enforces it): copy them from an existing diagram,
  use its role classes (`node`, `core`, `action`, `decision`, `ok`, `warn`,
  `error`), title each diagram in the image, and check light and dark rendering.

## Planning and Delivery

- Milestone issues are the only roadmap. Each has Problem, Decision, Scope,
  Acceptance (at most eight checkable items), and Out of scope; split larger
  ones. Put standing rules here, not in issue bodies.
- One issue maps to one small, reviewable PR with its implementation, tests, and
  affected docs, unless the issue says otherwise.
- Do not poll or watch CI or release pipelines. Ask the maintainer to report the
  result, then inspect that run once.

## Profiles and Storage

- The profile schema lives in `schemas/`, examples in `examples/`, and synthetic
  data with its tests; change all three together. Release-bundled schemas are
  authoritative; profile documents omit application versions, and SQLite has its
  own version.
- The environment documented in `USAGE.md` is the contract. Export only what a
  supported client reads, the `*_CONFIG_FILE` paths of private config files, and
  `KANTRIP_*` session metadata; never connection values or secrets. A client's
  own variable (such as `KCAT_CONFIG`) belongs to its adapter and shim, goes only
  to supervised children, and gets no separate JSON schema.
- Profiles live in the private SQLite database from `KANTRIP_DATABASE`,
  `XDG_DATA_HOME`, then the default data directory: directory `0700`, database
  and sidecars `0600`, WAL, and bounded `BEGIN IMMEDIATE` writes. Reject
  symlinks, unsafe ownership or permissions, corrupt contents, and unsupported
  versions.
- Keep the layers split: orchestration in `profiles.py`, SQLite in
  `profile_storage.py`, pure planning in `profile_auth.py` and
  `profile_documents.py`. Call storage through the module (`storage.name(...)`)
  so tests patch one seam.
- `add` creates a missing database and never overwrites a profile; `edit` keeps
  the immutable ID and changes only explicit fields; `list` is empty without a
  database; Registry removal requires `--remove-registry`. Profile commands may
  migrate an existing database; reads never create one.
- Route every secret-bearing change through `credential_mutations`: journal each
  new immutable reference before its vault write, switch the validated profile
  with an expected revision, and retire only exact superseded references after
  the commit. A failed switch leaves the old profile usable and the staged
  reference recoverable.
- Outcomes are not committed, committed, or unknown. Classify a failed commit
  acknowledgement under the maintenance lock by reopening and matching the exact
  UUID, revision, document, and owned journal records; never infer rollback from
  an exception or retry an unknown outcome. Reload, close, file-hardening, and
  stdout errors after a confirmed commit are committed failures.
- `credential_reconciliation` owns pending deletions: validate each record
  against live references, reconcile only the current operation's records,
  delete only that exact credential, and remove its row only after the deletion
  succeeds.
- The database evolves through one linear chain in `kantrip/migrations.py`: one
  immutable `SqlMigration` subclass per change in the explicit `MigrationChain`
  (no discovery), identified and ordered only by a positive integer `sequence`.
  `schema_migrations` stores name, checksum, applied time, and `applied_by`
  (metadata only; never derive identity from SemVer or documents). Mirror the
  highest sequence in `PRAGMA user_version`, change schema and history in one
  transaction under the maintenance lock after a uniquely timestamped backup
  (never overwrite one), and fail closed on gaps, unknown entries, checksum
  changes, or disagreement.
- A new migration takes the next sequence on `main`; resolve collisions before
  merge. Migrations shipped in v0.1.0 or later are immutable and forward-only.
  Ship no user-run SQL or migration command. Test a fresh database, idempotent
  reopen, upgrades from each supported sequence (built at that sequence, no
  published package needed), rollback, backups, checksum tampering, missing or
  future sequences, and concurrent initialization.
- Never-published formats get no compatibility: reject every non-empty
  history-less database and all former YAML.

## Credential Vault

- Access credentials only through the `SecretStore` protocol, as
  `profile/<profile-uuid>/<credential-uuid>/<field>` under service `kantrip`,
  with fields qualified by owner (`kafka/oauth/client-secret`,
  `registry/oauth/client-secret`). Surface `VaultError` guidance, once per
  vault-level error, instead of a generic credential error.
- Read the vault state before any item access without prompting; never unlock
  just to inspect; reject empty secrets; prompt only when a command needs a
  credential and a controlling terminal exists; cache a refused, cancelled, or
  unanswered prompt for the process.
- macOS: only `~/Library/Keychains/kantrip.keychain-db`. Items go through
  `/usr/bin/security` with values on stdin, split below the `security -i` line
  limit; state and lock policy come from Security.framework with interaction
  disabled. Never read a locked vault, create or unlock it off the terminal, or
  fall back to the login keychain.
- Linux: only the Secret Service collection `kantrip` through `secretstorage`
  (Linux-only, imported on first use; convert D-Bus values explicitly so mypy
  passes with and without its types), in GNOME Keyring or KDE Wallet; reject
  other providers. Find it by the GNOME object path and label or the KDE alias
  and wallet file, never by guessing. Bound every window with the 60-second
  timeout and `Prompt.Dismiss()`; without a terminal, proceed only when the
  vault opens without a window. Restore KDE's `default` alias around create and
  unlock, never move it in `--repair`, and accept only Classic KDE wallets
  (detect GPG from the `kantrip.kwl` header, never by unlocking).
- Unit tests replace the OS layer (`KeychainSystem`, `SecretServiceSystem`) with
  fakes and never touch a real store; tests reaching `load_secret_store` patch
  it.

## Sessions and Supervision

- Reject `kantrip exec` when `KANTRIP_SESSION_ID` names an active parent session.
- Resolve one UUID/revision snapshot, with Kafka and Registry credentials from
  one store, under the maintenance lock before ping or launch; never re-read
  secrets after releasing it.
- Run one-off commands in their own POSIX process group and interactive shells
  through a PTY. Forward SIGINT, SIGTERM, and SIGHUP, keep the child's exit
  status, and escalate after five seconds or a repeated signal.
- Session roots are private and user-owned, with `fcntl.flock` liveness.
  Validated unlocked sessions are stale after five minutes; `exec` inspects at
  most 256 entries. Cleanup is descriptor-relative, refuses symlinks and unsafe
  metadata, and never removes active sessions or paths outside the root.
- `doctor` is read-only. `--repair` is the only maintenance path,
  non-interactive and idempotent under one lock: migrate, validate, reconcile
  exact journal entries, clean stale sessions, diagnose again. Add no
  task-specific migration or cleanup commands or flags.

## Connections, Adapters, and Shells

- PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and mTLS require verified TLS. Validate
  mTLS certificate/key correspondence, build Java JAAS internally, and render
  Java and librdkafka independently. Never accept arbitrary client properties or
  put secrets in arguments.
- Store selected Kafka CA bundles in the profile as validated public PEM and
  materialize them only in the private session, with canonical Java and
  librdkafka properties.
- A profile has at most one `registry` with an explicit `provider`;
  `add --registry-url` persists `confluent` by default. Confluent uses
  `schema.registry.url`, native Apicurio `apicurio.registry.url`. Plain
  `http://` takes no authentication or TLS material; secure Registries use
  verified HTTPS with Basic, fixed-token, mTLS, or OAuth.
- Keep Apicurio's shared `apicurio.registry.tls.certificates` and Confluent
  Java's single `ssl.*` trust for Registry and OAuth. Confluent Python has no
  token-CA property: only for a Kaskade Registry OAuth child, provide a private
  `SSL_CERT_FILE` of platform roots plus the IdP CA. Never change the parent's or
  the system's trust.
- Each client family is one `ClientAdapter` in `adapters.py`, dispatched without
  client-name branches. Native argument grammars live in `adapter_policy.py` and
  shell quoting in `adapter_shims.py`, as explicit per-client functions with one
  policy for direct execution and shims. Reject options that override the
  profile's connection and keep resource and presentation options;
  [Runtime options](COMPATIBILITY.md#runtime-options) lists each client's.
- Version gates: each adapter's `minimum_version` and feature checks are the
  only floors, checked before the operation starts; COMPATIBILITY.md and the
  Compatibility workflow follow them. Java CLIs in one directory share a release,
  read through the `kafka-topics` beside them; kafui's comes from its Go build
  information, because release builds print `dev`.
- Client specifics that the options don't show:
  - kcat reads `KCAT_CONFIG` and gets the plain Registry URL through `-r` for
    Avro.
  - kafui reads kaf's YAML under an empty private session `HOME` and
    `XDG_CONFIG_HOME` (never `/dev/null`, which it treats as fatal) with a `077`
    umask, so session cleanup accepts what it writes.
  - Java tools get `--bootstrap-server` plus their config option
    (`--command-config` for admin tools). The Confluent Avro, JSON Schema, and
    Protobuf consoles also get `--formatter-config` or `--reader-config` and
    `schema.registry.url` as a formatter or reader property (Confluent's
    localhost default otherwise wins), reject Apicurio profiles, and under
    Registry OAuth set the JVM URL allowlist and Java PEM trust for Registry and
    token endpoint.
  - Kaskade `admin`, `consumer`, and `producer` get a private INI through
    `--config-file`, plus a provider `[registry]` section for Registry
    serializers. Kaskade alone handles native Apicurio. Assume no Kaskade
    environment variable until Kaskade implements one.
- Bash, Zsh, and Fish sessions keep startup files and history, neutralize
  adapter shadows, scrub reserved Kafka, Registry, sandbox, and JVM injection
  variables, and restore the owned environment and shim path. Never install
  persistent aliases.
- `ping` polls librdkafka statistics and error callbacks until an addressable
  broker is `UP`, never topic, group, schema, or cluster APIs. Registry ping uses
  `GET /subjects?limit=1` (Confluent-compatible) or `GET /search/versions?limit=1`
  (Apicurio v3), validates empty and non-empty shapes, and for authenticated
  profiles requires the anonymous request to fail with authentication evidence.
  Use one bounded deadline; never claim schema access, write access, or OAuth
  refresh.

## Sensitive Values and Output

- Never expose sensitive values in arguments, fixtures, logs, output,
  diagnostics, tracebacks, or snapshots. Examples use conspicuously synthetic
  values and infrastructure. Classify and redact before presentation.
- Hold every secret as `kantrip.secret_value.Secret` (prompts, key files, auth
  inputs, `SecretReplacement`, resolved connections): `repr()` masks it;
  `str()`, `format()`, and pickling raise. Call `reveal()` only where the value
  leaves the process; `tests/unit/tests_secret_value.py` enforces the module
  allowlist.
- Results go to stdout and diagnostics to stderr. `--color auto|always|never`
  (before or after a subcommand) decides styling: `always` and `never` win;
  `auto` checks a non-empty `NO_COLOR`, then a non-empty `FORCE_COLOR`, then
  `TERM=dumb`, then whether each stream is a TTY. Animate and syntax-highlight
  only colored streams; plain output has stable labels, no ANSI escapes, and
  valid JSON and YAML. Styling carries no essential information. Boolean options
  are one-way switches; never add a `--no-` option.
- A `ping` failure includes a bounded, sanitized cause, never a traceback, and
  reports every attempted service: a Registry failure keeps the Kafka result,
  and a Kafka failure reports the Registry as skipped. `ping --quiet` prints
  nothing and exits only `0` or `1`.

## Tests and Sandbox

- Offline tests live in `tests/unit` (including the Bash, Zsh, and Fish PTY
  contract with fake clients); acceptance lives in `tests/e2e`. Generate PKI in
  memory or test-owned temporary directories; never commit keys. Shared script
  helpers belong in `scripts/__init__.py`; other script modules are workflows.
- Crash and race tests for credential mutations use in-process failpoints and
  `tests/unit/mutation_worker.py` subprocess barriers with real `SIGKILL`, never
  sleeps, and keep these invariants for every credential owner and input path,
  including combined Kafka and Registry mutations:

  | Cut or race | Invariant |
  | --- | --- |
  | Intent committed before the first vault write | Old profile, or no profile after `add`; exact cleanup record |
  | A vault write raises, including the second of several | Every possibly written reference stays journaled |
  | Validation, revision check, or database failure before commit | Previous generation stays active; nothing retired |
  | Lost commit acknowledgement | Inspection yields committed (`3`), unchanged (`1`), or unknown (`4`) |
  | Secret deleted before its journal row | Committed generation survives; repair is idempotent |
  | Concurrent add, edit, or remove | One serial outcome; no lost update or wrong-UUID deletion |
  | Repair versus staging, snapshot versus rotation | No live value deleted; one coherent generation |
  | Live reference in the cleanup journal | Integrity error, no deletion |

  Assertions may inspect references and journal rows, never real values.
- `sandbox/` holds the Kind configuration, manifests, pinned versions, synthetic
  data, and lifecycle tooling, and never imports tests. Bind every host endpoint
  to loopback. Generate credentials at deployment and keep them, client
  properties, and certificates only below ignored `sandbox/.state`
  (`0700`/`0600`); never print them. Source `credentials.env` with `set +a`,
  never `set -a`, and never inside a session.
- The sandbox is one authorizer-enabled Strimzi Kafka cluster on a disposable
  volume with plaintext, verified TLS, SCRAM-SHA-512, mTLS, OAuth, PLAIN over
  TLS, and SCRAM-SHA-256 over TLS listeners, plus an internal TLS/SCRAM-SHA-512
  listener for Registry services and provisioning. `sandbox-admin` is the only
  superuser; model client and Registry ACLs through `KafkaUser`. The idempotent
  in-cluster Job authenticates as `sandbox-admin`, provisions SCRAM-SHA-256 from
  mounted files, and owns only the `kantrip-smoke-` `ANONYMOUS` ACL; no
  credential enters Job arguments, manifests, or logs. Both Apicurio variants use
  KafkaSQL with isolated delete-policy topics and infinite retention; Schema
  Registry runs separate Basic and OAuth processes. Reject a retired topology
  with `down`/`up` guidance rather than deleting it.
- `--suite unit` is the default offline gate. `--suite e2e` is the only Kafka and
  Registry acceptance entry point: it needs a provisioned sandbox, the released
  clients in `tests/e2e/versions.env` (CI installs them exactly, local runs
  accept newer, and a unit test keeps pins at or above each floor), the
  candidate wheel installed separately, and a real, unlocked vault. It never
  creates or removes the caller's sandbox and cleans only its own profiles,
  topics, schemas, and artifacts. Preconditions tell missing tools or
  infrastructure apart from product failures; test version checks with stable
  releases. `--suite e2e-floors` runs the plaintext clients and the Java floor
  checks for Compatibility rows below a floor.
- E2E exercises real operations, not help output; below a Java floor it requires
  Kantrip's refusal before launch, as Kantrip's own gates decide. It reads TUIs
  through parsed terminal state, starts Zsh with `-d` so runner startup files
  can't prompt, serializes the shared OAuth identities, and proves refresh and
  post-revocation failure in one long-lived client per Registry OAuth
  implementation (Confluent Java, Kaskade Confluent Python, Kaskade Apicurio). A
  Registry ping proves only initial token acquisition.
- `--suite vault` is the sandbox-free vault acceptance of the current platform.
  It locks, unlocks, and hides the real vault, so it runs only on a disposable
  one: a macOS vault whose password is in `KANTRIP_E2E_VAULT_PASSWORD`, or the
  empty-password `kantrip.keyring` Linux CI pre-creates (runners can't show
  GNOME Keyring's window). Tests that lock or reload that keyring store
  single-line secrets, because GNOME's plain-text format corrupts multi-line
  ones.

## Verification

Run the applicable checks after code, environment, schema, tooling, or
documentation work:

```text
uv run --locked python -m scripts.analyze
uv run --locked python -m scripts.tests --suite unit
# For E2E-impacting changes, with the sandbox and released clients provisioned:
uv run --locked python -m scripts.tests --suite e2e
uv build --clear
uv run --locked python -m scripts.verify_release dist
```

- Format with `uv run --locked python -m scripts.styles` (black and ruff).
- E2E is needed for runtime, schema, dependency, packaging, sandbox, E2E
  tooling, and workflow changes, and for mixed, renamed, deleted-runtime, or
  unknown paths; prose-only docs, images and site assets, templates, license,
  and unit-test-only changes skip it. `scripts/tests.py` owns this policy for the
  pre-commit hook (`--staged-wheel`, standard library only until E2E is
  selected), CI selection (`--ci-event`), and result checks
  (`--verify-e2e-result`); [When E2E runs](DEVELOPMENT.md#when-e2e-runs)
  describes the triggers.
- Force E2E when docs change executable behavior: `KANTRIP_E2E_FORCE=1` locally,
  a fresh `run-e2e` label event on a PR, or a manual workflow run. A PR opened
  with the label fires racing `opened` and `labeled` runs, so `opened` reads the
  current labels; `main` compares the whole push range and selects E2E when the
  base is missing or a path is unknown.
- The CI client cache is keyed by OS, architecture, pinned versions, and setup,
  and saves only checksum-verified downloads, never sandbox state.

## Website and Images

- `site/` is plain HTML, CSS, and vanilla JavaScript: no build step, web fonts,
  analytics, or cookies. Its only third-party request is the GitHub release
  lookup in `site/site.js` (CSP `connect-src https://api.github.com`), falling
  back to the Releases link. Links and assets stay relative for `/kantrip/`;
  JavaScript and CSS stay under 30 KB. The `*-badge.svg` files are
  byte-identical copies of the README badges in `images/` (a unit test enforces
  it).
- The site follows the system color scheme until a visitor picks one with the
  header toggle; `site/theme.js` loads synchronously and stores only an explicit
  choice that differs from the system's in `localStorage`.
- `site/demo.json` holds the demo, and `site/index.html` embeds the same
  transcript between the `demo-transcript` markers (`render` regenerates it).
  `scripts.website check` validates transcript, links, assets, anchors, origins,
  sizes, the absence of sandbox values, and every demo command against
  `kantrip COMMAND --help`, so a CLI rename updates the demo in the same PR.
- `scripts.website capture` records the demo against the sandbox in a real
  terminal when its options or output style may change. It types the password
  from `sandbox/.state` (a capture containing it fails), maps colors to Arcana
  style names, drops localhost IPv6 noise, and removes its profile and topics
  even on failure; from a worktree it uses the main checkout's state unless
  `--state-dir` is given. The `site-demo` hook runs it for changes to
  `kantrip/`, `site/demo.json`, `scripts/website.py`, `scripts/__init__.py`, or
  dependency files; skip it with `SKIP=site-demo` only when the demo can't
  change.
- Before merging visual site changes, check 320 px width, keyboard navigation,
  JavaScript disabled, light and dark schemes, and reduced motion. The `Pages`
  workflow validates every PR read-only and deploys only from `main` (source:
  GitHub Actions; the `github-pages` environment allows only `main`).
- `scripts.banner` regenerates `images/banner.svg` when the banner, console
  theme, or SVG helper changes. Draw frames and the `-*` wand
  (`scripts.banner.wand_svg`) in the banner, social preview, and site as shapes,
  never glyphs: Linux and Android fallback fonts break the monospace grid.
- `scripts.screenshots` renders the README's 2x2 grid from real command output
  in a temporary data directory with fixed profile IDs and one shared height; its
  profiles have no secrets, so it never opens the vault. The `screenshots` hook
  regenerates it when `kantrip/` changes.
- After editing `images/social-preview.svg`, render `site/social-preview.png`
  (1280×640) with headless Chrome or Chromium (`--headless=new
  --hide-scrollbars --force-device-scale-factor=1 --window-size=1280,640
  --screenshot=…`), check its font, and ask the maintainer to upload it under
  Settings > General > Social preview, which has no API.

## Releases and Contributions

- Pin third-party GitHub Actions to explicit published version tags verified
  against their upstream releases; never commit hashes or floating majors.
- Annotated stable and PEP 440 pre-release tags (`vMAJOR.MINOR.PATCH` with
  optional `aN`, `bN`, or `rcN`) on `main` are the only version source, through
  hatch-vcs. GitHub Releases are the changelog: no maintained changelog,
  version-bump commits, or hard-coded current version in docs, templates,
  examples, or commands. Migration sequences are independent of releases.
- Commits and PR titles use Conventional Commits,
  `<type>(<optional scope>): <imperative summary>`, with a short summary that is
  not a change list.
- End commit messages and PR descriptions with a blank line and
  `Assisted-by: <AI model> <version>`, using the actual model. It is the only
  attribution and the last line: no `Co-Authored-By` trailers or "Generated
  with" footers, even when a tool's defaults add them.
