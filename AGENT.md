# Agent Instructions

## Engineering Contract

- Write all source code, identifiers, comments, tests, fixtures, CLI text, and
  documentation in English, regardless of the language used in conversations.
- When a durable convention changes, update this file and every affected guide,
  schema, fixture, example, template, and command. Replace obsolete or duplicate
  guidance.
- Do not add empty modules or speculative adapters. Keep cyclomatic complexity
  at or below 10 in every function (Ruff `C901` in `scripts.analyze`); never add
  `# noqa: C901`, extract a real responsibility instead.
- Importing `kantrip` must have no filesystem, logging, console, or network side
  effects. Keep behavior independent from Rich.
- Support Linux and macOS on Python 3.10 through 3.14 in product, development
  environment, documentation, and CI. Keep paths, permissions, signals,
  terminals, and shell instructions portable.
- Keep Kantrip scoped to profile storage, secret resolution, temporary client
  configuration, and supervising the active execution. It is not a process
  manager, global context selector, or Kafka client replacement.
- Keep the CLI resource-oriented: extend `add`, `edit`, `remove`, `list`,
  `describe`, `current`, `doctor`, `ping`, and `exec` instead of adding command
  families for input formats or credential operations. Never expose secret
  retrieval or round-trip profile export.
- Keep unimplemented work and future CLI contracts in GitHub issues under the
  `v0.1` milestone, never in current docs, schemas, examples, commands,
  or code comments. Document behavior only when it lands.
- No backward compatibility before v0.1.0; published alphas and betas are not a
  boundary. Replace obsolete CLI, environment, profile, storage, and runtime
  contracts directly, without aliases or upgrade paths, and reject incompatible
  state with explicit reset guidance; never silently delete user data. CLI,
  structured-output, and migration guarantees begin with v0.1.0.

## Documentation

| Audience | Documents | Responsibility |
| --- | --- | --- |
| End users | `USAGE.md`, `COMPATIBILITY.md` | Installed commands, supported clients, protocols, and formats, actionable limits |
| Developers | `DEVELOPMENT.md`, `ARCHITECTURE.md`, `THREAT_MODEL.md`, `MANUAL_TESTING.md` | Quick setup and everyday workflows, design decisions and rationale, security analysis, human smoke tests |
| AI agents | `AGENT.md`, `RELEASE_CHECKLIST.md` | Durable engineering rules and release gates; `CLAUDE.md` only imports `AGENT.md` |

- User guides describe only what users can run with the installed version: no
  sandbox, `uv run`, repository workflows, internal capabilities, or future
  flags.
- `ARCHITECTURE.md` is the canonical, human-oriented record of decisions,
  rationale, invariants, and limits. Keep actionable rules here and link to
  architecture instead of repeating its explanations. Keep `THREAT_MODEL.md`
  aligned with implemented controls and residual risks.
- `DEVELOPMENT.md` stays a short setup and workflow guide for humans; put
  workflow rules and edge cases for agents here.
- `MANUAL_TESTING.md` holds short smoke tests of what only a person can check
  (the real vault, terminals, desktops, readability). Each test states why it
  matters, the commands to run, and what to expect; a platform table says which
  sections run on macOS, Linux, or each Linux desktop. Leave anything a program
  can check to the unit and E2E suites.
- `USAGE.md`, `COMPATIBILITY.md`, `ARCHITECTURE.md`, `DEVELOPMENT.md`, and
  `THREAT_MODEL.md` open with a `## Contents` list of their `##` and `###`
  headings, linked by GitHub anchor; update it with every heading change.
  Checklists, `MANUAL_TESTING.md`, and this file have none.
- Architecture diagrams in `images/` are hand-written SVGs that share one
  `<style>` block and marker (a unit test enforces it); copy it from an existing
  diagram, use its role classes (`node`, `core`, `action`, `decision`, `ok`,
  `warn`, `error`), give each diagram an in-image title, and check light and
  dark rendering.

## Planning and Delivery

- Milestone issues are the only roadmap; do not add a roadmap file. Each issue
  has the sections Problem, Decision, Scope, Acceptance (at most eight
  checkable items), and Out of scope; split one that needs more.
- One issue maps to one PR unless it says otherwise, with its implementation,
  tests, and affected docs. Keep PRs small enough to review in one sitting.
- Do not poll or watch CI or release pipelines. Ask the maintainer to report
  the result, then inspect that run once.
- Put standing rules here once; do not repeat them in issue bodies.

## Profiles and Storage

- The profile schema lives in `schemas/`, examples in `examples/`, and synthetic
  data with its tests; update all three with schema changes. Release-bundled
  schemas are authoritative; profile documents omit application versions, and
  SQLite has its own version.
- The environment documented in `USAGE.md` is the current contract. Export
  only what a supported client reads, the `*_CONFIG_FILE` paths of the private
  config files, and `KANTRIP_*` session metadata; never connection values or
  secrets. A client's own variable (such as `KCAT_CONFIG`) belongs to its
  adapter and shim, not the shared environment. Inject it only into supervised
  children, never into the caller, and add no separate JSON schema for it.
- Store profiles in the private SQLite database resolved through
  `KANTRIP_DATABASE`, `XDG_DATA_HOME`, then the default data directory:
  directory `0700`, database and sidecars `0600`, WAL, and writes in bounded
  `BEGIN IMMEDIATE` transactions. Reject symlinks, unsafe ownership or
  permissions, corrupt contents, and unsupported versions.
- Keep the profile layer split: orchestration in `profiles.py`, SQLite in
  `profile_storage.py`, pure planning in `profile_auth.py` and
  `profile_documents.py`. Call storage through the module (`storage.name(...)`)
  so tests patch one seam.
- `add` creates a missing database and never overwrites a profile; `edit` keeps
  the immutable ID and changes only explicit fields; `list` returns empty when
  the database is absent; Registry removal requires `--remove-registry`.
  Profile commands may migrate an existing database, but reads never create one.
- Route every secret-bearing change through `credential_mutations`: journal
  each new immutable reference before its vault write, switch the validated
  profile with an expected revision, and retire only exact superseded
  references after the commit. A failed switch leaves the old profile usable
  and the staged reference recoverable.
- Track outcomes as not committed, committed, or unknown. Classify a failed
  commit acknowledgement under the maintenance lock by reopening and matching
  the exact UUID, revision, document, and owned journal records; never infer
  rollback from an exception or retry an unknown outcome. Reload, close,
  file-hardening, and stdout errors after a confirmed commit are committed
  failures.
- Keep pending deletions in `credential_reconciliation`. Validate every record
  against live references, reconcile only the current operation's records,
  delete only that exact credential, and remove its row only after the deletion
  succeeds.
- Evolve the database through one linear chain in `kantrip/migrations.py`: one
  immutable `SqlMigration` subclass per change in the explicit `MigrationChain`
  (no file or import discovery), with a positive integer `sequence` as its only
  identity and order. `schema_migrations` stores name, checksum, applied time,
  and the applying Kantrip version (`applied_by` metadata only; never derive
  identity from SemVer or document fields). Mirror the highest sequence in
  `PRAGMA user_version`, update schema and history in one transaction under
  the maintenance lock, write a uniquely timestamped backup first (never
  overwrite an earlier one), and fail closed on gaps, unknown entries, checksum
  changes, or disagreement.
- When adding a migration, take the next sequence on `main` and resolve branch
  collisions before merge. Migrations shipped in v0.1.0 or later are immutable
  and forward-only; before then the chain may be rewritten. Keep migrations in
  the package, with no user-run SQL or migration command. Test a fresh
  database, idempotent reopen, upgrades from each supported sequence (build the
  fixture at that sequence; no published package needed), rollback, backups,
  checksum tampering, missing or future sequences, and concurrent
  initialization.
- Add no compatibility for database formats never published: before the first
  release, reject every non-empty history-less database and all former YAML.

## Credential Vault

- Access credentials only through the `SecretStore` protocol, as
  `profile/<profile-uuid>/<credential-uuid>/<field>` under service `kantrip`.
  Fully qualify fields by owner, including `kafka/oauth/client-secret` and
  `registry/oauth/client-secret`. Surface `VaultError` guidance instead of a
  generic credential error, and report a vault-level error once.
- Read the vault state before any item access without prompting; never unlock a
  vault just to inspect it; reject empty secrets; open a prompt only when a
  command needs a credential and a controlling terminal exists; cache a
  refused, cancelled, or unanswered prompt for the process.
- macOS: use only `~/Library/Keychains/kantrip.keychain-db`. Read and write items
  only through `/usr/bin/security` with values on stdin, split them below the
  `security -i` line limit, read state and lock policy only through
  Security.framework with interaction disabled, never read a locked vault,
  create and unlock only on the terminal, and never fall back to the login
  keychain.
- Linux: use only the Secret Service collection `kantrip` through
  `secretstorage` (a Linux-only dependency imported on first use; keep D-Bus
  return values explicitly converted so mypy passes with and without its
  types), in GNOME Keyring or KDE Wallet; reject other providers. Find it by
  the GNOME object path and label or the KDE alias and wallet file, never by
  guessing. Bound every window with the 60-second timeout and
  `Prompt.Dismiss()`; without a terminal, proceed only when the vault opens
  without a window. Restore KDE's `default` alias around create and unlock, and
  never move it in `--repair`. Accept only Classic KDE wallets: detect GPG from
  the `kantrip.kwl` header, never by unlocking.
- Unit tests replace the OS layer (`KeychainSystem`, `SecretServiceSystem`) with
  fakes and never touch a real store; tests that reach `load_secret_store`
  patch it.

## Sessions and Supervision

- Reject `kantrip exec` when `KANTRIP_SESSION_ID` names an active parent session.
- Resolve one UUID/revision snapshot, with Kafka and Registry credentials from
  one store, under the maintenance lock before ping or launch, and never re-read
  secrets after releasing it.
- Supervise one-off commands in their own POSIX process group and interactive
  shells through a PTY. Forward SIGINT, SIGTERM, and SIGHUP, preserve the child's
  exit status, and escalate after five seconds or a repeated signal.
- Keep session roots private and user-owned. Liveness is `fcntl.flock`;
  validated unlocked sessions are stale after five minutes; `exec` inspects at
  most 256 entries. Cleanup is descriptor-relative, refuses symlinks and unsafe
  metadata, and never removes active sessions or paths outside the root.
- `doctor` is read-only and never creates or modifies files. `--repair` is the
  only maintenance path, non-interactive and idempotent: migrate, validate,
  reconcile exact journal entries, clean stale sessions, then diagnose again,
  under one lock. Add no task-specific migration or cleanup commands or flags.

## Connections, Adapters, and Shells

- Accept PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and mTLS only over verified TLS.
  Validate mTLS certificate/key correspondence, build Java JAAS internally, and
  keep Java and librdkafka rendering independent. Never accept arbitrary client
  properties or put secrets in arguments.
- Copy selected Kafka CA bundles into the profile as validated public PEM, and
  materialize them only inside the private session with canonical Java and
  librdkafka properties. Gate Java custom-CA sessions at Apache Kafka 2.7 or
  Confluent Platform 6.1 and fail before the operation when that can't be
  verified.
- A profile has at most one `registry` with an explicit `provider`;
  `add --registry-url` persists `confluent` when none is given. Confluent uses
  `schema.registry.url`, native Apicurio `apicurio.registry.url`. Plain `http://`
  takes no authentication or TLS material; secure Registries use verified HTTPS
  with Basic, fixed-token, mTLS, or OAuth.
- Keep Apicurio's shared `apicurio.registry.tls.certificates` and Confluent Java's
  single `ssl.*` trust for Registry and OAuth. Confluent Python has no token-CA
  property: only for a Kaskade Registry OAuth child, provide a private
  `SSL_CERT_FILE` with the platform roots plus the IdP CA. Never change the
  parent's or the system's trust. Kaskade's minimum version is 5.0.1.
- Describe each client family with one `ClientAdapter` in `adapters.py` and
  dispatch through it, without client-name branches. Keep native argument
  grammars in `adapter_policy.py` and shell quoting in `adapter_shims.py` as
  explicit per-client functions, with one argument policy for direct execution
  and shims. Reject options that override the profile's connection; keep
  resource and presentation options.
- Per client: kcat reads `KCAT_CONFIG` (shims keep it and reject `-F`) and gets
  the plain Registry URL through `-r` for Avro (explicit `-r` rejected). Java
  tools (Apache `.sh` and Confluent unsuffixed) get `--bootstrap-server` plus
  their config option, or `--command-config` for admin tools. Confluent Avro,
  JSON Schema, and Protobuf consoles get the matching config file for both the
  Kafka command and the formatter or reader, plus `schema.registry.url` as a
  formatter or reader property to suppress Confluent's localhost default, and
  reject Apicurio profiles; their Registry OAuth sessions set the JVM URL
  allowlist and Java PEM trust for Registry and token endpoint. Kaskade `admin`
  and `consumer` get a private INI through `--config-file` plus a Registry file
  with a provider `[registry]` section, and allow only `group.id` and
  `broker.address.family` through `--kafka`. Kaskade alone decodes native
  Apicurio Avro, JSON Schema, and Protobuf. Don't assume a Kaskade environment
  variable until Kaskade implements one.
- Bash, Zsh, and Fish sessions keep startup files and history, neutralize
  adapter shadows, scrub reserved Kafka, Registry, sandbox, and JVM injection
  variables, and restore the owned environment and shim path. Never install
  persistent aliases.
- `ping` polls librdkafka statistics and error callbacks until an addressable
  broker is `UP`, never topic, group, schema, or cluster APIs. Registry ping
  uses `GET /subjects?limit=1` (Confluent-compatible) or
  `GET /search/versions?limit=1` (Apicurio v3), validates empty and non-empty
  shapes, and for authenticated profiles requires the anonymous request to fail
  with authentication evidence. Use one bounded deadline; never claim schema
  access, write access, or OAuth refresh.

## Sensitive Values and Output

- Never expose sensitive values in arguments, fixtures, logs, output,
  diagnostics, tracebacks, or snapshots. Examples use conspicuously synthetic
  values and infrastructure. Classify and redact before presentation.
- Hold every secret as `kantrip.secret_value.Secret` (prompts, key files, auth
  inputs, `SecretReplacement`, resolved connections). `repr()` masks it; `str()`,
  `format()`, and pickling raise. Call `reveal()` only where the value leaves
  the process; `tests/unit/tests_secret_value.py` enforces the module
  allowlist.
- Results go to stdout and diagnostics to stderr. Animate and syntax-highlight
  only on colored TTYs; `NO_COLOR`, `TERM=dumb`, `--no-color` (accepted before or
  after a subcommand), and non-TTY output use stable labels, contain no ANSI
  escapes, and keep JSON and YAML valid. Styling carries no essential
  information.
- A `ping` failure includes a bounded, sanitized cause, never a traceback, and
  reports every attempted service: a Registry failure keeps the Kafka result,
  and a Kafka failure reports the Registry as skipped. `ping --quiet` prints
  nothing and uses only status `0` or `1`.

## Tests and Sandbox

- Offline tests live in `tests/unit` (including the Bash, Zsh, and Fish PTY
  contract with fake clients); acceptance lives in `tests/e2e`. Generate PKI in
  memory or in test-owned temporary directories; never commit keys. Shared
  script helpers belong in `scripts/__init__.py`; other script modules are
  workflows.
- Crash and race tests for credential mutations use in-process failpoints and
  `tests/unit/mutation_worker.py` subprocess barriers with real `SIGKILL`, never
  sleeps, and must keep these invariants for every credential owner and input
  path, including combined Kafka and Registry mutations:

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
- `sandbox/` holds the Kind configuration, manifests, pinned versions,
  synthetic data, and lifecycle tooling; sandbox code never imports tests.
  Bind every host endpoint to loopback. Generate credentials at deployment and
  keep them, client properties, and certificates only below ignored
  `sandbox/.state` (`0700`/`0600`); never print them. Sourcing
  `credentials.env` uses `set +a`, never `set -a`, and never inside a session.
- Keep one authorizer-enabled Strimzi Kafka cluster on a disposable persistent
  volume with plaintext (no authentication, no encryption), verified TLS,
  SCRAM-SHA-512, mTLS, OAuth, PLAIN over TLS, and SCRAM-SHA-256 over TLS, plus
  an internal TLS/SCRAM-SHA-512 listener reserved for Registry services and
  provisioning. `sandbox-admin` is the only superuser; model client and
  Registry ACLs through `KafkaUser`. The idempotent in-cluster Job
  authenticates as `sandbox-admin`, provisions SCRAM-SHA-256 from mounted files,
  and owns only the `kantrip-smoke-` `ANONYMOUS` ACL; no credential ever enters
  Job arguments, manifests, or logs. Both Apicurio variants use KafkaSQL with
  isolated delete-policy topics and infinite retention; Schema Registry uses
  separate Basic and OAuth processes because they are different server
  authentication paths. Reject a retired laboratory topology with `down`/`up`
  guidance rather than deleting it.
- `--suite unit` is the default offline gate. `--suite e2e` is the only Kafka
  and Registry acceptance entry point: it needs a provisioned sandbox, the
  released clients in `tests/e2e/versions.env` (CI installs them exactly; local
  runs accept newer, and a unit test keeps pins at or above each minimum), the
  candidate wheel installed separately, and a real, unlocked vault. It never
  creates or removes the caller's sandbox and cleans only its own profiles,
  topics, schemas, and artifacts. Preconditions tell missing tools or
  infrastructure apart from product failures; test version checks with stable
  releases, not only development strings.
- E2E exercises real operations, not help output; reads TUIs through parsed
  terminal state; starts Zsh with `-d` so runner startup files can't prompt
  (Kantrip's session `.zshrc` still runs); serializes the shared OAuth
  identities; and proves refresh
  and post-revocation failure in the same long-lived client, once per Registry
  OAuth implementation (Confluent Java, Kaskade Confluent Python, Kaskade
  Apicurio). A Registry ping proves only initial token acquisition.
- `--suite vault` is the sandbox-free vault acceptance of the current platform.
  It locks, unlocks, and hides the real vault, so it runs only on a disposable
  vault: a macOS vault whose password is in `KANTRIP_E2E_VAULT_PASSWORD`, or the
  empty-password `kantrip.keyring` Linux CI pre-creates because runners cannot
  show GNOME Keyring's window. Tests that lock or reload that keyring store
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
- Run E2E for runtime, schema, dependency, packaging, sandbox, E2E tooling, or
  workflow changes; skip it for prose-only docs, images and site assets,
  templates, license, and unit-test-only changes. Mixed, renamed or deleted
  runtime, and unknown paths require it. `scripts/tests.py` owns this policy for
  the staged pre-commit hook (`--staged-wheel`, standard library only until E2E
  is selected), CI selection (`--ci-event`), and result checks
  (`--verify-e2e-result`).
- Force E2E when docs change executable behavior: `KANTRIP_E2E_FORCE=1` locally,
  a fresh `run-e2e` label event on a PR, or workflow dispatch on `main`. The
  label is one-shot (pushes don't repeat E2E; remove and reapply it). A PR
  opened with the label fires `opened` and `labeled` runs that cancel each
  other, so the `opened` run reads the current labels. `main` compares the whole
  push range and selects E2E when the base is missing or a path is unknown.
  Releases always run E2E against the exact wheel their build job verified.
- The CI client cache is keyed by OS, architecture, pinned versions, and setup,
  and saves only checksum-verified downloads, never sandbox state.

## Website and Images

- `site/` is plain HTML, CSS, and vanilla JavaScript with no build step, web
  fonts, analytics, or cookies. Its only third-party request is the GitHub API
  release lookup in `site/site.js` (CSP `connect-src https://api.github.com`),
  with a Releases link as the fallback. Links and assets stay relative for the
  `/kantrip/` path; JavaScript and CSS stay under 30 KB. The site's
  `*-badge.svg` files are byte-identical copies of the README badges in
  `images/` (a unit test enforces it); recopy them when a badge changes.
- `site/demo.json` holds the demo, and `site/index.html` embeds the same
  transcript statically between the `demo-transcript` markers (`render`
  regenerates it). `scripts.website check` validates transcript, links, assets,
  anchors, origins, sizes, the absence of sandbox values, and every demo
  command against `kantrip COMMAND --help`, so a CLI rename updates the demo in
  the same PR.
- Run `scripts.website capture` when a change can affect the demo's options or
  output style. It runs the generic commands against the sandbox in a real
  terminal, types the password from `sandbox/.state` (never printed; a capture
  containing it fails), maps colors back to Arcana style names, drops
  localhost IPv6 connection noise, and removes its profile and topics even on
  failure. From a worktree it uses the main checkout's state unless
  `--state-dir` is given. The `site-demo` pre-commit hook runs it for changes to
  `kantrip/`, `site/demo.json`, `scripts/website.py`, `scripts/__init__.py`, or
  dependency files; skip it only with `SKIP=site-demo` when the demo can't change.
- Before merging visual site changes, check 320 px width, keyboard navigation,
  JavaScript disabled, light and dark schemes, and reduced motion. The `Pages`
  workflow validates every PR read-only and deploys only from `main` (Pages
  source: GitHub Actions; the `github-pages` environment allows only `main`).
- Regenerate `images/banner.svg` with `scripts.banner` when the banner, console
  theme, or SVG helper changes. Draw the frame and the `-*` wand in the banner,
  the social preview, and the site as shapes, never glyphs: fallback fonts on
  Linux and Android break the monospace grid. `scripts.banner.wand_svg` holds
  the wand geometry.
- After editing `images/social-preview.svg`, render `site/social-preview.png`
  (1280×640) with headless Chrome or Chromium
  (`--headless=new --hide-scrollbars --force-device-scale-factor=1
  --window-size=1280,640 --screenshot=…`), check its font, and ask the
  maintainer to upload it under Settings > General > Social preview, which has
  no API.

## Releases and Contributions

- Pin third-party GitHub Actions to explicit published version tags, verified
  against their upstream releases; never commit hashes or floating majors.
- Annotated stable and PEP 440 pre-release tags (`vMAJOR.MINOR.PATCH` with
  optional `aN`, `bN`, or `rcN`) on `main` are the only version source;
  hatch-vcs derives metadata from Git. GitHub Releases are the changelog: no
  maintained changelog or version-bump commits, and never hard-code the current
  version in docs, templates, examples, or commands.
- Migration sequences are independent from releases; a release may carry any
  number of them.
- Commits and PR titles use Conventional Commits,
  `<type>(<optional scope>): <imperative summary>`, with a short summary that is
  not a change list.
- End commit messages and PR descriptions with a blank line and
  `Assisted-by: <AI model> <version>`, using the actual model. It is the only
  attribution and the last line: no `Co-Authored-By` trailers or "Generated
  with" footers, even when a tool's defaults add them.
