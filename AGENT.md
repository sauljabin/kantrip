# Agent Instructions

## Engineering Contract

- Write all source code, identifiers, comments, tests, fixtures, CLI text, and
  documentation in English, regardless of the language used in conversations.
- When a durable convention changes, update this file and every affected guide,
  schema, fixture, example, template, and command. Replace obsolete or duplicate
  guidance.
- Do not add empty modules or speculative adapters. Keep cyclomatic complexity
  at or below 10 for new and materially changed functions; repository-wide Ruff
  `C901` runs in `scripts.analyze`. Five existing functions carry narrow
  suppressions; remove those through focused refactors rather than adding new
  suppressions.
- Importing `kantrip` must have no filesystem, logging, console, or network side
  effects. Classify and redact values before presentation; keep behavior
  independent from Rich.
- Support Linux and macOS on Python 3.10 through 3.14. Keep paths, permissions,
  signals, terminals, and shell documentation portable.
- Keep unimplemented product work and future CLI contracts in GitHub issues
  under the `v0.1`/`v0.2` milestones, not in current feature docs, schemas,
  examples, commands, or implementation comments.
- Preserve a resource-oriented CLI. Extend the existing `add`, `edit`,
  `remove`, `list`, `describe`, `current`, `doctor`, `ping`, and `exec` verbs
  instead of adding one-off command families for input formats or credential
  operations. Never expose secret retrieval or a round-trip profile export.
  Update current user documentation only when the corresponding behavior lands.

## Documentation Ownership

| Audience | Documents | Responsibility |
| --- | --- | --- |
| End users | `USAGE.md`, `COMPATIBILITY.md` | Installed `kantrip` commands, supported clients/protocols/formats, actionable limits and environment behavior |
| Developers | `DEVELOPMENT.md`, `ARCHITECTURE.md`, `THREAT_MODEL.md`, `MANUAL_TESTING.md` | Contributor workflows, technical decisions and rationale, security analysis, reproducible human QA |
| AI agents | `AGENT.md`, `RELEASE_CHECKLIST.md` | Durable engineering instructions and release execution gates |

- Keep sandbox instructions, `uv run`, repository workflows, internal-only
  capabilities, and future CLI contracts out of end-user guides. Document only
  behavior users can exercise with the installed version.
- Make `ARCHITECTURE.md` the canonical record of technical decisions, rationale,
  invariants, failure boundaries, and limits. Keep actionable conventions here
  and link to architecture instead of duplicating its full explanations. Align
  `THREAT_MODEL.md` with implemented controls and explicit residual risks.
- Keep each manual scenario's setup, commands, and expected results in
  `MANUAL_TESTING.md`; keep release orchestration in `RELEASE_CHECKLIST.md`.

## Planning and Delivery

- Milestone issues are the only roadmap; do not add a roadmap file. Each issue
  uses the sections Problem, Decision, Scope, Acceptance (at most eight
  checkable items), and Out of scope. Split an issue that needs more.
- One issue maps to one PR unless the issue says otherwise. Keep the
  implementation, its tests, and the affected user documentation in that PR.
  Keep PRs small enough for a human to review in one sitting.
- Do not poll or watch CI or release pipelines. Ask the maintainer to report
  success or failure, then inspect that run once.
- Put standing rules in this file once; do not repeat them in issue bodies.

## Profiles and Sessions

- Keep Kantrip scoped to profile storage, secret resolution, temporary client
  configuration, and active-execution supervision. Do not turn it into a
  persistent process manager, global context selector, or Kafka client
  replacement.
- Keep the implemented profile schema in `schemas/`, examples in `examples/`, and
  synthetic data with its tests. Update examples, tests, and documentation with
  schema changes. Release-bundled profile schemas are authoritative; profile
  documents omit application versions. The SQLite schema uses a separate
  internal version.
- Treat the environment documented in `USAGE.md` as the current contract.
  No backward compatibility is promised before v0.1.0. Alpha and beta packages
  published to PyPI are pre-releases, not a compatibility boundary. Until
  v0.1.0, replace obsolete CLI, environment, profile, storage, and runtime
  contracts directly without aliases, upgrade paths, or development-state
  compatibility. Reject incompatible state with explicit reset guidance; never
  silently delete user data. CLI, structured-output, and database-migration
  guarantees begin with v0.1.0.
- Use `KAFKA_*` for application values and reserve `KANTRIP_*` for
  Kantrip-owned profile/session metadata.
- Store profiles in the private SQLite database resolved through
  `KANTRIP_DATABASE`, `XDG_DATA_HOME`, then the user's default data directory.
  Keep its directory mode `0700`, database and sidecar modes `0600`, WAL enabled,
  and writes bounded by `BEGIN IMMEDIATE` transactions. Reject symlinks, unsafe
  ownership or permissions, corrupt contents, and unsupported schema versions.
- Keep the profile layer split as mapped in `ARCHITECTURE.md`: orchestration in
  `profiles.py`, SQLite in `profile_storage.py`, pure planning in `profile_auth.py`
  and `profile_documents.py`. Call storage functions through the module
  (`storage.name(...)`) so tests can patch one seam.
- Evolve the database through one bundled linear chain in
  `kantrip/migrations.py`. Keep one immutable `SqlMigration` subclass per change
  in the explicit `MigrationChain` registry; do not use filesystem or import
  discovery. Use a positive integer `sequence` as each migration's only identity
  and order; store its immutable name, checksum,
  applied timestamp, and applying Kantrip version in `schema_migrations`. Never
  derive migration identity from product SemVer or profile document fields.
- Treat migrations shipped in v0.1.0 or later as immutable and forward-only;
  before v0.1.0 the chain may be rewritten. Mirror the highest
  applied sequence in `PRAGMA user_version`, update schema and history in one
  transaction under the maintenance lock, create a uniquely timestamped backup
  before pending work, and fail closed on gaps, unknown entries, checksum
  changes, or version disagreement. Never overwrite an earlier migration backup.
- Do not add compatibility or baseline adoption for database formats that were
  never published in a release. Before the first release, reject every non-empty
  history-less database and all former YAML. The first published database starts
  the supported migration boundary.
- Profile add/edit/remove is validated and transactional; add creates the
  missing database and never overwrites a profile, edit preserves its immutable
  ID and changes only explicit fields, and list returns empty when the database
  is absent. Registry removal requires `--remove-registry`. Profile-dependent
  commands may migrate an existing supported database, but reads never create a
  missing repository. Normal `doctor` inspection never creates or modifies
  filesystem state.
- Access credentials only through the narrow `SecretStore` protocol. Approve
  only macOS Keychain on macOS and Secret Service-compatible keyring backends on
  Linux; reject null, plaintext, encrypted-file, chained, and unknown backends.
  Use service `kantrip` and canonical immutable
  `profile/<profile-uuid>/<credential-uuid>/<field>` keys. Fully qualify fields
  by owner, including `kafka/oauth/client-secret` and
  `registry/oauth/client-secret`.
- Route every secret-bearing profile change through `credential_mutations`.
  Journal each new immutable reference before its credential-store write,
  switch the validated profile with an expected revision, and retire only exact
  superseded references after the database commit. A failed switch leaves the
  old profile usable and the staged reference recoverable by reconciliation.
- Track mutation outcomes explicitly as not committed, committed, or unknown.
  A failed commit acknowledgement must be classified under the maintenance lock
  by reopening and matching the exact profile UUID/revision/document and owned
  journal records; never infer rollback from an exception or retry an unknown
  outcome automatically. Treat reload, close, file-hardening, and stdout errors
  after a confirmed commit as committed failures.
- Keep exact pending credential deletions in `credential_reconciliation`.
  Validate every record and live reference, reconcile only records owned by the
  current operation during its completion, delete only that exact credential,
  and remove its journal row only after deletion succeeds. Normal doctor reports
  all pending work; `doctor --repair` retries it under the maintenance lock.
- Inject the documented environment only into supervised children; never mutate
  the caller's environment or add a separate JSON schema for environment values.
- Reject `kantrip exec` when `KANTRIP_SESSION_ID` identifies an active parent
  session. The schema accepts PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and mTLS only
  over verified TLS. Resolve their exact profile-owned references through
  `SecretStore`, validate mTLS certificate/key correspondence, construct Java
  JAAS internally, and keep Java and librdkafka rendering independent. Resolve
  one UUID/revision snapshot with both Kafka and Registry credentials resolved
  through one store under the maintenance lock before ping or launch. Do not
  re-read Registry secrets after releasing that lock.
- Copy user-selected Kafka CA bundles into the profile as validated public PEM
  material. TLS always verifies certificates and hostnames. Materialize a
  custom CA only inside the private session and use canonical Java and
  librdkafka properties; never accept arbitrary client-property passthroughs.
  Version-gate Java custom-CA sessions at Apache Kafka 2.7 or Confluent Platform
  6.1 and fail before the Kafka operation when support cannot be verified.
- Supervise one-off commands in their own POSIX process boundary and interactive
  shells through a PTY. Forward SIGINT, SIGTERM, and SIGHUP, preserve child exit
  status, and escalate after five seconds or a repeated signal.
- Keep session roots and directories private and user-owned. Determine liveness
  with `fcntl.flock`, treat validated unlocked sessions as stale after five
  minutes, and inspect no more than 256 entries automatically before `exec`.
- Cleanup must be descriptor-relative, refuse symlinks and unsafe metadata, and
  never remove active sessions or paths outside the validated runtime root.
- Keep `doctor` read-only unless the user supplies `--repair`. Unified repair is
  non-interactive and idempotent: migrate, validate, reconcile exact journal
  entries, clean validated stale sessions, then diagnose again under one lock.
  Do not add public task-specific migration or cleanup commands or flags.

## Client Adapters and Shells

- Sessions expose private librdkafka properties through `KCAT_CONFIG`; shims
  preserve it and reject `-F`. kcat Avro deserializers receive the plain registry
  URL through `-r`; explicit `-r` is rejected. Never put secrets in arguments.
- Adapters recognize Apache Kafka's `.sh` commands and Confluent's unsuffixed
  equivalents. All receive `--bootstrap-server`; consumers/producers receive
  their config option and admin tools receive `--command-config`, using private
  Java properties.
- Profiles use one optional `registry` object whose stored `provider` is always
  explicit. `kantrip add --registry-url` selects and persists `confluent` when
  `--registry-provider` is omitted. Confluent uses the official
  `schema.registry.url` serializer/deserializer property and native Apicurio
  uses `apicurio.registry.url`. The providers are mutually exclusive. Plain
  `http://` requires no authentication or TLS material; secure profiles use
  verified HTTPS with typed Basic, fixed-token, mTLS, or OAuth variants.
- Confluent's Avro, JSON Schema, and Protobuf console producers and consumers
  are unsuffixed. They receive the matching Kafka producer/consumer config file
  and a Confluent-compatible `schema.registry.url`; connection overrides and
  native Apicurio profiles are rejected.
- Kaskade `admin` and `consumer` receive a private INI file through
  `--config-file`; registry deserializers select a second private file with a
  provider-specific `[registry]` section. Kaskade alone supports native
  Apicurio Avro, JSON Schema, and Protobuf decoding. Allow only `group.id` and
  `broker.address.family` as explicit `--kafka` runtime properties; reject
  connection and authentication overrides. Do not assume a Kaskade
  environment variable until Kaskade implements that contract.
- Preserve Apicurio's official shared `apicurio.registry.tls.certificates`
  trust contract for Registry and OAuth. Kaskade 5.0.1 separates the HTTP/TLS
  contexts so Registry client identity does not reach the IdP, and implements
  the official scope plus cleanup contract. Require Kaskade 5.0.1 or newer only
  when native Apicurio OAuth scopes are configured; preserve compatible older
  modes that do not need that property.
- Confluent Java uses one official `ssl.*` trust configuration for Registry and
  OAuth. Confluent Python has no token-CA property; only for a Kaskade Registry
  OAuth child, provide a private `SSL_CERT_FILE` containing platform default
  roots plus the profile's IdP CA. Never mutate the parent or system trust.
- Bash, Zsh, and Fish sessions preserve startup files and history, neutralize
  adapter shadows, scrub reserved Kafka/Registry/sandbox and JVM injection
  variables, and restore the owned environment and private shim path. Never
  install persistent aliases.
- Adapters must reject connection arguments that override the selected profile.
  Keep one argument policy shared by direct execution and shell shims. Apply
  explicit per-client rules to native connection, configuration-file, and
  runtime-property options while preserving resource and presentation options.
- `kantrip ping` polls Confluent Kafka's `AdminClient` statistics and error
  callbacks for a configured/learned addressable broker reaching `UP`; never use
  topic/group/schema/cluster resource APIs for Kafka success. Registry ping uses
  `GET /subjects?limit=1` for Confluent-compatible endpoints and
  `GET /search/versions?limit=1` for native Apicurio v3. It validates empty or
  non-empty provider shapes and, for authenticated profiles, requires the same
  anonymous request to fail with explicit authentication evidence. Apply one
  bounded deadline and never claim schema-specific read, write access, or
  long-lived OAuth refresh.

## Sensitive Values and Output

- Never expose sensitive values in arguments, fixtures, logs, output,
  diagnostics, tracebacks, or snapshots. Examples use conspicuously synthetic
  values and infrastructure.
- Hold every secret in memory as `kantrip.secret_value.Secret`: prompt and
  key-file input, auth inputs, `SecretReplacement`, and resolved Kafka, OAuth,
  and Registry connections. `repr()` masks it; `str()`, `format()`, and
  pickling raise. Call `reveal()` only where the value leaves the process
  (client renderers, validators, ping requests, session key files, the
  credential-store write); `tests/unit/tests_secret_value.py` enforces the
  module allowlist.
- Send results to stdout and diagnostics to stderr. Animate progress only on
  colored TTYs; `NO_COLOR`, `TERM=dumb`, `--no-color`, and non-TTY output use
  stable text labels. Accept `--no-color` before or after a subcommand through
  the shared local-option decorator. Styling carries no essential information.
- Syntax-highlight JSON and YAML only on colored TTYs. `--no-color`, `NO_COLOR`,
  `TERM=dumb`, and non-TTY streams must contain no ANSI escapes and remain valid
  structured documents.
- Include a bounded, sanitized underlying cause in normal `ping` failures,
  never a traceback. Report every attempted service: a Registry failure after
  Kafka success keeps the Kafka result, and a Kafka failure reports the
  Registry as skipped, never as passed or failed. `ping --quiet` emits nothing
  and communicates only through status `0` or `1`.

## Tests, Scripts, and Sandbox

- Offline tests live in `tests/unit`; infrastructure acceptance lives in
  `tests/e2e`. Generate PKI in memory or in test-owned temporary directories;
  never commit certificate/key fixtures. Shared workflow helpers belong in
  `scripts/__init__.py`; other script modules are executable workflows.
- Keep Kind configuration, Kubernetes manifests, pinned versions, synthetic
  bootstrap data, and lifecycle tooling in `sandbox`. Bind every host endpoint
  to loopback. Offline tests may inspect manifests and import side-effect-free
  sandbox helpers; sandbox code must never import tests.
- Generate sandbox credentials at deployment time. Store exported credentials,
  client properties, and certificate material only below ignored
  `sandbox/.state`, with directory mode `0700` and file mode `0600`. Never print
  credential values from sandbox lifecycle commands.
- `python -m sandbox up` reconciles the Kind laboratory; `status`, `credentials`,
  and `down` inspect or remove it. Keep one persistent, authorizer-enabled
  Strimzi Kafka cluster with plaintext, verified TLS, SCRAM-SHA-512, mTLS,
  OAuth, PLAIN over TLS, and SCRAM-SHA-256 over TLS listeners. Reserve the
  internal TLS/SCRAM-SHA-512 listener for Registry services and authenticated
  provisioning. `plaintext` means no authentication and no encryption.
- Keep `sandbox-admin` as the only Kafka superuser. Model client and Registry
  ACLs through `KafkaUser`; the idempotent in-cluster Job authenticates as that
  administrator, provisions SCRAM-SHA-256 from private mounted files, and owns
  only the prefix-limited `ANONYMOUS` smoke ACL. Never place a credential in Job
  arguments, manifests, or logs.
- The Kafka cluster uses a disposable persistent volume so data and
  SCRAM-SHA-256 credentials survive broker pod restarts.
  Both Apicurio variants use KafkaSQL with isolated journal and snapshot topics,
  delete cleanup policy, and infinite retention so their data survives Apicurio
  pod restarts. Destroying the Kind cluster intentionally removes sandbox data.
- `python -m scripts.tests --suite unit` is the default offline gate. It includes
  the Bash, Zsh, and Fish PTY contract with generated fake clients.
- `python -m scripts.tests --suite e2e` is the sole external acceptance entry
  point. It requires an explicitly provisioned sandbox, the pinned released
  clients in `tests/e2e/versions.env`, the candidate wheel installed separately,
  and a real approved native credential backend. It must never create or remove
  the caller's sandbox. It owns exact temporary profiles, topics, schemas, and
  artifacts and cleans only those resources.
- E2E preconditions must distinguish missing tooling or infrastructure from
  product assertion failures. Exercise real operations, not help/version output;
  parse TUI behavior through terminal state, not raw redraw bytes. Serialize the
  shared OAuth identities and prove refresh and post-revocation failure in the
  same long-lived client processes.
- Keep reproducible, high-value exploratory scenarios in `MANUAL_TESTING.md`.
  Every scenario needs explicit setup, actions, and expected results.
  `DEVELOPMENT.md` contains environment and contributor workflows, not manual
  test cases. Manual checks complement rather than replace unit and E2E tests.

## Verification

Run analysis, unit tests, and build verification after code, environment,
schema, tooling, or documentation work. Run E2E for runtime, schema,
dependency, packaging, sandbox, E2E tooling, or workflow changes; skip it for
prose-only docs, images/site assets, templates, license, and unit-test-only
changes. Mixed, renamed/deleted runtime, or unknown paths require E2E. The
shared staged/`main` selection policy lives in `scripts/tests.py` and
is documented in [Development](DEVELOPMENT.md#sandbox-services-and-e2e-workflow).
Force E2E explicitly when docs change executable behavior. PRs run it only on
a newly applied `run-e2e` label or explicit workflow dispatch; releases always
run it against the exact wheel, regardless of changed paths.

Run the applicable checks:

```text
uv run --locked python -m scripts.analyze
uv run --locked python -m scripts.tests --suite unit
# For E2E-impacting changes, with sandbox and released clients provisioned:
uv run --locked python -m scripts.tests --suite e2e
uv build --clear
uv run --locked python -m scripts.verify_release dist
```

Regenerate `images/banner.svg` with `uv run --locked python -m scripts.banner`
when the banner, console theme, or SVG helper changes.

## Releases and Contributions

- Use explicit published version tags for third-party GitHub Actions in every
  workflow. Verify each tag against its upstream release before changing it;
  avoid commit hashes and floating major tags.
- Annotated stable and PEP 440 pre-release tags (`vMAJOR.MINOR.PATCH`, plus
  `aN`, `bN`, or `rcN` suffixes) on `main` are the only release version source;
  Hatchling and hatch-vcs derive package metadata from Git. GitHub Releases are
  the canonical changelog, so do not add maintained changelogs or version-bump
  commits.
- Never hard-code the current release version in documentation, templates,
  examples, or commands. Use `kantrip --version`, `MAJOR.MINOR.PATCH`, or Git
  metadata so releases need no follow-up edits.
- Keep database migration sequences independent from releases. A release may
  contain zero, one, or several migrations; record its resolved version only as
  `applied_by` metadata.
- Commits and pull-request titles use Conventional Commits:
  `<type>(<optional scope>): <imperative summary>`. Keep the summary short and do
  not use it as a change list.
- End commit messages and pull-request descriptions with a blank line followed by
  `Assisted-by: <AI model> <version>`, using the actual model and version.
