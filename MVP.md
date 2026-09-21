# First-release MVP roadmap

This is the implementation handoff for the remaining first-release work, audited
after completion of PR 1. Read PRs 2–7 in order. Each numbered PR is one delivery
unit; its subsections are tasks within that PR, not additional PRs. All commands
and contracts below are **targets**, unless explicitly identified as current.

Current behavior belongs in the other guides. Remove a roadmap item only when
its implementation, automated tests, applicable integration evidence, schema,
and affected documentation land together. Move its runnable manual checks to
`MANUAL_TESTING.md`; do not discard release QA when removing completed work.

## Scope and implementation rules

- This is a new, unreleased product. Replace obsolete CLI, environment, document,
  and runtime contracts directly: no deprecated aliases, dual readers, legacy
  migrations, or compatibility with previous development commits. Reject
  incompatible old state with reset guidance; never silently delete a user's
  database, runtime, or credentials. Published releases will establish the
  future migration boundary. Preserve the existing migration integrity engine.
- Reuse the implemented SQLite profile ID/revision, approved OS `SecretStore`,
  credential journal, CAS mutation helpers, PLAIN/SCRAM/mTLS schema and
  renderers, TLS validation, labels, `describe`, structured output, maintenance,
  process supervision, and shell shims. These are dependencies, not new work.
- Keep exactly `add`, `edit`, `remove`, `list`, `describe`, `doctor`, `ping`,
  `exec`, and `current`. No import/export/secret/clone command groups, ambient
  profile selection, daemon, secret-reading interface, or arbitrary property map.
- Every network command selects `PROFILE`. Store only connection settings:
  endpoints, trust, identity, credential acquisition, and required auth routing.
  Topic, serialization, schema selection, retry, cache, and application behavior
  belong to the selected client.
- Secrets enter through no-echo prompts, bounded private-key files, or the two
  explicit `add` input sources. Never add literal password, token, secret,
  private-key, or JAAS value flags. Do not put secrets in argv, persistent
  profile JSON, output, diagnostics, fixtures, or logs.
- Authenticated Kafka, Registry, and token endpoints require verified TLS,
  including localhost. No insecure test bypass. Plaintext remains supported
  only with no authentication. Keep Kafka, Registry, and token-endpoint trust
  and credentials independent.
- Capability checks must name the unsupported client/mechanism and fail before
  the requested operation. Never assume that supporting librdkafka or a Java
  library means a CLI exposes every library setting.
- Write implementation and documentation in English. Use the existing module
  boundaries below; introduce a module only with a concrete responsibility.

## Delivery sequence

| PR | Outcome | Includes | Depends on |
| --- | --- | --- | --- |
| 2 | Independent secure Registry and OAuth connections | Registry TLS/basic/token/mTLS, Kafka and Registry OAuth, authenticated provider probes, capability enforcement | Completed authenticated Kafka foundation |
| 3 | Complete profile creation from external files | Java/librdkafka/Confluent properties, Strimzi Secrets, all supported auth types, matching sandbox exports | PRs 1–2 |
| 4 | Additional native clients | `kcl` and `kafkactl`, direct commands and three shells | PRs 1–3 |
| 5 | Readable, verified CLI implementation | Focused refactoring and design patterns, remaining integration fixtures/matrix, CLI documentation reconciliation and manual QA | PRs 1–4 |
| 6 | Public product landing page ([issue #8](https://github.com/sauljabin/kantrip/issues/8)) | Static terminal-themed site, accessible content, PR artifact validation, main-only GitHub Pages deployment | PR 5 |
| 7 | Durable documentation and final release handoff | Preserve technical decisions, align security and agent instructions, remove this roadmap and obsolete references, verify final artifacts | PRs 1–6 |

Keep schema, lifecycle, rendering, diagnostics, and adapter changes together for
one mechanism. Do not split a PR merely by file type, authentication mechanism,
or documentation. PR 5 closes cross-cutting code and integration gaps; PR 6
keeps site delivery independently reviewable; PR 7 closes documentation and
packaging after every feature is implemented. Earlier PRs must already pass
their own acceptance tests and update current-feature documentation.

Implementation navigation (extend these tests; do not duplicate whole suites):

| PR | Main existing code and test seams |
| --- | --- |
| 2 | `registry.py`, schema, `secret_store.py`, shared lifecycle/resolution/probe modules; `tests/tests_registry.py`, `tests/tests_schemas.py`, `tests/tests_credential_mutations.py`, `tests/tests_redaction.py`, client integration fixtures |
| 3 | New focused input parser/normalizer modules feeding `profiles.py`; parser unit tests, CLI tests, `sandbox/__main__.py`, `tests/tests_sandbox.py` |
| 4 | Adapter/rendering/capability seams from PRs 1–2, `shells.py`, `doctor.py`; session, shell, smoke, and PTY contract tests |
| 5 | Lifecycle/resolution/adapter/rendering seams from PRs 1–4; platform integration evidence, `scripts/verify_release.py`, `scripts/smoke.py`, docs, examples, packaging/workflow checks |
| 6 | New `site/` static sources and focused site build/validation script; `.github/workflows/`, existing artwork, `README.md`, `DEVELOPMENT.md` |
| 7 | All root guides and their anchors, schemas/examples, site links, `pyproject.toml` sdist includes, `scripts/verify_release.py` required files, templates/workflows |

## PR 2 — Secure Registry connections and native OAuth

### 2.1 Registry model, lifecycle, and renderers

**Gap:** `registry.py` only accepts provider plus an HTTP URL. Registry security,
OAuth, and Registry mTLS reference fields are absent from the usable schema.

**Architecture:** retain the explicit provider and its existing endpoint key
(`schema.registry.url` or `apicurio.registry.url`). Add typed `tls` and `auth`
objects parallel to Kafka: public CA/certificate PEM, owned private-key/password
references, and a discriminated auth variant. Basic uses username/passwordRef;
token uses tokenRef; mTLS uses certificate/private-key references; OAuth uses
its fields below. HTTPS may use `auth: none`; HTTP requires `auth: none` and no
TLS material. Each connection selects one auth variant; combining mTLS with
basic/OAuth is outside this MVP. No credentials, query, or fragment in URLs.

Extend the existing mutation planner to stage Kafka and Registry replacements
in one recoverable operation. Keep omitted Registry fields during edits;
`--remove-registry` retires all and only Registry references. Changing provider
requires a complete compatible candidate; do not translate native Apicurio into
a Confluent endpoint automatically.

**CLI delta, on both `add` and `edit`:**

```text
--registry-auth none|basic|token|mtls|oauth
--registry-username TEXT
--registry-ca-file PATH
--registry-client-certificate-file PATH
--registry-client-key-file PATH
```

On `edit`, also add `--registry-default-trust` (conflicts with CA file).
`--remove-registry` conflicts with every Registry setter/replacement.
Credentials remain prompted. Extend `--replace-secret` and schema/reference
validation with `registry/password`, `registry/token`,
`registry/tls/private-key`, and `registry/tls/private-key-password`.

Create provider-specific resolved Registry models and adapter renderers. A
property valid in the Java Registry client is not automatically a kcat,
Kaskade, or Python Registry property. Confluent console credentials must reach
a private producer/consumer config file using the client-required Registry
prefixes; never pass secret-bearing `--property` arguments. kcat Avro support
must be checked against its actual linked Registry library. If a client only
supports credentials inside its Registry URL/argv, reject that mode. Kaskade
uses its private INI `[registry]` mapping, with independently tested provider
fields. Keep the plaintext URL-only path working. Reject native Apicurio
Registry decoding for Confluent consoles and kcat.

Use the native connection properties in the normalization appendix below.
Fixed bearer tokens apply only to Confluent-compatible Registry. Require basic
and OAuth paths for both providers through ping and at least one verified
client; support Registry mTLS only where the client exposes a proven safe PEM
mapping. Document unsupported cells explicitly, without insecure fallbacks.

### 2.2 OAuth lifecycle for Kafka and Registry

**Architecture:** add a shared typed OAuth configuration containing HTTPS token
URL, client ID, ordered unique scopes, clientSecretRef, and optional public
CA material for the token endpoint. Keep each owner's OAuth configuration
independent. Token secrets use `kafka/oauth/client-secret` and
`registry/oauth/client-secret`. Scope replacement is atomic; omission on edit
keeps scopes. Client credentials is the only grant. Do not persist access or
refresh tokens. No custom audience, executable callback, discovery provider,
client assertion, or Strimzi client-side OAuth module.

**CLI delta, on both `add` and `edit`:**

```text
--auth oauth
--oauth-token-url URL
--oauth-client-id TEXT
--oauth-scope TEXT                       (repeatable)
--oauth-ca-file PATH
--registry-oauth-token-url URL
--registry-oauth-client-id TEXT
--registry-oauth-scope TEXT              (repeatable)
--registry-oauth-ca-file PATH
--registry-oauth-logical-cluster TEXT
--registry-oauth-identity-pool-id TEXT
```

The last two are Confluent routing metadata, accepted only for Registry OAuth.
On `edit` also add `--clear-oauth-scopes`, `--oauth-default-trust`,
`--clear-registry-oauth-scopes`, `--registry-oauth-default-trust`,
`--clear-registry-oauth-logical-cluster`, and
`--clear-registry-oauth-identity-pool-id`. Clear/default flags conflict with their
setters. Reject any option inapplicable to the selected final auth/provider.
Include both OAuth secret fields in prompted replacement and the editor.

For Kafka, delegate acquisition/refresh to librdkafka native OIDC or the
verified Apache Java callback. Version-gate direct Java client-credentials
properties versus the documented legacy JAAS callback form; one profile model
serves both. Version checks must cover the token-endpoint trust mapping. Never
download plugins, add a token-refresh daemon, or inject a static Kafka token.

Registry clients use their own verified native refresh support. An adapter
without that capability rejects OAuth; do not emulate a long-lived client's
refresh via Kantrip. The bounded Registry HTTP ping is different: it needs a
single in-memory token request through a narrow client-credentials helper,
verified HTTPS and explicit token-endpoint trust. Bound response size, validate
token type/expiry, redact OAuth errors, and discard the token after the check.
Test basic and form-based client authentication only as supported by the chosen
native clients/provider; do not silently retry different credential methods.

**Acceptance for 2.1–2.2:** extend the implemented mutation failure matrix in
`DEVELOPMENT.md` to simultaneous
Kafka/Registry creation, rotation, and removal, including an unavailable store
halfway through staging either owner's credentials. A profile must never contain
a new Kafka identity with an unintended old/partial Registry identity.
Also verify independent Kafka/Registry identities and CAs,
credential rotation/recovery/removal, wrong endpoint trust, invalid secret,
expired fixed token, and no leakage from wrapped HTTP errors. Keep a real Java
and librdkafka session alive across token expiry and demonstrate successful
refresh and later revocation failure. Repeat native Registry refresh for each
distinct implementation and credential/trust path: Confluent Java, Confluent
Python, and native Apicurio. Equivalent console formats, direct commands, and
shell shims keep short adapter coverage but do not repeat the same expiry wait.
A successful short ping is not refresh evidence.

### 2.3 Registry authentication probes with minimum authorization

**Finding:** Registry HTTP authentication cannot be proven independently of an
allowed route. Deployments commonly block auxiliary identity and system routes
while exposing only the read/search APIs used by their clients. Subject,
artifact, config, and mode operations can require provider- and
deployment-specific roles. Sources:
[Confluent operation authorization](https://docs.confluent.io/platform/current/confluent-security-plugins/schema-registry/authorization/index.html)
and [Apicurio security](https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-configuring-registry-security.html).

**Decision:** use one fixed, non-mutating read query per provider:
Confluent-compatible `GET /subjects?limit=1` and native Apicurio v3
`GET /search/versions?limit=1`. Apicurio ccompat uses the Confluent strategy at
its configured base URL; never rewrite it into native mode. Validate JSON
content type and the exact provider response envelope. A valid empty collection
succeeds, does not require user-supplied IDs or canary resources, and proves
only permission for this list/search query. Confluent classifies subject listing
as `GLOBAL_READ`, not `SCHEMA_READ`; Apicurio's standard `sr-readonly` role
permits version search. Do not claim that every application invokes these
endpoints or that success proves access to a particular schema.

For a credentialed 200, make a bounded anonymous control request to the exact
same URL within the same deadline: anonymous 401/403 followed by credentialed
200 establishes the configured deployment's authentication gate. An anonymous
200 does not. Do not probe `/users/me`, `/system/info`, `/schemas/types`, or
artifact search as a fallback, submit deliberately wrong passwords during
normal ping, or add a user-controlled arbitrary probe URL.
For mTLS use verified handshake evidence from the actual configured connection;
never claim that optional client certificates were required by the server.
If authentication cannot be established for a deployment, report `transport
verified; authentication unverified` and return `1`. For `auth: none`, a
validated provider response suffices for connectivity but says nothing about
resource access. Never downgrade an authenticated profile to this result.

A credentialed HTTP 401 is authentication failure and a credentialed 403 is an
authorization failure; 404 and unrelated failures remain failures without
fallback. For the anonymous control only, 401 or 403 is conclusive rejection
after the same route succeeded with credentials. Success requires the expected
validated 200 response from an identity granted exactly the documented read
permission. Document that proxies must expose the chosen route.

Disable redirects for authenticated and token requests; never forward
Authorization to another origin or downgrade HTTPS. Ignore inherited HTTP
proxy configuration unless an explicitly supported policy is added later.
Bound reads and retries as well as connection time. Preserve partial service
results: Kafka success plus Registry failure is an overall failure with both
outcomes visible in normal mode. Quiet mode remains fully silent.

**Acceptance:** test empty and non-empty results, an identity with only the
documented read permission, the same identity without permission, wrong
credentials, public read, credentialed 401/403/404, invalid response,
redirects, revoked/expired tokens, custom CA, and server-required mTLS. Include
a synthetic reverse proxy that permits only the selected read endpoints and
blocks auxiliary routes. Add separate provider/version/security configurations
and record every remaining deployment limitation.

## PR 3 — Properties and Strimzi input sources

### 3.1 One normalization pipeline

**CLI delta:** `add PROFILE --from-properties FILE|-` and
`add PROFILE --from-strimzi FILE|-`, mutually exclusive and unavailable on
`edit`. No extra top-level command. Defaults apply only after source/manual
merging: the existing `localhost:9092`, plaintext, and default provider must
not masquerade as explicit values conflicting with an imported connection.

**Architecture:** use format-specific parsers feeding a typed candidate plus
separate ephemeral secret values and source provenance. Merge explicitly
provided non-secret flags only to fill absent fields or confirm equal normalized
values. Reject conflicting values, even if supplied later on the command line.
Validate everything before staging credentials through the existing add path.
A duplicate profile never overwrites. No raw document, raw JAAS, decoded Secret,
or arbitrary property map reaches SQLite or a temporary session file.

Bound source bytes and decoded content (1 MiB per document/PEM initially),
property count, YAML nesting, and token lengths. Reject duplicate YAML/JSON
keys, multiple documents, invalid encoding, invalid base64, and non-regular file
inputs; `-` is the explicit streaming alternative. Use safe parsers and never
include source values/lines in parser errors. Preserve user-owned source files.
Print only a bounded list/count of ignored property names, never values.

### 3.2 Java, librdkafka, and Confluent-generated properties

Implement Java escaping, Unicode escapes, comments, separators, and continuation
semantics explicitly; generic INI parsing is insufficient. Use a tokenizer for
supported JAAS modules/options and quoted escapes, not regex credential
extraction. Reject duplicate/conflicting security aliases and mixed dialects.
Shared unambiguous fields may normalize without choosing a dialect; reject a
combination requiring ambiguous interpretation instead of guessing.

Normalize the allowlist below, including all PR 2 auth variants. Resolve PEM
file references relative to the source file; stdin requires absolute paths.
Read file contents into the model/store, never retain an external private-key
path as a live dependency. Reject JKS/PKCS12 and custom login/callback classes.
Support only the documented official OAuth callback shapes. Ignore application
properties such as `acks`, `group.id`, serializers, timeouts, retries, caches,
subject strategies, and schema IDs; reject unknown security-like keys,
disabled verification, proxy configuration, and unsupported security modes.

Registry namespace handling must be explicit: a combined Kafka document's bare
`ssl.*` always refers to Kafka. Accept Registry-prefixed client properties only
with a verified producer dialect and strip the prefix into Registry fields.
Do not guess that one CA or key applies to both services. Standalone
Registry-only properties can fill a Registry connection while explicit flags
provide Kafka endpoints; ambiguous unprefixed TLS material is rejected.

Confluent-generated properties are public generator output, not a private CLI
credential cache. Version and preserve synthetic fixtures produced by
the [public Java generator](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/client-config/create/confluent_kafka_client-config_create_java.html)
(`confluent kafka client-config create java`); never invoke the vendor CLI on
the user's behalf or store its output. An unsupported generated property must fail
with the property name and a capability explanation.

### 3.3 Strimzi KafkaUser-generated Secrets

Accept exactly one Kubernetes `kind: Secret`, `apiVersion: v1`, JSON/YAML,
with standard base64 `data` from a TLS or SCRAM KafkaUser. Reject `KafkaUser`,
`List`, arbitrary secret shapes, ambiguous mixed TLS/SCRAM fields, and
unsupported `stringData` in this generated-Secret contract.

- SCRAM requires `data.password`, selects SCRAM-SHA-512 over TLS, and uses
  `metadata.name` as username only for a recognized generated KafkaUser Secret.
  Explicit `--username` may fill missing metadata, never conflict with it.
  Validate Strimzi labels/shape without calling Kubernetes.
- mTLS requires `data.user.crt` and `data.user.key`; verify correspondence.
  Standard ancillary `user.p12`, `user.password`, and `sasl.jaas.config` entries
  may be ignored by name when the required PEM/password fields suffice. They
  must not turn an otherwise standard Strimzi Secret into a rejected document,
  nor become a fallback to PKCS12 or raw JAAS parsing.
- Require `--bootstrap-servers`; use `--ca-file` for cluster server trust when
  necessary. A user's certificate issuer CA does not automatically establish
  listener trust; do not infer it from unrelated Secret data. Infer TLS from
  the recognized secure source; explicit conflicting `--transport plaintext`
  fails. Do not discover listeners, namespaces, or other Secrets.

### 3.4 Sandbox exports and acceptance

Update `sandbox/__main__.py` exports alongside import support. Its current mTLS
properties use PKCS12 and cannot serve as a positive PEM import test. Export a
separate Java PEM mTLS fixture, librdkafka variants, complete bootstrap metadata,
and independent OAuth endpoint trust. Retain PKCS12 only as a clearly negative
import fixture if still useful for external-client tests. All generated values
stay under ignored mode-0700 `sandbox/.state`, files mode 0600.

**Acceptance:** reuse the implemented mutation failure matrix in
`DEVELOPMENT.md` after normalization;
import is never a separate transaction path. Verify file and stdin forms,
every supported auth source, escaped
credentials/continuations, source-relative paths, missing terminal for a prompt,
manual/source conflicts, duplicate name/keys, malformed/oversized input,
unknown security keys, and source-file preservation. Verify credentials land
only in the OS store and canonical regenerated client files. Add round-trip
*normalization tests*, not a public profile export feature. Exercise Confluent
and both Strimzi forms against the live services.

## PR 4 — `kcl` and `kafkactl` adapters

**Architecture:** extend `adapters.py`, session rendering, shim dispatch,
capabilities, doctor discovery, and environment sanitization together.

| Client | Configuration contract to verify against the pinned client | Required first-release security |
| --- | --- | --- |
| `kcl` | Private TOML selected with `KCL_CONFIG_PATH`; reject profile/config-path/bootstrap/TLS/auth overrides | Plaintext, TLS, PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 |
| `kafkactl` | Private YAML selected with `KAFKA_CTL_CONFIG`; `KAFKA_CTL_WRITABLE_CONFIG` points inside the session; disable its keyring integration | Plaintext, TLS, PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 |

Pin and record actual client versions, official config-key references, and
minimum tested versions in the same PR. The upstream [kcl configuration contract](https://github.com/twmb/kcl#configuration)
and [kafkactl configuration contract](https://github.com/deviceinsight/kafkactl#configuration)
document these environment variables. Recheck against the exact pinned versions.
Use `keyring.enabled=false` in kafkactl; reject `--clear-keyring` and any
credential-provider/plugin path that bypasses the selected profile. kcl's
`-B`/`--bootstrap-servers`, `-C`/`--profile`, `--config-path`, and security-bearing
`-X` overrides must not supersede the profile. Remove conflicting inherited
`KCL_*` and `KAFKA_CTL_*` connection settings before injecting owned variables.
Do not invent flags or assume Registry support. mTLS and OAuth are optional
capability cells requiring native mappings and live tests; otherwise reject
them explicitly. Add no new Kantrip commands or options. Document each injected
file variable and the exact rejected external-client flags in compatibility.

Keep original user config/keyring untouched; create no persistent client
context. Both clients work through `exec PROFILE -- CLIENT ...` and temporary
Bash/Zsh/Fish shims. Recognize split, equals, and short override forms. Scope
config-write behavior to the runtime and verify cleanup after errors/signals.

**Acceptance:** version-pinned direct and interactive operations for each
required mechanism; unsupported combination refuses before operation; original
config files remain unchanged; no secrets in process arguments; no external
keyring writes. Include client-native metadata/produce/consume commands proven
by the pinned `--help` in manual QA before marking the PR complete.

## PR 5 — Readability, design patterns, and CLI release verification

### 5.1 Refactor the completed features for readability

**Outcome:** make the implementation delivered by PRs 1–4 easy to follow and
safe to change before release. Refactor those concrete flows together in this
PR; do not introduce a separate cleanup PR for each module or mechanism.

**Architecture:**

- Separate CLI input/prompt collection, domain validation, side effects, and
  output presentation. The CLI creates typed requests; lifecycle services own
  mutations; resolvers create one immutable execution snapshot; renderers
  translate it; adapters select native command/config mappings. Keep errors
  and redaction independent from Rich and shell text.
- Reuse the narrow `SecretStore` protocol and explicit adapter/strategy
  boundaries where supported clients or providers differ. Share capability
  checks and normalization, while retaining distinct Java, librdkafka, and
  provider property mappings. Prefer composition and focused functions; add a
  class only for a real responsibility. Avoid speculative factories, generic
  plugin discovery, and inheritance that hides behavior.
- Keep transaction and resource lifetimes visible. A caller must be able to
  identify who owns the maintenance lock, expected profile generation,
  credential journal stages, database commit, and post-commit cleanup. Do not
  move I/O into constructors or conceal lock acquisition in unrelated helpers.
  Preserve the cross-store recovery and exit-status contract in
  `ARCHITECTURE.md`.
- Consolidate duplicated connection-option rejection, child environment policy,
  capability decisions, and diagnostic classification at their shared seams.
  Keep shell-specific quoting and client-specific argument grammars separate.
  Keep secret-bearing values out of reprs, diagnostics, and test snapshots.
- Use clear domain names, small focused functions, and explicit typed results
  for success, unsupported capabilities, and mutation outcomes. Remove dead
  branches, obsolete helpers, and duplicate documentation left by earlier PRs.
  Keep Ruff `C901` at or below 10 without new suppressions. Do not reduce the
  count by merely hiding branches behind opaque dispatch or boolean switches.

**CLI delta:** none beyond the already implemented PRs 1–4. Preserve their
commands, flags, validation, exit statuses, structured output, and connection
precedence. This refactor does not add compatibility with earlier development
commits. Any discovered functional defect needs an explicit regression case and
an explained correction in the same PR.

**Acceptance:** review representative add/edit/remove, import, authenticated
exec, and ping paths end to end. Run existing behavior tests, especially
mutation fault/race recovery, secret redaction, profile snapshot attribution,
environment precedence, and direct/shell override rejection. Add tests only
for uncovered behavior or discovered defects, not to mirror helper structure.
Update architecture decisions and module diagrams in the same PR. Record
which duplication or responsibility problem each substantial refactor resolves.

### 5.2 Integration evidence and release gates

Run and record supported Linux/macOS and Python 3.10–3.14 checks; use actual
macOS Keychain and Linux Secret Service for store integration. Offline tests
remain offline. Keep the existing smoke workflow and PTY shell contract; add
secure integrations separately rather than making ordinary tests require
Docker or keyring access. Pin test clients and retain sanitized evidence for
minimum and representative current versions.

Fill any remaining infrastructure gaps: PLAIN/SCRAM-SHA-256, authorizer-enabled
Kafka with no-ACL principals, no-role and wrong-credential Registry identities,
Registry TLS-only/mTLS, token expiry/revocation, and encrypted/realistic PEM
keys. Include the transaction fault/race matrix and source-environment precedence
checks from the completed Kafka foundation; record process-crash evidence separately from OS/store restart
and power-loss assumptions. These fixtures must be ready before their
corresponding manual checks.
Do not accept “not installed” as a pass for a required compatibility cell.
Record optional unsupported cells explicitly.

Required existing gates for every PR:

```bash
uv run --locked python -m scripts.analyze
uv run --locked python -m scripts.tests
uv build --clear
uv run --locked python -m scripts.verify_release dist
```

Release integration also runs:

```bash
uv run --locked python -m scripts.verify_shell_contract
uv run --locked python -m scripts.smoke
```

Complete the manual QA below against the built wheel, record candidate commit,
OS, client/server/library versions, expected/actual result, and sanitized
failure evidence. Automated gates and smoke do not replace human QA, and human
QA does not replace them. Do not tag or publish a package/release as part of
roadmap implementation. PR 6 separately specifies the website deployment.

### 5.3 Reconcile CLI documentation by audience

Update affected docs in every PR, then audit the CLI documentation in PR 5.
PR 6 adds the site and PR 7 performs the final repository-wide closure. Replace
obsolete claims rather than accumulating “old/new” sections.

**Ownership:** end users read `USAGE.md` and `COMPATIBILITY.md`; developers read
`DEVELOPMENT.md`, `ARCHITECTURE.md`, `THREAT_MODEL.md`, and `MANUAL_TESTING.md`;
AI agents use `AGENT.md`, `RELEASE_CHECKLIST.md`, and this temporary `MVP.md`.
End-user guides contain installed `kantrip` commands and current support only:
no sandbox, `uv run`, development workflows, internal-only capabilities, or
future flags. Repository setup and laboratory commands belong in developer
guides. Keep technical decisions and rationale in architecture, actionable
engineering conventions in agent instructions, and release orchestration in
the agent checklist.

| Artifact | Required final review |
| --- | --- |
| `README.md` | Actual first-release capabilities, onboarding, current examples and concise limitations |
| `USAGE.md` | Mutation outcome/exit-status contract, environment precedence, complete help/option contract, no-echo interactions, imports, independent trust, scoped doctor/sessions, ping proof/exit status, exact child environment |
| `COMPATIBILITY.md` | Separate CLI/version, Kafka protocol/auth, Registry provider/auth, input-format, and generated-format matrices; supported/unsupported/conditional cells with evidence and minimum tested version |
| `ARCHITECTURE.md`, `images/*.svg` | Actual resolvers, mutation flow, capability checks, native refresh, session revision attribution, probe state and input pipeline |
| `THREAT_MODEL.md`, `SECURITY.md` | Atomic profile visibility versus cross-store recovery, durability/backup limits, implemented controls versus residual risk; token request/redirect handling, imported documents, keyring limits, no-ACL/no-role proof limits |
| `AGENT.md`, `DEVELOPMENT.md` | Durable final contracts, first-release boundary, fixture/integration workflows, supported platforms and dependencies |
| `MANUAL_TESTING.md` | Move completed runnable QA scenarios here, remove superseded expectations, keep setup/actions/results and release checklist entry point |
| `RELEASE_CHECKLIST.md` | Link manual first-release gate and compatibility evidence; distinguish first release from upgrades of published versions |
| `schemas/`, `examples/` | Implemented schema only, synthetic examples covering secure profiles and supported input formats; no secrets or live references |
| `scripts/`, `sandbox/`, `.github/`, `pyproject.toml` | Help, fixtures, smoke, release packaging inclusion, workflow/template references and version pins match the final contract |
| `MVP.md` | Until PR 7, remove completed items only after transferring decisions and manual QA; in PR 7 delete the file and its references after the closure criteria pass |

User compatibility must distinguish executable CLI creation, session execution,
ping, and import support. Keep schema-only acceptance and renderer internals in
architecture. Document Java PEM gates, actual linked librdkafka
and Registry libraries, kcat's Avro-only Registry decoding, Kaskade's native
Apicurio support, unsupported provider/mechanism combinations, and file formats
that are output-only. Do not label JSON/YAML `describe` as an import/export
format. Include plaintext, verified TLS, PLAIN, both SCRAM mechanisms, mTLS,
OAuth/OAUTHBEARER, Registry basic/fixed bearer, Java/librdkafka properties,
Confluent-generated properties, and Strimzi Secret JSON/YAML explicitly.

## PR 6 — Publish the terminal-themed GitHub Pages site

**Scope:** implement [issue #8](https://github.com/sauljabin/kantrip/issues/8) as
one PR containing the site, artifact validation, workflow, and contributor
instructions. Site claims must reflect PR 5's verified behavior.

**Content:** reuse the repository banner/artwork; show the slogan, concise
product description, version-independent installation command, and a short
`kantrip add` / `kantrip exec` example. Summarize supported clients and actual
credential/security guarantees, including relevant limits. Link to Usage,
Compatibility, releases, issues, and source. Do not duplicate the full usage
manual or link users to the temporary roadmap. Match README claims and the
available installation channel; do not advertise an unpublished stable package.

**Architecture:**

- Keep source in `site/`: static HTML, CSS, local artwork, and minimal optional
  JavaScript. Essential content/navigation works without JavaScript. Use
  semantic HTML, visible keyboard focus, sufficient contrast, responsive
  layout, light/dark support, and reduced-motion preferences. No required
  frontend framework, analytics, cookies, external fonts, or runtime requests
  to third-party services.
- Add a focused `scripts.site` workflow with developer commands
  `uv run --locked python -m scripts.site build --output PATH` and
  `uv run --locked python -m scripts.site validate --path PATH`. Build into a
  new or empty output directory and refuse unsafe paths rather than deleting
  arbitrary contents. Copy an explicit allowlist of public site files and
  selected artwork; never publish the repository root, `.state`, credentials,
  build caches, private fixtures, or symlinks. Validate the assembled artifact,
  required content, local links/assets, and project URL prefix `/kantrip/`.
- Build and validate on pull requests with read-only repository permissions,
  no deployment credentials, and no `pull_request_target` execution of PR
  code. Make successful site validation a required CI gate. Deploy only on a
  push to `main`, after that commit's validation succeeds, using the same
  validated Pages artifact and the standard configure/upload/deploy actions.
  Pin reviewed action versions and follow the
  [GitHub custom Pages workflow](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages).
- Give only the deploy job `pages: write` and `id-token: write`, use the
  `github-pages` environment restricted to `main`, and serialize deployment.
  Document the repository's Actions-based Pages setting and environment setup
  in `DEVELOPMENT.md`. Check live output after merge; an uploaded artifact alone
  does not satisfy the issue's published-site requirement.

**CLI delta:** no change to installed `kantrip` commands or arguments. The two
new `scripts.site` commands are developer tooling and belong only in developer
and agent instructions. No website secrets or new runtime service dependency.

**Acceptance:** a PR workflow builds/validates without a deploy job running;
main deployment publishes the validated artifact; links/assets work under the
project prefix; essential content works with JavaScript disabled. Inspect the
artifact allowlist and confirm no tracking, remote fonts, or private material.
Record the deployed URL and commit. Close issue #8 only when its published-site
acceptance is satisfied, not when this planning roadmap lands.

**Manual QA to move into `MANUAL_TESTING.md`:**

- [ ] Build into a fresh temporary directory and validate it:

  ```bash
  site_qa_root="$(mktemp -d "${TMPDIR:-/tmp}/kantrip-site.XXXXXX")"
  uv run --locked python -m scripts.site build --output "$site_qa_root/kantrip"
  uv run --locked python -m scripts.site validate --path "$site_qa_root/kantrip"
  python3 -m http.server 8000 --bind 127.0.0.1 --directory "$site_qa_root"
  ```

  Open `http://127.0.0.1:8000/kantrip/`. Expect the banner, install/example text,
  working local assets and all destination links. Stop the server after QA.
- [ ] Use a narrow mobile viewport and a wide desktop viewport; zoom to 200%,
  navigate by keyboard only, and inspect accessible names/headings. Expect
  usable navigation, visible focus, readable text, and no clipped controls.
- [ ] Disable JavaScript, select light/dark appearance, and enable reduced
  motion. Expect essential content and navigation in every combination, with
  decorative animation removed or reduced as requested.
- [ ] Inspect the browser network panel and storage: no analytics, cookies,
  external fonts, or third-party runtime assets. Verify PR CI has no deployment,
  then verify the main deployment URL and repeat the link/content checks live.

## PR 7 — Preserve technical knowledge and remove the roadmap

**Outcome:** leave a self-contained repository for developers, users, and future
agents, with no temporary planning document or contradictory security claims.
This PR depends on all preceding work, including the site's acceptance. Do not
delete `MVP.md` now or use deletion to hide incomplete release obligations.

### 7.1 Transfer decisions and align the threat model

Inventory every remaining roadmap section, property mapping, acceptance rule,
manual check, and known limitation. For each, record its durable destination in
the PR description and verify the text arrived before removal:

| Knowledge | Durable destination |
| --- | --- |
| Product boundary, module responsibilities, chosen patterns and rationale | `ARCHITECTURE.md`; concise enforceable rules and links in `AGENT.md` |
| Mutation state machine, UUID/revision checks, lock ownership, reader snapshots, recovery and durability/backup limits | `ARCHITECTURE.md`; actionable mutation invariants in `AGENT.md` |
| Credential ownership, independent trust, OAuth refresh, provider probes, capability gates, input normalization and native property mappings | `ARCHITECTURE.md`; required implementation constraints in `AGENT.md` |
| Current commands/options, mutation outcome statuses, child environment and user-visible failure guidance | `USAGE.md`; tested support and version limits in `COMPATIBILITY.md` |
| Security objectives, trust boundaries, implemented mitigations, attack/failure cases, residual risks and evidence | `THREAT_MODEL.md`, aligned with the final architecture; disclosure policy in `SECURITY.md` |
| Laboratory setup, generated temporary PKI, integration and site workflows | `DEVELOPMENT.md`; runnable human scenarios in `MANUAL_TESTING.md` |
| All first-release manual QA and site QA | `MANUAL_TESTING.md`, preserving setup, exact commands, expected results and evidence requirements |
| Candidate/platform evidence, required checks and publication orchestration | `RELEASE_CHECKLIST.md`; link to human QA instead of duplicating it |

Preserve both the decision and why it was chosen, including rejected alternatives
where needed to explain a constraint. `ARCHITECTURE.md` owns detailed technical
knowledge; `AGENT.md` owns concise instructions and links sufficient to find it.
Do not replace this file with another copy of the roadmap or an archive required
to understand the product. Keep unrelated deferred ideas as issues only if
explicitly deferred; unresolved MVP acceptance requirements block this PR.

Reconcile the threat model against the actual final code and architecture:
add/edit/remove and journal recovery, OS-store and filesystem trust, session and
process lifetime, imported files, environment precedence, TLS/OAuth/Registry
boundaries, probe proof limits, secret redaction, backups, and site deployment
permissions. Separate proven controls from platform assumptions and residual
risk. Do not claim distributed ACID, universal power-loss recovery, secure
erasure, or credential protection from a compromised account/OS. Link each
security-sensitive invariant to its maintained verification workflow or test
coverage; close any missing coverage before declaring the MVP finished.

### 7.2 Delete obsolete content and verify the final repository

- Delete `MVP.md` only after PRs 1–6 are accepted and the transfer above is
  complete. Remove its entry from `pyproject.toml` source-distribution includes
  and `scripts/verify_release.py` required files; update affected packaging tests.
- Clean `AGENT.md`: replace future-work pointers, obsolete restrictions, repeated
  architecture prose, and temporary instructions with current conventions and
  durable links. Retain the documentation ownership rule and release safety
  boundaries. No instruction may require a deleted planning document.
- Review README, all root guides, diagrams, schemas/examples, site, scripts,
  workflows, and templates for obsolete names, promises, roadmap links/anchors,
  and duplicated decisions. Remove the README roadmap section. Keep
  `RELEASE_CHECKLIST.md` as the single filename; update its first-release gate to
  the completed manual guide and remove temporary planning references.
- Verify every local link and anchor resolves, user guides contain neither
  sandbox nor `uv run` instructions, and no tracked artifact still requires
  `MVP.md`. Run `rg -n 'MVP\.md' --glob '!.git/**'`; expect no remaining references
  after this document itself is deleted. Validate the site artifact again.
- Run the four required gates from PR 5 against the final tree. Inspect the
  wheel and source distribution: the removed roadmap is absent, all durable
  guides and required tests remain included, and source-distribution validation
  succeeds. Review candidate CI and manual/integration evidence after cleanup;
  repeat affected checks if any commands, assets, or behavior changed.

**CLI delta:** none. Changes are documentation, links, and packaging inventory;
no migration or compatibility layer for this deleted development document.

**Acceptance:** a new agent can implement maintenance or prepare a release using
`AGENT.md`, architecture, the developer guides, and `RELEASE_CHECKLIST.md`, with
no dependency on this roadmap or conversation. Every technical decision and
manual release obligation has a durable home, the threat model matches the
implemented system, and final required verification passes. Package publication
and tagging still require the separate release authorization.

## Connection-property normalization reference

This is a closed mapping specification, not generic passthrough. Verify exact
keys and minimum client versions when implementing PRs 2–4. Native libraries
with incompatible prefixes, unsupported PEM, or missing refresh get a distinct
adapter mapping or a capability error.

| Concern | Java Kafka | librdkafka |
| --- | --- | --- |
| Bootstrap / transport | `bootstrap.servers`, `security.protocol` | Same |
| CA | `ssl.truststore.type=PEM` + `ssl.truststore.certificates` or PEM `ssl.truststore.location` | `ssl.ca.pem` or `ssl.ca.location` |
| Client certificate | `ssl.keystore.type=PEM`, `ssl.keystore.certificate.chain` | `ssl.certificate.pem` or `.location` |
| Private key | `ssl.keystore.key`, optional `ssl.key.password` | `ssl.key.pem` or `.location`, optional `ssl.key.password` |
| Verification | `ssl.endpoint.identification.algorithm=HTTPS` | `enable.ssl.certificate.verification=true`, `ssl.endpoint.identification.algorithm=https` |
| SASL | `sasl.mechanism`, internally constructed `sasl.jaas.config` | `sasl.mechanism`/recognized `sasl.mechanisms` alias, `sasl.username`, `sasl.password` |
| OAuth | Verified callback plus `sasl.oauthbearer.token.endpoint.url`, direct `sasl.oauthbearer.client.credentials.client.id`/`.client.secret` and scope, or version-gated official JAAS form | `sasl.oauthbearer.method=oidc`, `.token.endpoint.url`, `.client.id`, `.client.secret`, `.scope` |
| Token CA | Standard callback's versioned SSL options; separate from broker CA | `https.ca.pem` or `https.ca.location` |

| Concern | Confluent Registry client namespace | Native Apicurio namespace |
| --- | --- | --- |
| URL | `schema.registry.url` | `apicurio.registry.url` |
| Basic | `basic.auth.credentials.source=USER_INFO`, `basic.auth.user.info` | `apicurio.registry.auth.username`, `.password` |
| Fixed bearer | `bearer.auth.credentials.source=STATIC_TOKEN`, `bearer.auth.token` | Unsupported |
| OAuth | `bearer.auth.credentials.source=OAUTHBEARER`, `.issuer.endpoint.url`, `.client.id`, `.client.secret`, `.scope` | `apicurio.registry.auth.service.token.endpoint`, `.client.id`, `.client.secret` |
| Routing | `bearer.auth.logical.cluster`, `bearer.auth.identity.pool.id` | No invented equivalents |
| TLS/mTLS | Standard client `ssl.*` PEM trust/key fields, correctly prefixed when combined with Kafka | Version-verified `apicurio.registry.tls.*` PEM trust, certificates, verify-host and client identity properties |

Never accept Registry `URL`, `SASL_INHERIT`,
`SASL_OAUTHBEARER_INHERIT`, custom credential providers, credentials in URLs,
`trust-all`, empty hostname-verification settings, or Kafka/Registry identity
inheritance. Do not invent a native Apicurio scope or client-key property if its
selected client does not expose it. Reject that profile/client combination.

## Manual QA — First-release checklist

Run this section **after the owning PRs land**, using the candidate wheel. These
are human checks, not claims that future flags work in the audited commit.
Keep boxes unchecked here; record results externally per candidate. Use only
disposable laboratory identities and data. Run shell blocks in Bash/Zsh unless
the step explicitly enters Fish. Any capitalized placeholder must be replaced
with a non-secret value from the pinned fixture's setup instructions.

### QA 1 — Install and isolate the release candidate

- [ ] Build/verify artifacts using PR 5 commands. Install the exact wheel in a
  fresh virtual environment; use that `kantrip` for all following commands.
  Supply the actual wheel filename, not a literal placeholder:

  ```bash
  uv venv /tmp/kantrip-release-qa
  uv pip install --python /tmp/kantrip-release-qa/bin/python dist/ACTUAL_WHEEL.whl
  export PATH="/tmp/kantrip-release-qa/bin:$PATH"
  kantrip --version
  kantrip --help
  export KANTRIP_QA_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/kantrip-release.XXXXXX")"
  chmod 700 "$KANTRIP_QA_ROOT"
  mkdir -m 700 "$KANTRIP_QA_ROOT/runtime"
  export KANTRIP_DATABASE="$KANTRIP_QA_ROOT/data/profiles.db"
  export XDG_RUNTIME_DIR="$KANTRIP_QA_ROOT/runtime"
  kantrip list -o json
  kantrip doctor
  test ! -e "$KANTRIP_DATABASE"
  ```

  Expect the candidate version, exactly the nine lifecycle commands, an empty
  JSON list, and no database creation by inspection. Doctor may report missing
  optional clients; record each. Configure an approved OS store and do not use
  a plaintext keyring fallback. Copy these three isolation variables to any
  second terminal. Confirm the virtual environment path was unused beforehand.

### QA 2 — Sandbox, plaintext, TLS, and profile observations

- [ ] Bring up the existing sandbox from the repository root and inspect it:

  ```bash
  uv run --locked python -m sandbox up
  uv run --locked python -m sandbox status
  kantrip add qa-plain -b localhost:9092 -l release=qa
  kantrip add qa-tls -b localhost:9093 --transport tls \
    --ca-file sandbox/.state/ca.crt -l release=qa
  kantrip ping qa-plain
  kantrip ping qa-tls
  kantrip exec qa-plain -- kafka-topics.sh --create \
    --topic qa-release --partitions 1 --replication-factor 1
  kantrip list -l release=qa -o json
  kantrip describe qa-tls -o yaml
  NO_COLOR=1 kantrip describe qa-tls
  kantrip describe qa-tls --no-color -o json
  ```

  Expect correct transport/auth observations, custom CA trust, no ANSI in
  unstyled outputs, no credentials/references, and valid JSON/YAML. Test duplicate
  `add qa-tls` fails without mutation. A profile with a wrong CA or hostname
  must fail ping as TLS failure, not authentication failure.

### QA 3 — Password and mTLS lifecycle

- [ ] Create SCRAM and mTLS profiles using sandbox material. Obtain the SCRAM
  password through a private local credential viewer, enter it only at the
  no-echo prompt, and do not paste it into argv or transcripts:

  ```bash
  kantrip add qa-scram -b localhost:9094 --transport tls \
    --ca-file sandbox/.state/ca.crt --auth scram-sha-512 \
    --username kantrip-scram -l release=qa
  kantrip add qa-mtls -b localhost:9095 --transport tls \
    --ca-file sandbox/.state/ca.crt --auth mtls \
    --client-certificate-file sandbox/.state/user.crt \
    --client-key-file sandbox/.state/user.key -l release=qa
  kantrip ping qa-scram
  kantrip ping qa-mtls
  kantrip describe qa-scram
  kantrip edit qa-scram --replace-secret kafka/password
  kantrip ping qa-scram
  kantrip edit qa-scram
  ```

  Enter a deliberately wrong password during replacement, expect authentication
  failure, then replace it with the correct value and expect success. In the
  editor verify keep/replace/remove and cancellation without displaying existing
  values. Repeat creation/ping for `--auth plain` and `--auth scram-sha-256`
  against the documented sandbox fixture endpoints; do not reuse port 9094 for these.
  Repeat mTLS with an encrypted key, incorrect key password, and mismatched
  certificate; invalid identity must fail before launching a client.

### QA 4 — Session attribution and concurrent edits

- [ ] Terminal A runs a long enough command to inspect in Terminal B:

  ```bash
  kantrip exec qa-scram -- sleep 180
  ```

  Terminal B, with the same isolated state, runs:

  ```bash
  kantrip describe qa-scram
  kantrip doctor qa-scram --sessions
  kantrip edit qa-scram -d 'Changed during active session'
  kantrip doctor qa-scram --sessions --verbose
  kantrip doctor qa-mtls --sessions
  kantrip doctor qa-scram --repair
  kantrip doctor --sessions
  ```

  Expect old revision on the active session, new revision in `describe`, no
  cross-profile sessions, and errors for both invalid doctor combinations.
  Default output omits PID/session ID/private paths. After A exits, its runtime
  is gone. Separately open `edit qa-scram` in A, change its description in B,
  then finish A: A must reject the stale revision without losing B's change.

### QA 5 — ACL-independent Kafka ping and silent status

- [ ] Follow the authorizer-enabled sandbox instructions to create
  `qa-noacl` with valid credentials and no topic, group, or cluster permissions:

  ```bash
  kantrip ping qa-noacl --timeout 5
  kantrip ping qa-noacl -q >"$KANTRIP_QA_ROOT/out" 2>"$KANTRIP_QA_ROOT/err"
  echo $?
  wc -c "$KANTRIP_QA_ROOT/out" "$KANTRIP_QA_ROOT/err"
  kantrip exec qa-noacl -- kafka-topics.sh --create \
    --topic qa-permission-denied --partitions 1 --replication-factor 1
  kantrip edit qa-noacl --replace-secret kafka/password
  kantrip ping qa-noacl -q >"$KANTRIP_QA_ROOT/out" 2>"$KANTRIP_QA_ROOT/err"
  echo $?
  wc -c "$KANTRIP_QA_ROOT/out" "$KANTRIP_QA_ROOT/err"
  ```

  Expect first ping/status `0`, empty files, and an explicit authorization
  failure from topic creation. Enter a wrong password at replacement; second
  ping returns `1` with both files still empty. Restore the password. Repeat
  with unavailable service and invalid CA; time the operation to check one
  deadline. Confirm no debug logs or broker counts presented as auth proof.

### QA 6 — Registry security, providers, and no-role diagnostics

- [ ] Add basic-auth Registry to `qa-scram`, keeping Kafka credentials unchanged:

  ```bash
  kantrip edit qa-scram --registry-provider confluent \
    --registry-url https://localhost:8083 --registry-auth basic \
    --registry-username REGISTRY_BASIC_USER \
    --registry-ca-file sandbox/.state/ca.crt
  kantrip ping qa-scram
  kantrip describe qa-scram -o json
  kantrip edit qa-scram --replace-secret registry/password
  kantrip ping qa-scram
  kantrip edit qa-scram --remove-registry
  kantrip ping qa-scram
  ```

  Use the generated sandbox username and prompted password. First ping succeeds;
  replacing only the Registry password with a wrong value reports Registry auth
  failure while retaining Kafka success; removal restores Kafka-only success.
  Repeat basic for native Apicurio `https://localhost:8084/apis/registry/v3`.
  Repeat HTTPS/no-auth, fixed token for Confluent, and server-required mTLS with
  PR 2's fixtures and the corresponding `--registry-auth`/certificate flags.

- [ ] Verify `GET /subjects?limit=1` for Confluent-compatible profiles and
  `GET /search/versions?limit=1` for native Apicurio v3 with empty and non-empty
  results. Use read-only and no-role identities, then repeat invalid credentials,
  anonymous/public read, credentialed 401/403/404, and wrong-origin redirect.
  A synthetic proxy must allow only these selected endpoints while blocking
  `/users/me`, `/system/info`, `/schemas/types`, and artifact search. Expect
  `1` for insufficient proof and no credential forwarding. Test Apicurio
  ccompat separately without rewriting its base URL.

### QA 7 — OAuth and refresh

- [ ] Create an OAuth profile with the generated public client ID and prompted
  secret; token endpoint trust is explicit:

  ```bash
  kantrip add qa-oauth -b localhost:9096 --transport tls \
    --ca-file sandbox/.state/ca.crt --auth oauth \
    --oauth-token-url https://localhost:8443/realms/kantrip/protocol/openid-connect/token \
    --oauth-client-id KAFKA_OAUTH_CLIENT_ID \
    --oauth-ca-file sandbox/.state/ca.crt -l release=qa
  kantrip ping qa-oauth
  kantrip exec qa-oauth -- kcat -C -t qa-release -o end
  ```

  Use PR 2's short-token-lifetime fixture and publish records before and after
  expiry from another terminal. Consumption must continue after refresh. Repeat
  with a verified Java consumer. Revoke the client at the IdP and confirm the
  next refresh fails without secret output, then restore the fixture. Ctrl-C
  exits cleanly. Repeat Registry OAuth for each advertised client with
  `--registry-oauth-*`; native Apicurio and Confluent need separate cases.
  Invalid token CA must fail even when broker CA is correct. A ping alone does
  not complete this checkbox.

### QA 8 — Properties and Strimzi file/stdin input

- [ ] Import the final sandbox exports (PR 3 must generate the named PEM file):

  ```bash
  kantrip add qa-props --from-properties sandbox/.state/kafka-scram.properties
  cat sandbox/.state/kafka-scram.properties | kantrip add qa-props-stdin --from-properties -
  kantrip add qa-pem --from-properties sandbox/.state/kafka-mtls-pem.properties
  kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
    get secret kantrip-scram -o yaml | kantrip add qa-strimzi \
    --from-strimzi - -b localhost:9094 --ca-file sandbox/.state/ca.crt
  kubectl --context kind-kantrip-sandbox -n kantrip-sandbox \
    get secret kantrip-mtls -o json | kantrip add qa-strimzi-mtls \
    --from-strimzi - -b localhost:9095 --ca-file sandbox/.state/ca.crt
  kantrip ping qa-props
  kantrip ping qa-props-stdin
  kantrip ping qa-pem
  kantrip ping qa-strimzi
  kantrip ping qa-strimzi-mtls
  ```

  Repeat Java/librdkafka/OAuth/Registry and Confluent generator fixtures from
  PR 3. For Strimzi file mode, redirect a disposable generated Secret to a
  mode-0600 file under the QA root and pass that path; verify it is unchanged.
  Compare safe observations, never print credentials. Confirm no source document
  or secret value was persisted in SQLite, backups, diagnostics, or history.

- [ ] Try negative inputs: `kubectl ... get kafkauser kantrip-scram -o yaml`,
  PKCS12 properties, malformed base64, duplicate keys, unknown `ssl.*`, disabled
  hostname verification, conflicting manual bootstrap, both source flags, and
  an existing profile name. Each `add` must fail without a partial profile or
  unjournaled credential. Exercise non-secret ignored settings and verify the
  summary contains property names only. Test relative PEM paths from a file
  and rejection of relative paths from stdin.

### QA 9 — All supported client families and shell modes

- [ ] With an authorized `qa-scram` profile and the required clients installed,
  run a small disposable record round trip:

  ```bash
  kantrip exec qa-scram -- kafka-topics.sh --describe --topic qa-release
  printf 'release-check\n' | kantrip exec qa-scram -- kcat -P -t qa-release
  kantrip exec qa-scram -- kcat -C -t qa-release -o beginning -c 1
  kantrip exec qa-scram -- kafka-console-consumer.sh \
    --topic qa-release --from-beginning --max-messages 1
  kantrip exec qa-scram -- kaskade admin
  ```

  Expect the same record, successful Kaskade connection, and cleanup after
  exit. Repeat with Confluent's unsuffixed tools and every remaining shared
  command in `COMPATIBILITY.md`, using disposable topics/groups for mutations.
  Use `MANUAL_TESTING.md` schema-record scenarios for all six Confluent
  Avro/JSON Schema/Protobuf console commands, kcat Avro, and both Kaskade Registry
  providers, repeating supported secure cells. An unsupported cell must fail
  before its operation with a capability explanation.

- [ ] Inspect the installed versions/help, then run their metadata operations:

  ```bash
  kcl --help
  kafkactl --help
  kantrip exec qa-scram -- kcl topic list
  kantrip exec qa-scram -- kafkactl get topics
  ```

  Run the version-pinned produce/consume commands added in PR 4 through
  `kantrip exec qa-scram -- ...`. Verify original user configs and external
  keyring entries are unchanged.
  Check both documented private config variables inside the supervised child,
  without printing file contents or secret values.

- [ ] For each installed absolute shell path, run
  `SHELL=/ABSOLUTE/SHELL kantrip exec qa-scram`, then `kantrip current`, the
  adapter commands above, and `exit`. Cover Bash, Zsh, and Fish with startup
  aliases/functions (and Fish abbreviations) shadowing clients. Verify shims
  restore the selected connection and history/startup behavior. Try
  `kantrip exec qa-scram` inside the session: it must reject nesting. Try
  bootstrap/config/auth overrides, including `kcat -F /tmp/other.conf` and
  Java `--bootstrap-server other.example.com:9092`; they must be rejected.

### QA 10 — Sourced environment and shell startup precedence

- [ ] Deliberately export a synthetic sandbox
  secret plus conflicting public connection values in a disposable subshell.
  This tests the hazardous `set -a` case without exposing real credentials:

  ```bash
  (
    set -a
    KANTRIP_SANDBOX_KAFKA_SCRAM_PASSWORD=synthetic-release-qa
    KAFKA_BOOTSTRAP_SERVERS=unselected.invalid:19092
    KCAT_CONFIG=/synthetic/unselected.conf
    SCHEMA_REGISTRY_URL=http://unselected.invalid
    set +a
    kantrip exec qa-plain -- python -c '
  import os
  assert os.environ["KAFKA_BOOTSTRAP_SERVERS"] == "localhost:9092"
  assert os.environ["KCAT_CONFIG"] != "/synthetic/unselected.conf"
  assert "SCHEMA_REGISTRY_URL" not in os.environ
  assert not any(name.startswith("KANTRIP_SANDBOX_") for name in os.environ)
  print("Profile precedence and sandbox scrubbing verified")'
    test "$KAFKA_BOOTSTRAP_SERVERS" = unselected.invalid:19092
    test "$KANTRIP_SANDBOX_KAFKA_SCRAM_PASSWORD" = synthetic-release-qa
  )
  ```

  Expect the selected child config, no inherited sandbox values, and unchanged
  parent values. Repeat after `. sandbox/.state/credentials.env`, with and
  without automatic export, checking only presence/names and public config
  values. Never print the resulting environment. Run the equivalent checks in
  each interactive shell from QA 9 with disposable startup files that export a
  conflicting `KCAT_CONFIG`; both the initial prompt environment and supported
  adapters must use the selected profile. An intentionally configured custom
  child remains responsible for its own behavior.

### QA 11 — Mutation outcome, confirmation races, and persistence

- [ ] In Terminal A create a disposable profile and leave its remove
  confirmation pending:

  ```bash
  kantrip add qa-remove-race -b localhost:9092
  kantrip describe qa-remove-race
  kantrip remove qa-remove-race
  ```

  In Terminal B, using the same QA database, change that profile:

  ```bash
  kantrip edit qa-remove-race -d 'Changed after confirmation started'
  ```

  Confirm in A. Expect rejection with exit `1`; the newer revision remains.
  Repeat, but in B remove with `--force` and recreate the same name before
  confirming in A. The recreated UUID must survive A's outdated confirmation.
  `--force` skips the question, not identity validation or safe cleanup.

- [ ] Verify add/edit/reopen/remove from fresh CLI processes:

  ```bash
  kantrip add qa-persist -b localhost:9092
  kantrip describe qa-persist -o json
  kantrip edit qa-persist -d 'Persisted revision'
  kantrip describe qa-persist -o json
  kantrip remove qa-persist --force
  kantrip list -o json
  kantrip doctor --repair
  kantrip doctor --repair
  ```

  Expect one stable UUID, one revision increment, successful removal, and
  idempotent repair. Repeat with a disposable secret-bearing profile, restarting
  the approved OS store through its documented local workflow before a fresh
  `describe`/`ping`; the stored credential must remain available after unlock.
  Run only against laboratory accounts, not a production keychain/session.

- [ ] Review the automated fault-injection suite and reproduce its documented
  subprocess-crash cases in the isolated fixture. Inspect through `describe`,
  `list`, and `doctor` after each restart. A committed change with cleanup debt
  must report exit `3` and remain committed; an indeterminate commit reports `4`
  and must not trigger automatic retry or speculative deletion. Failures before
  commit preserve the old profile/absent add, with any staged entries journaled.
  Verify a live-reference journal conflict causes no credential deletion. Record
  the actual injected cut, exit status, and observed generation without secrets.
  Do not corrupt a real keychain or database to simulate this case. Document
  separately that process-crash tests do not establish power-loss guarantees.


### QA 12 — Store failures, repair, cleanup, and release sign-off

- [ ] Lock the disposable OS credential store through its native UI; run
  `kantrip describe qa-scram`, `kantrip doctor qa-scram`, and
  `kantrip exec qa-scram -- kcat -L`. Expect unavailable states and no client
  launch. Unlock it. Delete only the test profile's password entry via the OS
  UI, confirm `missing`, then restore via
  `kantrip edit qa-scram --replace-secret kafka/password`. Repeat on both OSes.
  Use existing manual reconciliation scenarios for an interrupted mutation;
  `kantrip doctor --repair` must clean only exact journaled entries and a second
  repair must be idempotent.

- [ ] Run the existing signal/crash/symlink scenarios in `MANUAL_TESTING.md`,
  now with a secret-bearing profile. Use `doctor PROFILE --sessions --verbose`
  to identify only the disposable supervisor before a forced kill. Verify
  active sessions survive repair, unlocked sessions become stale after five
  minutes, safe stale directories are removed, invalid/symlink entries remain,
  and terminal settings recover after ordinary signals. Inspect modes and
  process arguments without printing private config contents.

- [ ] Remove every QA profile through `kantrip remove PROFILE`: cancel once and
  confirm no change, then use `--force` for the rest. Verify profile-owned
  secrets are gone or exact failures remain journaled; run doctor/repair again.
  Do not delete the database before credential cleanup. Then remove only the
  recorded temporary QA root/virtual environment and tear down disposable
  fixtures. `python -m sandbox down` retains `.state`; remove that private
  generated directory separately only when finished with its credentials.
  Record manual results and remaining issues beside automated/smoke evidence.
  Complete `RELEASE_CHECKLIST.md`; any required unchecked or failed case blocks
  a first-release readiness claim.

## Outside the MVP

Database downgrade or development-state compatibility; profile history/rollback;
secret retrieval or round-trip export; detached/background sessions; Kubernetes
discovery; arbitrary secret providers; Kafka fixed-token/refresh-token OAuth;
client-side Strimzi OAuth callbacks; MSK IAM; Windows; automatic JKS/PKCS12
conversion; HTTP proxy profiles; additional adapters without a versioned safe
contract; hosted service, daemon, programmatic profile API, or interactive TUI.
