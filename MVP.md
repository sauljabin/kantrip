# First-release MVP roadmap

This is the implementation handoff for the remaining first-release work, audited
against commit `70f2d42`. Read PRs 1–5 in order. Each numbered PR is one delivery
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
| 1 | Usable authenticated Kafka profiles | Lifecycle CLI, credential observations, existing adapters, Kafka ping, profile doctor and session attribution | Current foundation |
| 2 | Independent secure Registry and OAuth connections | Registry TLS/basic/token/mTLS, Kafka and Registry OAuth, provider probes, capability enforcement | PR 1 |
| 3 | Complete profile creation from external files | Java/librdkafka/Confluent properties, Strimzi Secrets, all supported auth types, matching sandbox exports | PRs 1–2 |
| 4 | Additional native clients | `kcl` and `kafkactl`, direct commands and three shells | PRs 1–3 |
| 5 | Verified first-release candidate | Remaining integration fixtures/matrix, full documentation reconciliation, manual release QA | PRs 1–4 |

Keep schema, lifecycle, rendering, diagnostics, and adapter changes together for
one mechanism. Do not split a PR merely by file type, authentication mechanism,
or documentation. PR 5 closes cross-cutting gaps; earlier PRs must already pass
their own acceptance tests and update current-feature documentation.

Implementation navigation (extend these tests; do not duplicate whole suites):

| PR | Main existing code and test seams |
| --- | --- |
| 1 | `cli.py`, `profiles.py`, `profile_output.py`, `kafka.py`, `session.py`, `runtime.py`, `doctor.py`, `ping.py`, `adapters.py`; `tests/tests_cli.py`, `tests/tests_profiles.py`, `tests/tests_profile_output.py`, `tests/tests_kafka.py`, `tests/tests_session.py`, `tests/tests_runtime.py`, `tests/tests_doctor.py`, `tests/tests_ping.py`, `tests/tests_shells.py` |
| 2 | `registry.py`, schema, `secret_store.py`, shared lifecycle/resolution/probe modules; `tests/tests_registry.py`, `tests/tests_schemas.py`, `tests/tests_credential_mutations.py`, `tests/tests_redaction.py`, client integration fixtures |
| 3 | New focused input parser/normalizer modules feeding `profiles.py`; parser unit tests, CLI tests, `sandbox/__main__.py`, `tests/tests_sandbox.py` |
| 4 | Adapter/rendering/capability seams from PRs 1–2, `shells.py`, `doctor.py`; session, shell, smoke, and PTY contract tests |
| 5 | Platform integration evidence, `scripts/verify_release.py`, `scripts/smoke.py`, docs, examples, packaging/workflow checks |

## PR 1 — Complete Kafka execution, lifecycle, and local diagnostics

### 1.1 Authentication and credential lifecycle through the CLI

**Gap:** `profiles.py` accepts `KafkaAuthInput`, but `cli.py` exposes no auth
options. `edit` without options fails, `remove` has no confirmation, and
`profile_output.py` does not inspect credential availability.

**Architecture:** collect a typed mutation request in `cli.py`, validate it
through `kafka.py` and `profiles.py`, and reuse `credential_mutations.py` and
`reconciliation.py`. Read the profile ID/revision before prompting; collect
input outside the maintenance lock; commit with that expected generation.
Cancellation, keyring failure, or a concurrent edit must preserve the old usable
profile. Reuse the existing immutable-reference staging; do not build another
secret store or transaction mechanism.

**CLI delta:**

| Command | New options / behavior |
| --- | --- |
| `add PROFILE`, `edit PROFILE` | `--auth none\|plain\|scram-sha-256\|scram-sha-512\|mtls`, `--username TEXT`, `--client-certificate-file PATH`, `--client-key-file PATH` |
| `edit PROFILE` | No options opens a field-based interactive editor; omitted options in scripted edits keep existing fields |
| `edit PROFILE` | Repeatable `--replace-secret FIELD`; prompt without echo for text secrets, prompt for a file path for private-key replacement, and prompt without echo for an encrypted key's password |
| `remove PROFILE` | Confirm removal of every profile; new `--force` skips confirmation only; noninteractive removal without it fails |
| `describe PROFILE [-o human\|json\|yaml]` | Add safe per-field credential states `stored`, `missing`, `unavailable`; keep ID/revision and omit references and values |

Required passwords and key passwords are prompted only when creating or
replacing them. The editor offers keep/replace/remove without showing or
prefilling values. Removing a required secret must accompany an auth change
that makes it unnecessary. No standalone `--remove-secret` flag. Reject unknown,
duplicate, or inapplicable replacement fields. Missing credentials are repaired
through the same replacement path. The initial field vocabulary is
`kafka/password`, `kafka/tls/private-key`, and
`kafka/tls/private-key-password`; PR 2 extends it explicitly.

Changing an auth variant discards incompatible old fields in the validated
candidate and retires only its superseded references after commit. Switching
to plaintext requires explicit `--auth none` if currently authenticated, and
removes obsolete TLS material. TLS requires `--transport tls`; do not silently
upgrade/downgrade an explicitly selected transport. Do not read or prompt for
unaffected credentials during metadata-only edits.

Noninteractive input must never hang: if a required secret has no source and
there is no controlling terminal, fail with guidance. When stdin carries an
import in PR 3, any additional prompt uses the controlling terminal, not stdin.
Credential observations may read exact references through the approved store
and immediately discard values; backend selection alone cannot prove `stored`.
They must never create, unlock by changing policy, repair, or enumerate entries.

**Acceptance:** exercise add/edit/remove, no-options editing, canceled prompts,
missing/locked store, missing secret recovery, concurrent prompted edits,
mechanism changes, encrypted and mismatched PEM keys, and redaction in all
output modes. Public CA/certificate values stay in the profile; private keys
and passwords stay in the OS store. Verify realistic PEM sizes on both OSes.

### 1.2 Enable existing adapters and authenticated execution

**Gap:** `session.py` rejects every `requires_secrets` connection before using
the already implemented resolution/rendering helpers.

**Architecture:** load the profile document and revision from one collection,
resolve one immutable connection snapshot, and pass it to rendering and runtime
creation. Enable PLAIN, both SCRAM variants, and mTLS for verified mappings in
Apache/Confluent Kafka, Confluent console, kcat/kafkacat, and Kaskade adapters.
Resolve once per session; do not let shims query SQLite or the keyring again.
Materialize PEM files only in the private runtime and retain existing cleanup.
Java PEM support gates must cover client keys/certificates even when the CA uses
default trust. Keep Java and librdkafka serialization separate.

Build one capability table keyed by adapter, installed client version/library
build, Kafka mechanism, Registry provider/auth, and trust requirements. Direct
commands and shell shims consume the same decisions. A subshell may start for a
valid profile; each invoked adapter rejects unsupported combinations before its
operation. Generate only supported client artifacts rather than blocking an
entire shell on an unused installed client. Unknown custom commands continue to
receive the documented generic session files/environment, without a claim of
automatic adaptation. Preserve connection-override rejection, including
single-token assignments, short forms, and property injection flags.

**CLI delta:** no new `exec` syntax. The existing `exec PROFILE [-- COMMAND...]`
now executes supported authenticated profiles. `KAFKA_*` remains client
configuration; `KANTRIP_*` remains session metadata. Any new file variable must
be explicitly added to `USAGE.md`; do not expose literal secrets by default.

**Acceptance:** real produce/consume/admin operations for every advertised
mechanism, direct commands and Bash/Zsh/Fish, missing client/version refusal,
wrong credentials, no secret in argv, and cleanup after preparation failures,
normal exit, signals, and forced termination. Add a disposable integration
fixture for PLAIN and SCRAM-SHA-256: the current Strimzi sandbox supplies only
SCRAM-SHA-512, mTLS, OAuth, TLS, and plaintext. Do not mark the missing mechanisms
verified using mock tests alone.

### 1.3 Make Kafka ping independent of resource ACLs

**Gap:** `ping.py` rejects authenticated profiles and bases success on
`AdminClient.list_topics()`. Its broker count is a metadata observation, not a
pure authentication result.

**Decision:** retain the Python/librdkafka stack and shared resolver, but use a
bounded connection-state probe. Poll an `AdminClient` with `stats_cb` and
`error_cb`, a short statistics interval within the deadline, and connection
initiation verified against the pinned library (use its documented
`enable.sparse.connections=false` probe setting if needed to initiate a real
connection without an application request). A real configured/learned broker
reaching `UP` after its required TLS/SASL exchange is success. Exclude internal,
logical, and address-less pseudo-brokers; do not require a nonnegative broker ID
because a bootstrap connection can legitimately use `-1`. The pinned library's
[statistics contract](https://github.com/confluentinc/librdkafka/blob/v2.15.0/STATISTICS.md)
and [connection state implementation](https://github.com/confluentinc/librdkafka/blob/v2.15.0/src/rdkafka_broker.c)
support this design; prove callback delivery and authentication sequencing with
real integration tests before enabling it. Do not parse debug logs or access
private C handles.

Do not call topic/group/schema listing or `describe_cluster` to decide success.
Library background discovery can still occur; do not claim zero metadata
traffic. Its authorization errors must not override independently observed
successful authentication. If the pinned binding cannot establish the required
state evidence, treat that as a PR blocker requiring a revised documented
public-library approach; do not silently fall back to ACL-dependent success.

A TCP connection or `ApiVersions` response alone cannot prove SASL success:
Kafka accepts API-version negotiation before authentication. See the
[Kafka authentication sequence](https://kafka.apache.org/26/design/protocol/).
TLS without client authentication proves server identity only; plaintext proves
reachability only. Neither should claim an authenticated user identity. mTLS
proves the configured client exchange, not that a permissive server required
that certificate.

**CLI delta:** add `-q` as the alias for existing `--quiet`; keep
`--timeout SECONDS` (default 5, minimum 0.1). Replace broker/resource counts with
per-service transport and authentication observations. Apply one monotonic
network deadline across Kafka, configured Registry, and any token acquisition,
passing remaining time to each phase. No per-retry fresh timeout. Success means
at least one real broker connection, not health of all brokers.

Return `0` when all configured services meet their required connectivity/auth
proof, `1` for failure or inconclusive authentication. Quiet mode produces no
stdout/stderr on either outcome (including library logs and profile errors).
Report DNS/TCP, TLS, authentication, capability, timeout, and inconclusive
outcomes distinctly. Authentication failure cannot become success because a
public endpoint or TLS socket worked. Add Registry behavior in PR 2.

**Acceptance:** a principal with no topic/group/cluster ACLs can ping; an
invalid password cannot. Test zero-topic clusters, internal pseudo-broker
statistics, unavailable bootstrap plus reachable bootstrap, invalid CA/hostname,
missing certificate, deadline exhaustion, and quiet output. Provision an
authorizer-enabled test broker: the current sandbox Kafka manifest has no
explicit authorizer and cannot prove the no-ACL requirement by itself.

### 1.4 Implement `doctor PROFILE --sessions` and credential diagnostics

**Gap:** `doctor` currently has no positional profile or `--sessions`;
`runtime.py` markers contain no profile identity/revision. Global doctor checks
backend availability and journal counts, not each referenced credential or
certificate expiry. Its unconditional “profiles are executable” claim also
needs replacement with evidence-based checks.

**Architecture:** extend runtime markers with required `profileId` (UUID) and
`profileRevision` (positive integer) from the same snapshot used by the child.
Carry those fields through creation and `mark_running`. Extend the existing
safe scanner to return validated observations plus aggregate counts. Never
open generated client files to determine ownership. Keep `flock` as liveness
proof, the five-minute stale threshold, descriptor-relative inspection, and
bounded scans. Report truncation explicitly. Old development markers are
invalid and never guessed or automatically deleted.

Add profile scope to `run_doctor`; inspect the database read-only, then exact
credential references, certificate/key match, not-before/not-after dates, and
compatible installed clients. Expired/not-yet-valid certificates are errors;
expiry within 30 days is a warning. Display only safe status. Global doctor
checks all profiles; scoped doctor excludes unrelated profile failures while
still reporting shared database/backend/runtime safety failures.

**CLI delta:**

```text
kantrip doctor [--verbose] [PROFILE]
kantrip doctor PROFILE --sessions [--verbose]
kantrip doctor --repair [--verbose]
```

`--sessions` requires `PROFILE`. Reject `PROFILE --repair` and
`--sessions --repair` as usage errors. Repair remains global and unchanged.
Default scoped output summarizes session counts; `--sessions` shows active,
recent, and stale entries with captured revision, age, and state. Session IDs,
PIDs, and resolved runtime paths are verbose-only; generated config paths,
contents, and credential references are never displayed. Recreated profiles
with the same name must not inherit old UUID-owned sessions. Edits do not
rewrite live sessions; report an older captured revision without treating that
alone as corruption. Deleted-profile sessions remain visible globally.

**Acceptance:** two profiles, two simultaneous revisions, remove/recreate same
name, active/recent/stale/invalid/truncated entries, missing/locked credentials,
expired/mismatched certificates, empty and missing database. Ordinary doctor
must create no database, lock file, runtime directory, or credential and must
perform no network request. Test read-only behavior before pending migrations.

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

**Acceptance for 2.1–2.2:** independent Kafka/Registry identities and CAs,
credential rotation/recovery/removal, wrong endpoint trust, invalid secret,
expired fixed token, and no leakage from wrapped HTTP errors. Keep a real Java
and librdkafka session alive across token expiry and demonstrate successful
refresh and later revocation failure. Repeat native Registry refresh for each
advertised client. A successful short ping is not refresh evidence.

### 2.3 Registry ping without resource-list permission dependencies

**Finding:** current `/subjects` can require Confluent `GLOBAL_READ`, and native
Apicurio resource searches depend on configured roles. Replacing them with a
public health endpoint would prove availability, not authentication. Sources:
[Confluent operation authorization](https://docs.confluent.io/platform/current/confluent-security-plugins/schema-registry/authorization/index.html)
and [Apicurio security](https://www.apicur.io/registry/docs/apicurio-registry/3.0.x/getting-started/assembly-configuring-registry-security.html).

**Decision:** introduce explicit provider probe strategies in `ping.py` using
`ssl.SSLContext`, safe Authorization headers, and the common deadline. First
validate a non-resource endpoint: Confluent-compatible `/schemas/types`
([API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html))
and native Apicurio `/users/me`
([API](https://javadoc.io/static/io.apicurio/apicurio-registry-common/3.0.10/io/apicurio/registry/rest/v3/UsersResource.html)).
These are endpoint candidates with version/deployment verification gates, not
a claim that all servers protect them identically. Apicurio ccompat remains a
separate compatibility case; do not rewrite its base URL into native mode.

For each pinned server/security deployment, prove a valid identity without
resource roles succeeds and invalid credentials fail at the selected endpoint.
Validate response shape/content type, and for `/users/me` require non-anonymous
identity when auth is configured. Do not accept an HTML login page as success.
A credentialed 200 from a public endpoint or a token issued by an IdP alone is
insufficient evidence that the Registry accepted the credentials.

Keep a reviewed provider/version probe contract recording whether the endpoint
authenticates, needs a role, or is public. Do not infer this from a product name
or optional version header alone. For a credentialed 200 without an identity
response, make a bounded anonymous control request to the same endpoint within
the same deadline: anonymous 401 followed by credentialed 200 establishes an
authentication gate. An anonymous 200 does not. Do not submit deliberately wrong
passwords during normal ping, or add a user-controlled arbitrary probe URL.
For mTLS use verified handshake evidence from the actual configured connection;
never claim that optional client certificates were required by the server.
If authentication cannot be established for a deployment, report `transport verified; authentication unverified` and return
`1`. For `auth: none`, a validated provider response suffices for connectivity
but says nothing about resource access. Never downgrade an authenticated
profile to this result.

An HTTP 401 is authentication failure. An HTTP 403 proves reachability; label
it authenticated-but-denied only with a verified server contract guaranteeing
that ordering. Such independent authentication evidence can satisfy ping even
when resource authorization is denied. Otherwise it is inconclusive, not a
bad-password claim or a success. Do not grant roles just to make ping green,
and do not fall back to `/subjects`, `/search/artifacts`, `/config`, or `/mode`.
Document that arbitrary proxies/authorization filters can make a universally
role-independent authentication check impossible.

Disable redirects for authenticated and token requests; never forward
Authorization to another origin or downgrade HTTPS. Ignore inherited HTTP
proxy configuration unless an explicitly supported policy is added later.
Bound reads and retries as well as connection time. Preserve partial service
results: Kafka success plus Registry failure is an overall failure with both
outcomes visible in normal mode. Quiet mode remains fully silent.

**Acceptance:** real no-role identities, wrong credentials, public endpoint,
401, verified and ambiguous 403, unsupported endpoint, invalid response,
redirects, revoked/expired tokens, custom CA, and server-required mTLS. Current
sandbox Registry role filters are not evidence of a role-free probe. Add
separate test configurations and record any remaining deployment limitation.

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

**Acceptance:** file and stdin forms, every supported auth source, escaped
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

## PR 5 — Release verification and documentation closure

### 5.1 Integration evidence and release gates

Run and record supported Linux/macOS and Python 3.10–3.14 checks; use actual
macOS Keychain and Linux Secret Service for store integration. Offline tests
remain offline. Keep the existing smoke workflow and PTY shell contract; add
secure integrations separately rather than making ordinary tests require
Docker or keyring access. Pin test clients and retain sanitized evidence for
minimum and representative current versions.

Fill any remaining infrastructure gaps: PLAIN/SCRAM-SHA-256, authorizer-enabled
Kafka with no-ACL principals, no-role and wrong-credential Registry identities,
Registry TLS-only/mTLS, token expiry/revocation, and encrypted/realistic PEM
keys. These fixtures must be ready before their corresponding manual checks.
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
QA does not replace them. Do not publish or tag as part of roadmap implementation.

### 5.2 Synchronize all documentation with the implemented result

Update affected docs in every PR, then audit the whole repository in this final
PR. Replace obsolete claims rather than accumulating “old/new” sections.

| Artifact | Required final review |
| --- | --- |
| `README.md` | Actual first-release capabilities, onboarding, current examples and concise limitations |
| `USAGE.md` | Complete help/option contract, no-echo interactions, imports, independent trust, scoped doctor/sessions, ping proof/exit status, exact child environment |
| `COMPATIBILITY.md` | Separate CLI/version, Kafka protocol/auth, Registry provider/auth, input-format, and generated-format matrices; supported/unsupported/conditional cells with evidence and minimum tested version |
| `ARCHITECTURE.md`, `images/*.svg` | Actual resolvers, mutation flow, capability checks, native refresh, session revision attribution, probe state and input pipeline |
| `THREAT_MODEL.md`, `SECURITY.md` | Implemented controls versus residual risk; token request/redirect handling, imported documents, keyring limits, no-ACL/no-role proof limits |
| `AGENT.md`, `DEVELOPMENT.md` | Durable final contracts, first-release boundary, fixture/integration workflows, supported platforms and dependencies |
| `MANUAL_TESTING.md` | Move completed runnable QA scenarios here, remove superseded expectations, keep setup/actions/results and release checklist entry point |
| `RELEASE_CHECKLIST.md` | Link manual first-release gate and compatibility evidence; distinguish first release from upgrades of published versions |
| `schemas/`, `examples/` | Implemented schema only, synthetic examples covering secure profiles and supported input formats; no secrets or live references |
| `scripts/`, `sandbox/`, `.github/`, `pyproject.toml` | Help, fixtures, smoke, release packaging inclusion, workflow/template references and version pins match the final contract |
| `MVP.md` | Delete completed PRs, retain only genuinely unfinished work; preserve manual QA by moving it before removal |

Compatibility must distinguish schema acceptance from CLI creation, session
execution, ping, and import. Document Java PEM gates, actual linked librdkafka
and Registry libraries, kcat's Avro-only Registry decoding, Kaskade's native
Apicurio support, unsupported provider/mechanism combinations, and file formats
that are output-only. Do not label JSON/YAML `describe` as an import/export
format. Include plaintext, verified TLS, PLAIN, both SCRAM mechanisms, mTLS,
OAuth/OAUTHBEARER, Registry basic/fixed bearer, Java/librdkafka properties,
Confluent-generated properties, and Strimzi Secret JSON/YAML explicitly.

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
  against PR 1's documented fixture endpoints; do not reuse port 9094 for these.
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

- [ ] Follow PR 1's authorizer-enabled fixture instructions to create
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
    --registry-url https://localhost:8082 --registry-auth basic \
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

- [ ] With PR 2's no-role fixtures run `kantrip ping PROFILE --timeout 5`, then
  use the fixture's resource-list request to demonstrate denied authorization.
  Verify ping succeeds only where authentication is independently proven.
  Repeat invalid credentials, anonymous/public endpoint, ambiguous 403, and
  wrong-origin redirect. Expect `1`/unverified for insufficient proof and no
  credential forwarding. Test Apicurio ccompat separately from native mode.
  Record any deployment limitation in compatibility instead of adding a role.

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

### QA 10 — Store failures, repair, cleanup, and release sign-off

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
