# Kantrip Usage

Kantrip is a pre-release CLI for plaintext or verified TLS profiles and scoped
sessions:

```bash
kantrip add local
kantrip doctor
kantrip list
kantrip describe local
kantrip ping local
kantrip exec local -- kcat -L
```

Planned capabilities are tracked separately in the [MVP roadmap](MVP.md).

Kantrip is not a persistent process manager, a global context selector, or a
replacement for Kafka clients. Its responsibility ends at storing profiles,
resolving the connection material supported by the installed release,
generating the correct temporary configuration, and supervising active
execution. This pre-release CLI currently creates and executes plaintext and
server-authenticated TLS profiles with `auth.type: none`. The connection core
already validates and renders PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and mTLS,
while authenticated input commands, adapters, and `ping` remain tracked in the
MVP roadmap. Authenticated profiles fail before a child or network client starts.

## Local diagnostics

Run `kantrip doctor` to inspect the installation without opening a Kafka
connection:

```bash
kantrip doctor
```

Doctor groups local checks under System, Profiles, Credentials, Session, and
Clients, names missing commands, and summarizes health. It validates the profile
database, approved OS credential backend, Registry support, reconciliation
state, active-session state, recoverable runtime artifacts, and installed
clients. Use `--verbose` for paths, backend identity, and individual checks.

Missing optional clients or a first-run profile database produce warnings. An
invalid database, unsupported registry settings, or inconsistent session state
exits with status 1. Doctor never contacts Kafka or a registry.

Profile-dependent commands automatically apply known SQLite migrations. A
normal `doctor` run only reports pending work; it does not create or modify
storage. Use `kantrip doctor --repair` for an explicit maintenance pass that
migrates the profile database, reconciles exact pending credential cleanup,
removes validated stale sessions, and then runs the diagnostics again. Kantrip
has no separate migration or cleanup command.
Migration backups include their UTC creation time and a unique suffix, so later
migrations preserve earlier recovery points. Unreleased databases without
migration history are unsupported and must be recreated.

## Kafka and registry connectivity

Check Kafka and, when configured, registry connectivity:

```bash
kantrip ping local
```

`ping` uses Confluent's Admin client for Kafka metadata with the profile's
plaintext or verified TLS transport. It requests `/subjects`
from a Confluent-compatible registry or `/search/artifacts` from native
Apicurio, reporting broker and provider-specific resource counts. It needs no
external CLI and defaults to a five-second timeout, configurable with
`--timeout SECONDS`. Colored terminals animate checks; plain output uses
`[running]`. Failures include a sanitized message from the underlying client or
transport exception.

These current probes can depend on resource permissions. A failure can be an
authorization rejection even when the service is reachable. Success does not
prove topic, group, schema, or administrative access. The future connection/auth
probe is specified separately in [MVP.md](MVP.md); it is not implemented yet.

For scripts that need only the exit status, suppress all output with:

```bash
kantrip ping local --quiet
```

## First profile

Add a plaintext profile using the default local broker:

```bash
kantrip add local
```

Use TLS with each client's default trust store for a broker whose certificate
is issued by a trusted public CA:

```bash
kantrip add production \
  --bootstrap-servers kafka.example.com:9093 \
  --transport tls
```

For a private CA, pass a PEM certificate bundle. Kantrip validates the bundle,
copies the public certificates into the profile, and later materializes them
only in the private session directory:

```bash
kantrip add production-private-ca \
  --bootstrap-servers kafka.internal.example:9093 \
  --transport tls \
  --ca-file ./cluster-ca.pem
```

The CA source must be a regular UTF-8 PEM file of at most 1 MiB. Certificate and
hostname verification cannot be disabled. `kcat`, `kafkacat`, and Kaskade use
the PEM bundle directly. Java Kafka commands require Apache Kafka 2.7+ or
Confluent Platform 6.1+ for native PEM trust-store support. Kantrip checks the
installed Java client version before the Kafka operation and reports an
actionable error when it cannot prove support. Kafka 2.6 and Confluent Platform
6.0 can still use TLS with their default trust stores.

Choose one or more broker addresses when needed:

```bash
kantrip add development \
  --bootstrap-servers kafka-1.example.com:9092,kafka-2.example.com:9092 \
  --description 'Development cluster' \
  --label environment=development \
  --label owner=platform \
  --registry-url http://registry.example.com:8081
```

`--registry-url` defaults to the Confluent provider. Select native Apicurio
explicitly and pass its Core Registry API v3 endpoint:

```bash
kantrip add development-apicurio \
  --bootstrap-servers kafka-1.example.com:9092,kafka-2.example.com:9092 \
  --registry-provider apicurio \
  --registry-url http://registry.example.com:8080/apis/registry/v3
```

`--registry-provider` without `--registry-url` is invalid.

`add` creates the database when needed and never overwrites a profile. `-l` is
the short form of repeatable `--label KEY=VALUE`. Edit explicit fields without
changing the profile identity:

```bash
kantrip edit development \
  --bootstrap-servers kafka-3.example.com:9092,kafka-4.example.com:9092 \
  --description 'Shared development cluster' \
  --label environment=development \
  --registry-url http://registry.example.com:8081
```

`edit --transport tls` enables TLS with the client's default trust store.
`edit --ca-file PATH`
replaces the custom CA for an existing TLS profile; combine it with
`--transport tls` when upgrading a plaintext profile. Switching back to default
trust is an explicit `edit --default-trust` operation and does not disable TLS.
Switching to `--transport plaintext` removes the stored TLS configuration.
`--default-trust` and `--ca-file` are mutually exclusive.

`edit` adds or updates labels and can add a Registry to a profile that has none.
When only `--registry-url` is supplied, the new Registry defaults to Confluent.
Use `--clear-description`, repeatable `--remove-label KEY`, or
`--remove-registry` for explicit removal. Omitting an option preserves its
current value; calling `edit` without any change fails safely.

Remove one with:

```bash
kantrip remove development
```

`kantrip list` succeeds without rows when no profiles exist. Kantrip validates
every profile whenever it reads or updates the database.

Human list output includes labels. Filter by an exact pair with `-l` or
`--label`; repeated filters use logical AND:

```bash
kantrip list --label environment=development --label owner=platform
```

Use `--output json` or `--output yaml` (`-o` for short) for a stable structured
summary. On a colored TTY, Kantrip applies syntax highlighting; `--no-color` or
output redirection emits plain machine-readable content without ANSI escapes.
Structured empty results are an empty sequence.

## Profile workflow

```bash
kantrip list
kantrip describe local
kantrip describe local --output yaml
kantrip exec local -- kcat -L
```

`describe` presents Rich sections by default. JSON and YAML are safe
machine-readable observations rather than profile export documents. They omit
arbitrary client properties, secret values, and internal credential references,
and include the profile's current database revision.

Without a command, `kantrip exec PROFILE` opens a supervised Bash, Zsh, or Fish
subshell from `SHELL`, falling back to Bash when it is unset. Other shells fail
with an actionable error:

```bash
kantrip exec local
kantrip current
kcat -L
kafka-topics --list
exit
```

Kantrip never exports the profile to the parent shell. Nested sessions and
detached children are unsupported; exit before starting another session.

`kantrip current` prints the active profile or reports that none is selected. It
is equivalent to reading `KANTRIP_PROFILE`.

The subshell preserves normal startup files and history. It neutralizes aliases,
functions, and Fish abbreviations that shadow adapters, keeps the shim directory
first on `PATH`, and removes these changes on exit.

## Session supervision and recovery

One-off commands run in their own POSIX process group. Interactive Bash, Zsh,
and Fish subshells run through a PTY so terminal input, Ctrl-C, job control, and
window resizing behave normally. Kantrip preserves normal child exit codes and
maps signal termination to `128 + signal number`.

Kantrip forwards SIGINT, SIGTERM, and SIGHUP to the managed process boundary. A
second termination signal or a child that remains alive for five seconds causes
escalation to SIGKILL. Terminal state and signal handlers are restored when the
supervisor exits. Processes that deliberately daemonize or create a new POSIX
session remain unsupported.

Session files live under `$XDG_RUNTIME_DIR/kantrip/sessions` when the configured
runtime directory is private and user-owned. Otherwise Kantrip uses a private
`kantrip-<uid>/sessions` directory below the operating-system temporary root.
Every session holds a liveness lock; PIDs are metadata and are not used alone to
decide whether a session is active.

Before `exec`, Kantrip inspects at most 256 direct runtime entries and removes
validated unlocked sessions older than five minutes. Inspect the complete local
state without modifying it, or explicitly repair safe deterministic issues, with:

```bash
kantrip doctor
kantrip doctor --repair
```

Repair applies known database migrations and removes every validated stale
session. It refuses malformed markers, symlinks, unsafe permissions, wrong
owners, and paths outside the runtime root. Active and recent sessions remain
untouched. Runtime paths appear only with `--verbose`.

## Displaying the active profile in your prompt

Prompt integrations should read `KANTRIP_PROFILE`, which exists only inside a
`kantrip exec PROFILE` subshell. They need not invoke Kantrip repeatedly.

Configure only the integration that renders your prompt:

- Use **Starship** if Starship controls your prompt.
- Use **Powerlevel10k** if Powerlevel10k controls your prompt, even when it is
  installed through Oh My Zsh.
- Use **Oh My Zsh** for a theme that uses the standard `PROMPT` variable.

### Choose a display style

Examples use `kantrip:PROFILE`; the alternatives below provide compact prefixes:

| Style | Reference | Requirement |
| --- | --- | --- |
| Magic-wand emoji | 🪄 | An emoji fallback font |
| Kantrip magic staff | <img src="images/nf-md-magic-staff.svg" alt="Nerd Font magic staff glyph" width="48"> | MesloLGS NF or another Nerd Font with `nf-md-magic_staff` (`U+F1844`) |
| Apache Kafka | <img src="images/nf-md-apache-kafka.svg" alt="Nerd Font Apache Kafka glyph" width="48"> | MesloLGS NF or another Nerd Font with `nf-md-apache_kafka` (`U+F100F`) |

GitHub may render Nerd Font characters as boxes; the images show their terminal
appearance. See the [Nerd Fonts cheat sheet](https://www.nerdfonts.com/cheat-sheet)
for more glyphs.

### Starship

Add a custom module to `~/.config/starship.toml`:

```toml
[custom.kantrip]
command = 'printf %s "$KANTRIP_PROFILE"'
when = 'test -n "$KANTRIP_PROFILE"'
format = '[kantrip:$output]($style) '
style = 'bold purple'
```

For a compact prefix, replace `format` with one of these alternatives.

Magic-wand emoji:

```toml
format = '[🪄 $output]($style) '
```

Kantrip magic staff (`nf-md-magic_staff`):

```toml
format = '[󱡄 $output]($style) '
```

Apache Kafka (`nf-md-apache_kafka`):

```toml
format = '[󱀏 $output]($style) '
```

Starship's default prompt includes custom modules. If you define a custom global
`format`, add `${custom.kantrip}` where the profile should appear.

### Oh My Zsh

For an Oh My Zsh theme that uses the standard `PROMPT` variable, add this after
`source $ZSH/oh-my-zsh.sh` in `~/.zshrc`:

```zsh
kantrip_prompt_info() {
  [[ -n ${KANTRIP_PROFILE:-} ]] || return
  print -P -n '%F{magenta}kantrip:%f%F{cyan}'
  print -rn -- "$KANTRIP_PROFILE"
  print -P -n '%f '
}

setopt prompt_subst
PROMPT='$(kantrip_prompt_info)'"$PROMPT"
```

For a compact prefix, replace the first `print` command with one alternative.

Magic-wand emoji:

```zsh
print -P -n '%F{magenta}🪄 %f%F{cyan}'
```

Kantrip magic staff (`nf-md-magic_staff`):

```zsh
print -P -n '%F{magenta}󱡄 %f%F{cyan}'
```

Apache Kafka (`nf-md-apache_kafka`):

```zsh
print -P -n '%F{magenta}󱀏 %f%F{cyan}'
```

Themes that replace `PROMPT` after this code may need the snippet moved to the
end of `~/.zshrc`.

### Powerlevel10k

Define a custom segment in `~/.p10k.zsh`:

```zsh
function prompt_kantrip() {
  [[ -n ${KANTRIP_PROFILE:-} ]] || return
  p10k segment -f 5 -t "kantrip:${KANTRIP_PROFILE}"
}
```

For a compact prefix, replace the `p10k segment` line with one alternative.

Magic-wand emoji:

```zsh
p10k segment -f 5 -t "🪄 ${KANTRIP_PROFILE}"
```

Kantrip magic staff (`nf-md-magic_staff`):

```zsh
p10k segment -f 5 -t "󱡄 ${KANTRIP_PROFILE}"
```

Apache Kafka (`nf-md-apache_kafka`):

```zsh
p10k segment -f 5 -t "󱀏 ${KANTRIP_PROFILE}"
```

Then add `kantrip` to either `POWERLEVEL9K_LEFT_PROMPT_ELEMENTS` or
`POWERLEVEL9K_RIGHT_PROMPT_ELEMENTS` in the same file.

### Verify the prompt

Start a new shell or reload the relevant prompt configuration, then run:

```bash
kantrip exec local
```

The profile should appear in the subshell and disappear after `exit`.

## Supported command-line tools

Kantrip configures supported tools inside `kantrip exec` and blocks profile
overrides. It reports missing clients but does not install them.

### kcat

Each session sets `KCAT_CONFIG` to a private librdkafka properties file.
Interactive sessions add a pass-through executable to resist startup-time
`PATH`, alias, and function changes without configuration arguments.

```bash
kantrip exec local -- kcat -L
kantrip exec local -- kcat -C -t orders
kantrip exec local -- kcat -P -t orders
kantrip exec local -- kcat -C -t avro-orders -s value=avro
```

For kcat 1.7+, `-s avro`, `-s key=avro`, and `-s value=avro` inject a
Confluent-compatible registry URL through `-r`. Native Apicurio profiles are not
supported by kcat. Kantrip rejects `-F`, `-r`, and
`-X schema.registry.url=...` overrides. Other modes need no registry profile.

### Apache Kafka and Confluent Kafka commands

Kantrip recognizes Apache Kafka's `.sh` commands and Confluent Platform's
unsuffixed equivalents:

```bash
kantrip exec local -- kafka-topics --list
kantrip exec local -- kafka-console-producer --topic orders
kantrip exec local -- kafka-console-consumer.sh --topic orders --from-beginning
kantrip exec local -- kafka-consumer-groups --list
kantrip exec local -- kafka-configs --describe --entity-type topics --entity-name orders
kantrip exec local -- kafka-acls --list
kantrip exec local -- kafka-broker-api-versions
```

Kantrip injects `--bootstrap-server` and a private Java client-properties file.
Console consumers receive `--consumer.config`, console producers receive
`--producer.config`, and administrative commands receive `--command-config`.
Injected and legacy connection options cannot override the profile. Use `.sh`
with Apache Kafka archives and unsuffixed names with Confluent Platform.

Interactive sessions create temporary shims, so commands work without repeating
`kantrip exec`:

```bash
kantrip exec local
kafka-topics --list
kafka-console-consumer --topic orders --from-beginning
exit
```

These shims follow the session behavior above and disappear on exit; Kantrip
installs no CLI or persistent aliases.

### Additional Confluent Schema Registry console clients

Confluent Platform supplies six unsuffixed Avro, JSON Schema, and Protobuf
producer and consumer scripts:

```bash
kantrip exec local -- kafka-avro-console-producer --topic orders \
  --property value.schema='{"type":"string"}'
kantrip exec local -- kafka-avro-console-consumer --topic orders --from-beginning
kantrip exec local -- kafka-json-schema-console-producer --topic orders \
  --property value.schema='{"type":"string"}'
kantrip exec local -- kafka-json-schema-console-consumer --topic orders --from-beginning
kantrip exec local -- kafka-protobuf-console-producer --topic orders \
  --property value.schema='syntax = "proto3"; message Order { string id = 1; }'
kantrip exec local -- kafka-protobuf-console-consumer --topic orders --from-beginning
```

Each receives the profile's bootstrap servers, private Java client file, and
`schema.registry.url`. Connection and endpoint overrides are rejected in direct
and interactive sessions.

They require a plain registry with the Confluent provider. Missing registries
and native Apicurio profiles fail before launch.

### Kaskade

Kantrip passes a private INI to Kaskade `admin` and `consumer` through
`--config-file`:

```bash
kantrip exec local -- kaskade admin
kantrip exec local -- kaskade consumer --topic orders
kantrip exec local -- kaskade consumer --topic avro-orders --earliest -v registry
```

The first command becomes:

```bash
kaskade admin --config-file /tmp/kantrip-SESSION/kaskade.ini
```

The default INI contains Kafka properties. Registry deserialization (`-k
registry` or `-v registry`) selects a second INI with a provider-specific
`[registry]` section. Kantrip rejects `-b`/`--bootstrap-servers`, `--kafka`,
`--config-file`, and `--registry` overrides. Interactive shims behave
identically.

Kaskade supports Avro, JSON Schema, and Protobuf decoding with Confluent Schema
Registry and native Apicurio Registry through this adapter. Native Apicurio uses
its default `contentId` framing because Kantrip currently configures only the
registry URL. Kantrip sets no Kaskade-specific environment variable.

## Profile storage

The [profile schema](https://github.com/sauljabin/kantrip/blob/main/schemas/profile.schema.json)
validates every profile stored in Kantrip's private SQLite database; see the
[synthetic profile document](https://github.com/sauljabin/kantrip/blob/main/examples/profile.json).
Use Kantrip commands to modify the database instead of editing it directly.

Database lookup order is:

1. `KANTRIP_DATABASE`.
2. `$XDG_DATA_HOME/kantrip/profiles.db`.
3. `~/.local/share/kantrip/profiles.db`.

The database directory is private to the current user (`0700`), the database is
`0600`, and writes use SQLite transactions. A missing database is created by
the first mutation; read-only commands do not create it.

Profiles support plaintext or server-authenticated TLS Kafka connections and
one optional registry connection. TLS uses each client's default trust store
unless a custom PEM CA supplied through `--ca-file` is validated and copied
into the profile.
Java adapters version-gate custom PEM trust stores at Kafka 2.7 or Confluent
Platform 6.1; librdkafka adapters consume the same profile without that Java
version constraint.
The `provider` is explicit in every stored profile. When `--registry-url` is
supplied without `--registry-provider`, `kantrip add` and `kantrip edit` select
and persist Confluent:

```json
{
  "registry": {
    "provider": "confluent",
    "schema.registry.url": "http://localhost:8081"
  }
}
```

For native Apicurio:

```json
{
  "registry": {
    "provider": "apicurio",
    "apicurio.registry.url": "http://localhost:8082/apis/registry/v3"
  }
}
```

The two providers are mutually exclusive, and `provider` is required in stored
profiles. Kafka PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and mTLS shapes are valid
only over TLS and contain references instead of secret values; Registry TLS and
authentication remain schema-invalid. Profile documents accept no arbitrary
Java or librdkafka property maps; Kantrip renders only its typed connection
fields. Registry property names follow the official
[Confluent](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)
and [Apicurio](https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-configuring-kafka-client-serdes.html)
serializer and deserializer configuration.

Apicurio's Confluent-compatible API is not native Apicurio mode. Configure its
`ccompat` endpoint as Confluent to use Confluent console clients, kcat Avro, or
Kaskade with Confluent framing:

```json
{
  "registry": {
    "provider": "confluent",
    "schema.registry.url": "http://localhost:8082/apis/ccompat/v7"
  }
}
```

Create two profiles with the same Kafka connection when both native and
Confluent-compatible Apicurio endpoints are needed.

Profile documents have no application-version field; each release's bundled
profile schema is authoritative. The SQLite schema has a separate internal
version and unsupported database versions fail closed.

## Application environment from `kantrip exec`

Kantrip exposes settings only to supervised children. Kafka has no
cross-language environment standard, so applications must opt into the generic
`KAFKA_*` values below; `KANTRIP_*` is reserved for session metadata. Adapters
may instead pass generated client files directly.

### Precedence and inherited sandbox variables

For a direct child, Kantrip copies the exported parent environment, overwrites
its documented Kafka/config/session variables with the selected profile, then
removes both providers' Registry URL/config variables and sets only the chosen
provider's pair. Without a Registry, neither pair is present. The parent shell
and any sourced file remain unchanged.

This is not a purge of every `KAFKA_*` or credential variable. Currently,
`KAFKA_SCRAM_PASSWORD`, `KAFKA_OAUTH_CLIENT_SECRET`, `APICURIO_CLIENT_SECRET`,
`SCHEMA_REGISTRY_BASIC_PASSWORD`, and `KEYCLOAK_ADMIN_PASSWORD`, for example,
remain inherited if exported. They are not profile inputs and cannot supply a
missing stored credential. A custom child that independently reads them can
still use them, so no universal precedence over custom applications is promised.

`sandbox/.state/credentials.env` contains shell assignments, not `export`
commands. `. sandbox/.state/credentials.env` defines shell variables; new names
are not exported unless `set -a` is enabled. Names already marked for export
remain exported when assigned again. `set +a` stops automatic export of later
assignments; it does not unexport existing variables. The manual sandbox checks
only need parent-shell expansion of public paths/identifiers and already use
private native-client files for credentials, so they do not need blanket export.

Load this file before `kantrip exec`, never inside its subshell. Supported
interactive shells run user startup files after receiving the initial child
environment. Current startup handling restores adapter shims/PATH, but does not
restore every connection variable changed by `.bashrc`, `.zshrc`, or Fish
configuration. Avoid changing those variables in startup files during current
manual checks. The stronger environment precedence/scrubbing contract is pending
in [MVP PR 1](MVP.md#16-give-the-selected-profile-precedence-over-inherited-connection-state).

### Kafka variables

| Variable | Meaning |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated broker addresses |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` or `SSL`, matching the selected profile |
| `KAFKA_JAVA_CONFIG_FILE` | Generated Java Kafka properties path |
| `KAFKA_LIBRDKAFKA_CONFIG_FILE` | Generated librdkafka properties path |
| `KCAT_CONFIG` | Generated librdkafka properties path read natively by kcat |

### Registry variables

Only the selected provider's variables are present. Their generated properties
file contains the provider's official serializer/deserializer URL property.

| Variable | Meaning |
| --- | --- |
| `SCHEMA_REGISTRY_URL` | Confluent-compatible registry URL |
| `SCHEMA_REGISTRY_CONFIG_FILE` | Generated file containing `schema.registry.url` |
| `APICURIO_REGISTRY_URL` | Native Apicurio Core Registry API v3 URL |
| `APICURIO_REGISTRY_CONFIG_FILE` | Generated file containing `apicurio.registry.url` |

### Kantrip session metadata

| Variable | Meaning |
| --- | --- |
| `KANTRIP_PROFILE` | Selected profile display name |
| `KANTRIP_SESSION_ID` | Opaque session identifier |
| `KANTRIP_SESSION_DIR` | Private temporary session directory |

Prefer native generated files, fall back to these variables, and never log the
complete environment or properties.

## Output and color

Commands write results to stdout and diagnostics to stderr. Styling is disabled
when:

- `--no-color` is supplied globally or after a subcommand.
- `NO_COLOR` is present in the environment.
- `TERM=dumb`.
- The destination stream is not a terminal.

The global and command-local forms are equivalent:

```bash
kantrip --no-color list
kantrip list --no-color
```

`kantrip list` shows profile endpoints and labels; `kantrip describe PROFILE`
uses sectioned human output. Both also accept JSON or YAML through `--output`.
Kantrip syntax-highlights structured output on colored TTYs, while `--no-color`,
`NO_COLOR`, `TERM=dumb`, and non-TTY streams remain plain and machine-readable.
Human output remains readable without ANSI color.

Values are classified before reaching Rich. Styling neither changes exit status
nor carries essential information.
