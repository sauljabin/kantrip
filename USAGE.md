# Kantrip Usage

Kantrip is in pre-release development. The current CLI can manage plaintext
profiles and run profile sessions:

```bash
kantrip add local
kantrip doctor
kantrip list
kantrip show local
kantrip ping local
kantrip exec local -- kcat -L
```

Planned capabilities are tracked separately in the [MVP roadmap](MVP.md).

## Local diagnostics

Run `kantrip doctor` to inspect the installation without opening a Kafka
connection:

```bash
kantrip doctor
```

Doctor validates the resolved configuration against the bundled schema, checks
that profile IDs are unique, warns about unsafe file permissions, verifies an
active session and its adapter path, and discovers Kantrip, the interactive
shell, kcat, Apache Kafka commands, and Kaskade on `PATH`. Missing optional
clients and a missing first-run configuration are warnings; invalid
configuration or inconsistent active-session state makes the command exit with
status 1. Doctor performs only local checks.

## Kafka and Schema Registry connectivity

Check Kafka and, when configured, Schema Registry connectivity:

```bash
kantrip ping local
```

`ping` uses the Confluent Kafka Admin client to request cluster metadata. When
the profile contains `schemaRegistry`, it also requests the registry's
`/subjects` endpoint and reports the subject count. It does not require an
external Kafka CLI. Both requests use a five-second timeout by default; set a
different limit with `--timeout SECONDS`.

## First-run configuration

Add a plaintext profile using the default local broker:

```bash
kantrip add local
```

Choose one or more broker addresses when needed:

```bash
kantrip add development \
  --bootstrap-server kafka-1.example.com:9092 \
  --bootstrap-server kafka-2.example.com:9092 \
  --schema-registry-url http://registry.example.com:8081
```

`add` follows the documented configuration lookup order, creates the file when
necessary, and refuses to overwrite an existing profile. Remove a profile with:

```bash
kantrip remove development
```

`kantrip list` prints no profile rows and exits successfully when configuration
does not exist or contains no profiles. Existing files are validated automatically
whenever Kantrip reads or updates them.

## Profile workflow

```bash
kantrip list
kantrip show local
kantrip exec local -- kcat -L
```

Omitting the command after `kantrip exec PROFILE` opens an interactive supervised
Bash, Zsh, or Fish subshell using `SHELL`. When `SHELL` is unset, Kantrip selects
an installed Bash. Other shells are rejected with an actionable error:

```bash
kantrip exec local
kantrip current
kcat -L
kafka-topics --list
exit
```

Kantrip never exports a selected profile into the parent shell. Background or
detached child processes are not supported because they can outlive the temporary
session. Sessions cannot be nested: exit the current `kantrip exec` subshell
before starting another one.

`kantrip current` prints the active profile name inside the session. Outside a
session it reports that no profile is active. This is the command equivalent of
reading `KANTRIP_PROFILE` directly.

The subshell loads the user's normal startup configuration and keeps its normal
history file. Kantrip then removes aliases, functions, and Fish abbreviations
that shadow supported client names, restores its temporary adapter directory at
the front of `PATH`, and refreshes the shell's command lookup. These changes are
limited to the child shell and disappear on `exit`.

## Displaying the active profile in your prompt

Prompt integrations should read `KANTRIP_PROFILE`. It is available only inside
an interactive shell opened by `kantrip exec PROFILE`, and it disappears when
that session exits. Prompt code does not need to invoke Kantrip repeatedly.

Configure only the integration that renders your prompt:

- Use **Starship** if Starship controls your prompt.
- Use **Powerlevel10k** if Powerlevel10k controls your prompt, even when it is
  installed through Oh My Zsh.
- Use **Oh My Zsh** for a theme that uses the standard `PROMPT` variable.

### Choose a display style

The examples below use `kantrip:PROFILE` by default. You can keep that label or
replace the indicated line with one of these compact alternatives:

| Style | Reference | Requirement |
| --- | --- | --- |
| Magic-wand emoji | 🪄 | An emoji fallback font |
| Kantrip magic staff | <img src="images/nf-md-magic-staff.svg" alt="Nerd Font magic staff glyph" width="48"> | MesloLGS NF or another Nerd Font with `nf-md-magic_staff` (`U+F1844`) |
| Apache Kafka | <img src="images/nf-md-apache-kafka.svg" alt="Nerd Font Apache Kafka glyph" width="48"> | MesloLGS NF or another Nerd Font with `nf-md-apache_kafka` (`U+F100F`) |

The Nerd Font characters may appear as boxes in GitHub code blocks because the
site does not load Nerd Fonts. The images above show how they look in a
compatible terminal. Browse the
[Nerd Fonts cheat sheet](https://www.nerdfonts.com/cheat-sheet) for more glyphs.

### Starship

Add a custom module to `~/.config/starship.toml`:

```toml
[custom.kantrip]
command = 'printf %s "$KANTRIP_PROFILE"'
when = 'test -n "$KANTRIP_PROFILE"'
format = '[kantrip:$output]($style) '
style = 'bold purple'
```

To use a compact prefix, replace the `format` line with exactly one of the
following alternatives.

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

To use a compact prefix, replace the first `print` command in
`kantrip_prompt_info` with exactly one of the following alternatives.

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

To use a compact prefix, replace the `p10k segment` line with exactly one of the
following alternatives.

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

The active profile should appear in the prompt inside the new subshell. Run
`exit` and confirm that it disappears when you return to the parent shell.

## Supported command-line tools

Kantrip configures supported Kafka tools inside `kantrip exec` while preventing
command-line options from overriding the selected profile.

### kcat

Kantrip generates a private librdkafka properties file for each session and sets
`KCAT_CONFIG` to its path. kcat reads this variable natively, so Kantrip does not
create an alias or add configuration arguments. Interactive sessions use a
temporary pass-through executable only to keep the installed kcat ahead of
startup-time `PATH`, alias, and function changes.

```bash
kantrip exec local -- kcat -L
kantrip exec local -- kcat -C -t orders
kantrip exec local -- kcat -P -t orders
```

Kantrip reports a command-not-found error when an explicit executable is missing.
It does not install external tools. The kcat `-F` option is rejected because it
would override the selected profile.

The profile schema accepts only `transport: plaintext` with `auth.type: none`,
so every valid profile can be executed.

### Apache Kafka and Confluent Kafka commands

Apache Kafka's Unix archives use `.sh` command names. Confluent Platform ships
the equivalent commands without `.sh`. Kantrip recognizes both naming forms for
the shared Kafka tools:

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
Supplying an injected or legacy connection option explicitly is rejected because
it could override the selected profile. Use the `.sh` name with an Apache Kafka
archive and the unsuffixed name with Confluent Platform. Kantrip also recognizes
either form when another package exposes it.

An interactive session creates temporary executable shims for whichever variants
are installed before the session starts, so the same commands work without
repeating `kantrip exec`:

```bash
kantrip exec local
kafka-topics --list
kafka-console-consumer --topic orders --from-beginning
exit
```

The shims exist only inside that session and are removed on exit. Kantrip does
not install the Kafka CLI or create persistent shell aliases. Bash and Zsh use a
private startup file that loads the user's normal startup file; Fish uses an init
command after its normal configuration loads. Kantrip then clears child-shell
aliases, functions, or abbreviations for supported client names, restores the
shim directory to the front of `PATH`, and refreshes command lookup. This keeps
Oh My Zsh, Homebrew, Fish configuration, and other startup-time path changes from
bypassing the adapters. Each shell continues to use its normal user history
location instead of the temporary session directory.

### Additional Confluent Schema Registry console clients

In addition to its unsuffixed versions of the shared Kafka commands, Confluent
Platform supplies Avro, JSON Schema, and Protobuf producer and consumer scripts.
These six commands are unsuffixed in Confluent Platform's `bin` directory:

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

Each command receives the selected Kafka bootstrap servers, the private Java
client file, and `schema.registry.url`. Explicit connection flags and
`bootstrap.servers` or `schema.registry.url` properties are rejected. The same
behavior is available through Bash, Zsh, and Fish interactive sessions.

These commands require the plain `schemaRegistry` profile section documented
below. A missing section, authentication, HTTPS, or TLS metadata produces an
actionable error before the console client starts.

### Kaskade

Kaskade's current `admin` and `consumer` commands accept an explicitly selected
INI client file. Kantrip generates that private file and inserts
`--config-file` after the Kaskade command:

```bash
kantrip exec local -- kaskade admin
kantrip exec local -- kaskade consumer --topic orders
```

For example, the first command is prepared conceptually as:

```bash
kaskade admin --config-file /tmp/kantrip-SESSION/kaskade.ini
```

The temporary INI contains the selected profile's Kafka client properties.
Explicit `-b`/`--bootstrap-servers`, `--kafka`, and `--config-file` options are
rejected because they could override that profile. Interactive sessions expose a
temporary `kaskade` shim using the same behavior.

This adapter uses Kaskade's current CLI contract and does not set a
Kaskade-specific environment variable.

## Profile configuration

Profile metadata is YAML validated against
[`schemas/profile.schema.json`](https://github.com/sauljabin/kantrip/blob/main/schemas/profile.schema.json).
A synthetic example is available at
[`examples/config.yaml`](https://github.com/sauljabin/kantrip/blob/main/examples/config.yaml).

Configuration lookup order is:

1. `KANTRIP_CONFIG`.
2. `$XDG_CONFIG_HOME/kantrip/config.yaml`.
3. `~/.config/kantrip/config.yaml`.

Profile YAML stores plaintext broker metadata only. Authentication, TLS, and
encrypted Kafka connections are rejected by the current schema. A profile may
also contain a Schema Registry connection:

```yaml
schemaRegistry:
  url: http://localhost:8081
  auth:
    type: none
```

Only this plain, unauthenticated registry connection is executable today.
Authenticated and TLS-secured registry metadata may be represented for future
support, but Schema Registry-aware commands reject it before launch.

The configuration document does not contain a separate version field. The
schema bundled with each Kantrip application release is authoritative, so
upgrades do not require rewriting a version value in every configuration.
If an early alpha configuration reports `unknown field: version`, remove its
top-level `version: 1` line. See `MVP.md` for the complete alpha cleanup note.

## Application environment from `kantrip exec`

Kantrip publishes connection settings only to the supervised child process and
its descendants. The variables Kantrip may set are listed below.

Kafka clients share configuration-property names but do not define one
cross-language environment-variable standard. Kantrip therefore uses generic
`KAFKA_*` names for settings an application may consume. Kantrip-specific
session metadata uses `KANTRIP_*`.

Applications must opt in to these variables. Kantrip also generates
client-specific property files and adapters may pass those files or the
appropriate flags directly to supported tools.

### Kafka variables

| Variable | Meaning |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated broker addresses |
| `KAFKA_SECURITY_PROTOCOL` | Always `PLAINTEXT` for a valid current profile |
| `KAFKA_JAVA_CONFIG_FILE` | Generated Java Kafka properties path |
| `KAFKA_LIBRDKAFKA_CONFIG_FILE` | Generated librdkafka properties path |
| `KCAT_CONFIG` | Generated librdkafka properties path read natively by kcat |

### Schema Registry variables

These variables are present when the selected profile has a supported plain
Schema Registry connection.

| Variable | Meaning |
| --- | --- |
| `SCHEMA_REGISTRY_URL` | Schema Registry URL from the selected profile |
| `SCHEMA_REGISTRY_CONFIG_FILE` | Generated Schema Registry properties path |

### Kantrip session metadata

| Variable | Meaning |
| --- | --- |
| `KANTRIP_PROFILE` | Selected profile display name |
| `KANTRIP_SESSION_ID` | Opaque session identifier |
| `KANTRIP_SESSION_DIR` | Private temporary session directory |

Applications should prefer their native generated file where practical, fall
back to the documented variables, and avoid logging the complete environment or
generated properties.

## Output and color

Normal command output goes to stdout and diagnostics go to stderr. Styling is
disabled when:

- `--no-color` is supplied.
- `NO_COLOR` is present in the environment.
- `TERM=dumb`.
- The destination stream is not a terminal.

`kantrip list` displays configured profiles with their Kafka bootstrap servers
and Schema Registry URL. `kantrip show PROFILE` syntax-highlights its redacted
YAML. Both remain readable without ANSI color when styling is disabled.

Sensitive-looking values are classified before they reach Rich. Styling never
changes exit statuses or becomes necessary to interpret an error.
