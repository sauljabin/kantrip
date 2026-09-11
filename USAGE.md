# Kantrip Usage

Kantrip is a pre-release CLI for plaintext profiles and scoped sessions:

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

Doctor groups local checks under System, Configuration, Session, and Clients,
names missing commands, and summarizes health. It validates configuration,
permissions, profile IDs, registry support, active-session state, and the
installed shell, kcat, Kafka, Confluent Schema Registry, and Kaskade commands.
Use `--verbose` for executable paths and individual checks.

Missing optional clients or first-run configuration produce warnings. Invalid
configuration, unsupported registry settings, or inconsistent session state
exit with status 1. Doctor never contacts Kafka or Schema Registry.

## Kafka and Schema Registry connectivity

Check Kafka and, when configured, Schema Registry connectivity:

```bash
kantrip ping local
```

`ping` uses Confluent's Admin client for Kafka metadata and, when configured,
requests Schema Registry `/subjects`; it reports broker and subject counts. It
needs no external CLI and defaults to a five-second timeout, configurable with
`--timeout SECONDS`. Colored terminals animate checks; plain output uses
`[running]`.

## First-run configuration

Add a plaintext profile using the default local broker:

```bash
kantrip add local
```

Choose one or more broker addresses when needed:

```bash
kantrip add development \
  --bootstrap-servers kafka-1.example.com:9092,kafka-2.example.com:9092 \
  --description 'Development cluster' \
  --schema-registry-url http://registry.example.com:8081
```

`add` creates the configuration when needed and never overwrites a profile.
Remove one with:

```bash
kantrip remove development
```

`kantrip list` succeeds without rows when no profiles exist. Kantrip validates
configuration whenever it reads or updates it.

## Profile workflow

```bash
kantrip list
kantrip show local
kantrip exec local -- kcat -L
```

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

For kcat 1.7+, `-s avro`, `-s key=avro`, and `-s value=avro` inject the profile's
registry URL through `-r`. Kantrip rejects `-F`, `-r`, and
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

They require the plain `schemaRegistry` section below; missing or secured
configuration fails before launch.

### Kaskade 5

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
registry` or `-v registry`) selects a second INI with a `[registry]` URL. Kantrip
rejects `-b`/`--bootstrap-servers`, `--kafka`, `--config-file`, and `--registry`
overrides. Interactive shims behave identically.

Kaskade 5+ supports Avro, JSON Schema, and Protobuf registry decoding through
this adapter; Kantrip sets no Kaskade-specific environment variable.

## Profile configuration

The [profile schema](https://github.com/sauljabin/kantrip/blob/main/schemas/profile.schema.json)
validates YAML metadata; see the [synthetic example](https://github.com/sauljabin/kantrip/blob/main/examples/config.yaml).

Configuration lookup order is:

1. `KANTRIP_CONFIG`.
2. `$XDG_CONFIG_HOME/kantrip/config.yaml`.
3. `~/.config/kantrip/config.yaml`.

Profiles support plaintext Kafka metadata and an optional registry connection:

```yaml
schemaRegistry:
  url: http://localhost:8081
  auth:
    type: none
```

Kafka authentication and TLS are schema-invalid. Registry security metadata may
be stored for future support, but registry-aware commands currently require the
plain form above.

Configuration has no version field; each release's bundled schema is
authoritative. If an early alpha file reports `unknown field: version`, remove
its top-level `version: 1`; see `MVP.md`.

## Application environment from `kantrip exec`

Kantrip exposes settings only to supervised children. Kafka has no
cross-language environment standard, so applications must opt into the generic
`KAFKA_*` values below; `KANTRIP_*` is reserved for session metadata. Adapters
may instead pass generated client files directly.

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

Prefer native generated files, fall back to these variables, and never log the
complete environment or properties.

## Output and color

Commands write results to stdout and diagnostics to stderr. Styling is disabled
when:

- `--no-color` is supplied.
- `NO_COLOR` is present in the environment.
- `TERM=dumb`.
- The destination stream is not a terminal.

`kantrip list` shows profile endpoints; `kantrip show PROFILE` syntax-highlights
redacted YAML. Both remain readable without ANSI color.

Values are classified before reaching Rich. Styling neither changes exit status
nor carries essential information.
