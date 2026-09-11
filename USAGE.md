# Kantrip Usage

Kantrip is in pre-release development. The current CLI can manage plaintext
profiles and run profile sessions:

```bash
kantrip add local
kantrip list
kantrip show local
kantrip exec local -- kcat -L
```

Persistent selection, connectivity checks, and authenticated sessions are not
implemented yet.

## First-run configuration

Add a plaintext profile using the default local broker:

```bash
kantrip add local
```

Choose one or more broker addresses when needed:

```bash
kantrip add development \
  --bootstrap-server kafka-1.example.com:9092 \
  --bootstrap-server kafka-2.example.com:9092
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
subshell using `SHELL`, or `/bin/sh` when `SHELL` is unset:

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

## Displaying the active profile in your prompt

Prompt integrations should read `KANTRIP_PROFILE`. It is available to an
interactive shell opened by `kantrip exec PROFILE` and disappears when that
session exits. Prompt code does not need to invoke Kantrip repeatedly.

### Starship

Add a custom module to `~/.config/starship.toml`:

```toml
[custom.kantrip]
command = 'printf %s "$KANTRIP_PROFILE"'
when = 'test -n "$KANTRIP_PROFILE"'
format = '[kantrip:$output]($style) '
style = 'bold purple'
```

For a more compact prompt with a magic wand, use this `format` instead:

```toml
format = '[🪄 $output]($style) '
```

If you use MesloLGS NF or another Nerd Font, you can use its magic-wand glyph
(`U+F1844`) instead:

```toml
format = '[󱡄 $output]($style) '
```

Browse the [Nerd Fonts cheat sheet](https://www.nerdfonts.com/cheat-sheet) for
more glyphs you can use in its place.

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

As a magic-wand alternative, replace the first `print` command in
`kantrip_prompt_info` with:

```zsh
print -P -n '%F{magenta}🪄 %f%F{cyan}'
```

With MesloLGS NF or another Nerd Font, use its magic-wand glyph (`U+F1844`)
instead:

```zsh
print -P -n '%F{magenta}󱡄 %f%F{cyan}'
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

For a compact magic-wand segment, use this `p10k segment` line instead:

```zsh
p10k segment -f 5 -t "🪄 ${KANTRIP_PROFILE}"
```

With MesloLGS NF or another Nerd Font, use its magic-wand glyph (`U+F1844`)
instead:

```zsh
p10k segment -f 5 -t "󱡄 ${KANTRIP_PROFILE}"
```

Then add `kantrip` to either `POWERLEVEL9K_LEFT_PROMPT_ELEMENTS` or
`POWERLEVEL9K_RIGHT_PROMPT_ELEMENTS` in the same file. Start a new shell, run
`kantrip exec local`, and the segment will be visible until that subshell exits.

### kcat

Kantrip generates a private librdkafka properties file for each session and sets
`KCAT_CONFIG` to its path. kcat reads this variable natively, so Kantrip does not
create an alias or rewrite kcat's arguments.

```bash
kantrip exec local -- kcat -L
kantrip exec local -- kcat -C -t orders
kantrip exec local -- kcat -P -t orders
```

Kantrip reports a command-not-found error when an explicit executable is missing.
It does not install external tools. The kcat `-F` option is rejected because it
would override the selected profile.

Only profiles with `transport: plaintext` and `auth.type: none` can currently be
executed. Other valid profiles can still be listed and displayed safely.

### Kafka topics

Both executable names shipped by common Apache Kafka distributions are
supported:

```bash
kantrip exec local -- kafka-topics --list
kantrip exec local -- kafka-topics.sh --describe --topic orders
```

Kantrip injects the selected profile through `--bootstrap-server` and a private
Java `--command-config` file. Supplying either connection option explicitly is
rejected because it would override the selected profile.

An interactive session creates temporary executable shims for whichever variants
are installed before the session starts, so the same commands work without
repeating `kantrip exec`:

```bash
kantrip exec local
kafka-topics --list
exit
```

The shims exist only inside that session and are removed on exit. Kantrip does
not install the Kafka CLI or create persistent shell aliases. For Zsh and Bash,
Kantrip loads the user's normal interactive startup file through a private
session startup file, then restores the shim directory to the front of `PATH`
and clears the shell's command cache. This keeps Oh My Zsh, Homebrew, and other
startup-time `PATH` configuration from bypassing the adapters. Zsh sessions
restore and load the user's normal history file instead of writing command
history into the temporary session directory.

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

This adapter is a compatibility bridge for Kaskade's current CLI. It does not set
a Kaskade-specific environment variable. Kantrip can move to direct environment
integration after Kaskade defines and implements that contract.

## Profile configuration

Profile metadata is YAML validated against
[`schemas/profile-v1.schema.json`](https://github.com/sauljabin/kantrip/blob/main/schemas/profile-v1.schema.json).
A synthetic example is available at
[`examples/config.yaml`](https://github.com/sauljabin/kantrip/blob/main/examples/config.yaml).

Planned configuration lookup order is:

1. `KANTRIP_CONFIG`.
2. `$XDG_CONFIG_HOME/kantrip/config.yaml`.
3. `~/.config/kantrip/config.yaml`.

Profile YAML stores only non-secret metadata and operating-system credential
references. Passwords, tokens, private keys, secret-bearing JAAS strings, and
keystore passwords are rejected as literal profile fields.

## Application environment from `kantrip exec`

Kantrip publishes connection settings only to the supervised child process and
its descendants. The variables Kantrip may set are listed below.

Kafka clients share configuration-property names but do not define one
cross-language environment-variable standard. Kantrip therefore uses generic
`KAFKA_*` and `SCHEMA_REGISTRY_*` names for settings an application may consume.
Only Kantrip-specific session metadata uses `KANTRIP_*`.

Applications must opt in to these variables. Kantrip also generates
client-specific property files and adapters may pass those files or the
appropriate flags directly to supported tools.

### Kafka variables

| Variable | Meaning |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated broker addresses |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL` |
| `KAFKA_JAVA_CONFIG_FILE` | Generated Java Kafka properties path |
| `KAFKA_LIBRDKAFKA_CONFIG_FILE` | Generated librdkafka properties path |
| `KAFKA_SASL_MECHANISM` | Present when SASL is configured |
| `KAFKA_SASL_USERNAME` | Present when the mechanism uses a username |
| `KAFKA_SASL_PASSWORD` | Present when the mechanism uses a password |
| `KAFKA_OAUTH_TOKEN_ENDPOINT` | Present when OAuth token acquisition is configured |
| `KAFKA_OAUTH_CLIENT_ID` | Present when OAuth client identity is configured |
| `KAFKA_OAUTH_CLIENT_SECRET` | Present when OAuth client credentials are used |
| `KAFKA_OAUTH_ACCESS_TOKEN` | Present when a fixed or acquired token is used |
| `KAFKA_SSL_CA_LOCATION` | Materialized or referenced CA bundle path |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | Materialized client certificate path |
| `KAFKA_SSL_KEY_LOCATION` | Materialized client private-key path |
| `KAFKA_SSL_KEY_PASSWORD` | Present when the private key is encrypted |

### Schema Registry variables

These variables are present only when the selected profile configures Schema
Registry.

| Variable | Meaning |
| --- | --- |
| `SCHEMA_REGISTRY_CONFIG_FILE` | Generated Schema Registry properties path |
| `SCHEMA_REGISTRY_URL` | Registry URL |
| `SCHEMA_REGISTRY_USERNAME` | Present for basic authentication |
| `SCHEMA_REGISTRY_PASSWORD` | Present for basic authentication |
| `SCHEMA_REGISTRY_TOKEN` | Present for bearer authentication |
| `SCHEMA_REGISTRY_SSL_CA_LOCATION` | Materialized or referenced CA bundle path |
| `SCHEMA_REGISTRY_SSL_CERTIFICATE_LOCATION` | Materialized client certificate path |
| `SCHEMA_REGISTRY_SSL_KEY_LOCATION` | Materialized client private-key path |
| `SCHEMA_REGISTRY_SSL_KEY_PASSWORD` | Present when the private key is encrypted |

### Kantrip session metadata

| Variable | Meaning |
| --- | --- |
| `KANTRIP_PROFILE` | Selected profile display name |
| `KANTRIP_SESSION_ID` | Opaque session identifier |
| `KANTRIP_SESSION_DIR` | Private temporary session directory |

Secret values are intentionally child-only. A user-selected child can read
them; Kantrip protects them at rest and from shell history, not from the process
the user explicitly launches. Applications should prefer their native generated
file where practical, fall back to the documented variables, and never log the
resolved environment or generated properties.

## Output and color

Normal command output goes to stdout and diagnostics go to stderr. Styling is
disabled when:

- `--no-color` is supplied.
- `NO_COLOR` is present in the environment.
- `TERM=dumb`.
- The destination stream is not a terminal.

`kantrip list` displays configured profiles in a table with styled headers and
profile names. `kantrip show PROFILE` syntax-highlights its redacted YAML. Both
remain readable without ANSI color when styling is disabled.

Secret classification occurs before values reach Rich. Styling never changes
exit statuses or becomes necessary to interpret an error.
