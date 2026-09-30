# Kantrip Usage

Kantrip saves Kafka connection profiles and runs clients with them:

```bash
kantrip add local
kantrip doctor
kantrip list
kantrip describe local
kantrip ping local
kantrip exec local -- kcat -L
```

See [Compatibility](COMPATIBILITY.md) for supported clients, authentication
methods, and file formats.

Kantrip isn't a process manager, a global "current context" switch, or a
replacement for your Kafka clients. It stores profiles, looks up their
credentials, writes the temporary configuration each client needs, and
supervises the command while it runs. Connections can be plaintext, TLS with a
verified server, SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, mTLS, or OAuth client
credentials. Kafka authentication always requires verified TLS. An OAuth token
endpoint has its own client ID, scopes, secret, and optional CA.

## Contents

- [Short command names](#short-command-names)
- [Local diagnostics](#local-diagnostics)
- [Kafka and registry connectivity](#kafka-and-registry-connectivity)
- [First profile](#first-profile)
- [Profile workflow](#profile-workflow)
- [Session supervision and recovery](#session-supervision-and-recovery)
- [Displaying the active profile in your prompt](#displaying-the-active-profile-in-your-prompt)
  - [Choose a display style](#choose-a-display-style)
  - [Starship](#starship)
  - [Zsh](#zsh)
  - [Powerlevel10k](#powerlevel10k)
  - [Bash](#bash)
  - [Fish](#fish)
  - [Verify the prompt](#verify-the-prompt)
- [Supported command-line tools](#supported-command-line-tools)
  - [kcat](#kcat)
  - [Apache Kafka and Confluent Kafka commands](#apache-kafka-and-confluent-kafka-commands)
  - [Additional Confluent Schema Registry console clients](#additional-confluent-schema-registry-console-clients)
  - [Kaskade](#kaskade)
  - [kaf](#kaf)
  - [kcl](#kcl)
- [Profile storage](#profile-storage)
- [Credential vault](#credential-vault)
  - [macOS vault](#macos-vault)
  - [Linux vault](#linux-vault)
- [Application environment from `kantrip exec`](#application-environment-from-kantrip-exec)
  - [Environment precedence](#environment-precedence)
  - [Kafka variables](#kafka-variables)
  - [Registry variables](#registry-variables)
  - [Kantrip session metadata](#kantrip-session-metadata)
- [Output and color](#output-and-color)

## Short command names

Kantrip doesn't install shortcuts, but if you want a shorter command, the
suggested one is `kan`. Make it a symlink in a directory on your `PATH`, such as
`~/.local/bin`:

```bash
ln -s "$(command -v kantrip)" ~/.local/bin/kan
```

A symlink works everywhere a command does: in every shell, in session shells,
and in scripts. The rest of this guide uses `kantrip`.

Clients take short names the same way. Kantrip follows the link and configures
the client it points to, both in `kantrip exec PROFILE -- kas admin` and in
session shells:

```bash
ln -s "$(command -v kaskade)" ~/.local/bin/kas
```

Use symlinks rather than aliases or wrapper scripts. Kantrip can't tell which
client an alias to a full path or a wrapper script runs, so that client runs
without the profile's connection.

## Local diagnostics

Run `kantrip doctor` to inspect the installation without opening a Kafka
connection:

```bash
kantrip doctor
```

Doctor groups its checks under System, Profiles, Credentials, Session, and
Clients, names any missing commands, and ends with a summary. It checks the
profile database, the credential vault, Registry settings, pending credential
cleanup, sessions, leftover session files, and installed clients, including
any that are older than Kantrip supports.

It reads every secret each profile references, for Kafka and the Registry,
OAuth client secrets included, and reports each one as stored, missing, or
unavailable under its field name, such as `registry.auth.token`. It never prints
the values. It also checks that certificates and keys are valid and when the
certificates expire. `doctor` is the only command that reads secrets just to
report on them; `describe` only lists them as `configured`. Before reading
anything, doctor says where the [credential vault](#credential-vault) is and
whether it's locked. On macOS it also shows the vault's lock setting while it's
unlocked. Add `-v`/`--verbose` for paths, the backend, and every individual
check.

Doctor always lists the sessions on this machine, one line each with the
profile, revision, and age; `-v` adds the session ID, supervisor PID, and path.
The Session section also says whether the current shell is inside a Kantrip
session. Stale sessions come with a `--repair` hint. An entry Kantrip can't
verify as its own is reported by path, and you have to inspect and remove it
yourself, because `--repair` never deletes it.

To check a single profile and its sessions, name it. The title shows the
profile, and the database line still counts every profile:

```bash
kantrip doctor production
kantrip doctor production -v
```

A missing optional client, or no profile database yet on a first run, gives a
warning. An invalid database, unsupported Registry settings, or inconsistent
session state make doctor exit with status 1. Doctor never contacts Kafka or a
Registry.

Commands that use profiles apply database migrations automatically. A plain
`doctor` run only reports pending work and never creates or changes anything.
`kantrip doctor --repair` does the maintenance: it migrates the profile
database, finishes pending credential cleanup, removes a deleted macOS vault
from Keychain Access, removes stale sessions it has validated, and then runs the
checks again. There's no separate migration or cleanup command. Each migration
backup is named with its UTC creation time and a unique suffix, so a later
migration never overwrites an earlier backup. Unreleased databases without a
migration history aren't supported and have to be recreated.

A profile database created by a pre-release (alpha or beta) version is rejected
with "created by a pre-release version of Kantrip". Kantrip does not modify it.
To start over, move it aside and add your profiles again:

```bash
mv ~/.local/share/kantrip/profiles.db ~/.local/share/kantrip/profiles.db.pre-release
```

Use the path reported by `kantrip doctor --verbose` if you set
`KANTRIP_DATABASE` or `XDG_DATA_HOME`. The old credentials stay in the OS
credential store under the service name `kantrip`. Delete them once the new
profiles work, as [Credential vault](#credential-vault) describes for each
platform.

## Kafka and registry connectivity

Check Kafka and, when configured, registry connectivity:

```bash
kantrip ping local
```

`ping` watches librdkafka's connection-state callbacks until one of the
configured or discovered brokers reaches `UP`, after whatever TLS and SASL
exchange the profile needs. It doesn't request topics, groups, schemas, or a
cluster description. With plaintext, that shows the broker is reachable; with
TLS alone, that the server's identity checks out; with SASL or mTLS, that the
authentication exchange succeeded. None of them shows what the client is
allowed to do.

The Registry check sends one bounded read query: `/subjects?limit=1` to a
Confluent-compatible Registry, including Apicurio's ccompat API (the URL is
used as is), or `/search/versions?limit=1` to native Apicurio v3. A valid empty
list is a success. For an authenticated profile, `ping` sends the same query
again without credentials and expects HTTP 401/403, or for mTLS a TLS handshake
rejected for lack of a client certificate. If the anonymous query succeeds
too, the Registry is reachable but your credentials weren't tested: `ping`
reports it as ok with the proof "endpoint also allows anonymous reads,
credentials not proven", prints a warning, and still exits `0`, the same way a
plaintext Kafka check only reports reachability. If the Registry rejects your
credentials (401/403 on the authenticated query), the check fails. With OAuth,
`ping` first gets one client-credentials token, with a time limit. Each HTTPS
connection checks the CA configured for it.

`ping` needs no external client. All checks share one five-second deadline,
which you can change with `--timeout SECONDS`. Colored terminals show a
spinner; plain output prints `[running]`. A failure includes a cleaned-up
message from the underlying client or connection error.

`ping` reports each service as `ok`, `failed`, or `skipped` (not attempted) and
exits `0` only when every attempted check is ok. If Kafka succeeds and the
Registry fails, the Kafka result is still printed and the Registry failure
follows on stderr (exit `1`). If Kafka fails, the Registry is not contacted and
is reported as skipped.

Scripts can read the same result as JSON or YAML with `-o json` or `-o yaml`;
`--quiet` cannot be combined with them:

```json
{
  "profile": "local",
  "healthy": true,
  "kafka": {"status": "ok", "transport": "verified TLS", "authentication": "scram-sha-512 authenticated", "proof": "sasl"},
  "registry": {"status": "ok", "provider": "confluent", "transport": "verified TLS", "proof": "basic authenticated read query validated", "warning": null}
}
```

A failed service carries `error` and `cause`; a skipped Registry carries
`reason`.

Kafka success does not prove topic, group, schema, cluster, or administrative
access. Registry success proves only that the configured read/search query is
allowed; it does not prove access to a particular schema or permission to write.

For scripts that need only the exit status, suppress all output with:

```bash
kantrip ping local --quiet
```

`-q` is the short form of `--quiet`. Either way, `ping` prints nothing to stdout
or stderr, whether it succeeds or fails, and exits with 0 or 1.

## First profile

Add a plaintext profile using the default local broker:

```bash
kantrip add local
```

Use TLS with each client's default trust store for a broker whose certificate
is issued by a trusted public CA:

```bash
kantrip add production \
  --bootstrap-server kafka.example.com:9093 \
  --transport tls
```

`add` selects TLS on its own when `--ca-file`, `--auth`, or a client
certificate is given, so the examples below omit `--transport tls`. An explicit
`--transport plaintext` with any of them still fails: credentials never travel
over plaintext. Without them, `add` defaults to plaintext.

On success, `add` and `edit` print the resulting connection, and `remove`
confirms the removal:

```text
Added profile 'production': kafka.example.com:9093, tls, no authentication
Updated profile 'production': kafka-2.example.com:9093, tls, scram-sha-512
Removed profile 'production'
```

For a private CA, pass a PEM certificate bundle. Kantrip validates it and
copies the certificates into the profile. At run time they're written only to
the private session directory:

```bash
kantrip add production-private-ca \
  --bootstrap-server kafka.internal.example:9093 \
  --ca-file ./cluster-ca.pem
```

The CA file must be a regular UTF-8 PEM file of at most 1 MiB. Certificate and
hostname verification can't be turned off. `kcat`, `kafkacat`, and Kaskade read
the PEM bundle directly. The Java Kafka commands need Apache Kafka 2.7+ or
Confluent Platform 6.1+ to use a PEM trust store. Kantrip checks the installed
Java client's version before the Kafka operation and tells you what to install
when it can't confirm support. Kafka 2.6 and Confluent Platform 6.0 can still
use TLS with their default trust stores.

Add password authentication over TLS. Kantrip asks for the password at a
prompt that doesn't echo and stores it in the
[credential vault](#credential-vault), never in the profile or in command
arguments:

```bash
kantrip add production-scram \
  --bootstrap-server kafka.example.com:9093 \
  --auth scram-sha-512 \
  --username application
```

This works the same for `plain`, `scram-sha-256`, and `scram-sha-512`. Kantrip
asks for the password only when it creates the credential, or when you replace
`kafka.auth.password` explicitly:

```bash
kantrip edit production-scram --replace-secret kafka.auth.password
```

For mTLS, Kantrip copies the public certificate chain into the profile, and
stores the private key, plus its password if the key is encrypted, in the vault
after checking them:

```bash
kantrip add production-mtls \
  --bootstrap-server kafka.example.com:9093 \
  --auth mtls \
  --client-certificate-file ./client.crt \
  --client-key-file ./client.key
```

The certificate and key must be regular PEM files within a size limit, and
they must match. For an encrypted key, Kantrip asks for its password at a
prompt that doesn't echo.

OAuth uses the client-credentials grant. The broker and the token endpoint
each have their own CA:

```bash
kantrip add production-oauth \
  --bootstrap-server kafka.example.com:9093 \
  --ca-file ./kafka-ca.pem \
  --auth oauth \
  --oauth-token-url https://identity.example.com/oauth/token \
  --oauth-client-id kantrip-production \
  --oauth-scope kafka.read \
  --oauth-ca-file ./identity-ca.pem
```

Kantrip asks for the client secret at a prompt that doesn't echo. Repeat
`--oauth-scope` for several scopes; their order is kept. `edit` leaves OAuth
fields you don't mention alone; `--unset kafka.auth.oauth.scopes` and
`--unset kafka.auth.oauth.ca` remove them.

To give several brokers, repeat `-b/--bootstrap-server` or separate the
addresses with commas. Either way the order is kept and duplicates are
rejected:

```bash
kantrip add development \
  -b kafka-1.example.com:9092 \
  -b kafka-2.example.com:9092 \
  --description 'Development cluster' \
  --label environment=development \
  --label owner=platform \
  --registry-url http://registry.example.com:8081
```

`--registry-url` assumes a Confluent-compatible Registry. For native Apicurio,
say so and pass its Core Registry API v3 endpoint:

```bash
kantrip add development-apicurio \
  --bootstrap-server kafka-1.example.com:9092,kafka-2.example.com:9092 \
  --registry-provider apicurio \
  --registry-url http://registry.example.com:8080/apis/registry/v3
```

`--registry-provider` without `--registry-url` is invalid.

A secure Registry has its own CA and credentials, separate from Kafka's. For
example:

```bash
kantrip add registry-oauth \
  --registry-provider confluent \
  --registry-url https://registry.example.com \
  --registry-ca-file ./registry-ca.pem \
  --registry-auth oauth \
  --registry-oauth-token-url https://identity.example.com/oauth/token \
  --registry-oauth-client-id registry-client \
  --registry-oauth-scope registry.read \
  --registry-oauth-ca-file ./identity-ca.pem
```

Registry Basic auth, fixed Confluent bearer tokens, mTLS, and OAuth all take
their secrets from prompts that don't echo or from private-key files within a
size limit. Kafka and the Registry never share secrets or CA bundles.
`--unset registry.tls.ca`, `--unset registry.auth.oauth.ca`,
`--unset registry.auth.oauth.scopes`,
`--unset registry.auth.oauth.logical-cluster`, and
`--unset registry.auth.oauth.identity-pool-id` remove the matching field.
Changing the provider requires a new Registry URL, and keeps the current
authentication only if the new provider supports it.

`add` creates the database if needed and never overwrites an existing profile.
`-l` is short for `--label KEY=VALUE`, which you can repeat. `edit` changes only
the fields you pass and keeps the profile's identity:

```bash
kantrip edit development \
  --bootstrap-server kafka-3.example.com:9092,kafka-4.example.com:9092 \
  --description 'Shared development cluster' \
  --label environment=development \
  --registry-url http://registry.example.com:8081
```

`edit --transport tls` turns on TLS with the client's default trust store.
`edit --ca-file PATH` replaces the custom CA of a TLS profile; add
`--transport tls` to switch a plaintext profile to TLS at the same time. To go
back to the default trust store, run `edit --unset kafka.tls.ca`; TLS stays on.
`--transport plaintext` removes the stored TLS settings. `--unset kafka.tls.ca`
and `--ca-file` can't be combined.

`edit` adds or updates labels, and can add a Registry to a profile that has
none. With only `--registry-url`, the new Registry is Confluent. `--unset FIELD`,
which you can repeat, removes an optional field: `description`, `labels.KEY`,
`registry`, `kafka.tls.ca`, and the Kafka and Registry trust and OAuth fields
above. An unknown or required field fails with a list of the valid names.
Options you leave out keep their current values. `edit` needs at least one
option; `edit PROFILE` alone prints usage and exits with status 2.

Changing the Registry authentication type (for example `--registry-auth none`,
or from OAuth to Basic) drops all fields of the old type, and its stored
credentials are deleted once the change is committed. Changing the provider
keeps the current authentication if the new provider supports it; otherwise
`edit` changes nothing and asks you to pass `--registry-auth`.

Remove one with:

```bash
kantrip remove development
```

`remove` asks for confirmation, and the confirmation applies to the exact
profile version (UUID and revision) it showed you. Pass `-y/--yes` to skip it.
Without a terminal, `remove` fails unless you pass `--yes`.

`add`, `edit`, and `remove` exit with:

- `0` when the change and its cleanup finished.
- `1` when the change ran but certainly wasn't committed.
- `2` for invalid options or values. Nothing changes, and the message says what
  to fix.
- `3` when the change was committed but cleanup or the check afterwards didn't
  finish. Look at `describe` and run `doctor --repair`; don't just repeat the
  command.
- `4` when Kantrip can't tell whether the change was committed. Stop any
  automatic retries and look at the local storage first.

If stdout breaks (for example, a closed pipe) after the change is saved, the
exit status is 3, as long as the process can still exit normally. A signal or
`SIGKILL` can end the process before it reports a status; check the profile and
run `doctor` before retrying. Kantrip can't guarantee a command runs exactly
once if the process is killed.

`kantrip list` prints nothing and succeeds when there are no profiles; on a
terminal it also prints a hint to stderr. Kantrip validates every profile each
time it reads or updates the database.

The human-readable list shows labels and a short profile ID, the first eight
characters of the profile's UUID, which never changes. Filter by an exact
label with `-l` or `--label`; with several filters, a profile must match all of
them:

```bash
kantrip list --label environment=development --label owner=platform
```

Use `--output json` or `--output yaml` (`-o` for short) for output you can
parse. On a color terminal it's syntax-highlighted; with `--no-color`, or when
the output is redirected, it's plain text without ANSI escapes. With no
profiles, you get an empty list.

## Profile workflow

```bash
kantrip list
kantrip describe local
kantrip describe local --output yaml
kantrip exec local -- kcat -L
```

`kantrip exec PROFILE COMMAND…` passes everything after COMMAND to it unchanged,
so `kantrip exec local kafka-topics --help` shows the tool's own help. The `--`
before COMMAND is optional; the examples keep it for clarity. Kantrip's own
options, such as `--no-color`, go before PROFILE.

`describe` prints sections for people by default. Its JSON and YAML output is
for reading, not a profile export you can import again. It shows every public
connection field and the profile's current revision, and leaves out secret
values and internal credential references. Kafka and the Registry use the same
`auth` shape: `type`, the public fields for that type (`username`, the
`clientCertificate` subject and expiry, or the `oauth` endpoint, client ID,
scopes, and token CA), and `credentials`, which lists each secret the profile
uses as `configured`:

```json
"auth": {
  "type": "scram-sha-512",
  "username": "app",
  "credentials": {"kafka.auth.password": "configured"}
}
```

`describe` never reads the vault, so it never prompts or unlocks anything.
Run `kantrip doctor` to check that each credential is really stored.

Without a command, `kantrip exec PROFILE` opens a Bash, Zsh, or Fish subshell,
whichever `SHELL` names, or Bash if `SHELL` is unset. Other shells fail with an
error that says so:

```bash
kantrip exec local
kantrip current
kcat -L
kafka-topics --list
exit
```

Nothing is exported to the shell you started from. Sessions can't be nested,
and detached children aren't supported, so exit one session before starting
another.

`kantrip current` prints the active profile, or says there isn't one. It's the
same as reading `KANTRIP_PROFILE`.

The subshell loads your usual startup files and history. Kantrip then disables
any aliases, functions, and Fish abbreviations that would hide a supported
client, keeps its wrapper directory first on `PATH`, and undoes all of it when
you exit.

## Session supervision and recovery

A one-off command runs in its own POSIX process group. A Bash, Zsh, or Fish
subshell runs on a PTY, so typing, Ctrl-C, job control, and window resizing
behave normally. Kantrip exits with the child's exit code, or with
`128 + signal number` if a signal killed it.

Kantrip forwards SIGINT, SIGTERM, and SIGHUP to the process group or subshell.
If a second signal arrives, or the child is still alive five seconds later,
Kantrip sends SIGKILL. When Kantrip exits, it restores the terminal and its
signal handlers. Processes that daemonize or start a new POSIX session on
purpose aren't supported.

Session files live under `$XDG_RUNTIME_DIR/kantrip/sessions` when that runtime
directory is private and owned by you. Otherwise Kantrip uses a private
`kantrip-<uid>/sessions` directory in the system's temporary directory. Each
session holds a lock while it runs, and that lock, not the PID, decides whether
the session is still active.

Before each `exec`, Kantrip looks at up to 256 entries in that directory and
removes unlocked sessions older than five minutes once they pass validation.
To see the full state without changing anything, or to clean up what can be
cleaned up safely, run:

```bash
kantrip doctor
kantrip doctor --repair
```

Repair applies pending database migrations and removes every stale session
that passes validation. It leaves alone anything with a malformed marker, a
symlink, unsafe permissions, the wrong owner, or a path outside the session
directory. Active and recent sessions aren't touched. Paths are shown only with
`--verbose`.

## Displaying the active profile in your prompt

Your prompt can read `KANTRIP_PROFILE`, which is set only inside a
`kantrip exec PROFILE` subshell, so it doesn't need to run Kantrip at all.

Set up only the one that draws your prompt:

- Use **Starship** if Starship controls your prompt.
- Use **Powerlevel10k** if Powerlevel10k controls your prompt, even when it is
  installed through Oh My Zsh.
- Use **Zsh** for Zsh's own prompt, or for an Oh My Zsh theme that uses the
  standard `PROMPT` variable.
- Use **Bash** or **Fish** for their own prompt.

Kantrip loads your usual startup files, so these settings work inside the
session too.

### Choose a display style

The examples show the Apache Kafka glyph before the profile name. To use another
style, replace the glyph as each section describes:

| Style | Reference | Requirement |
| --- | --- | --- |
| Apache Kafka (default) | <img src="images/nf-md-apache-kafka.svg" alt="Nerd Font Apache Kafka glyph" height="16"> | A [Nerd Font](https://www.nerdfonts.com/) with `nf-md-apache_kafka` (`U+F100F`) |
| Kantrip magic staff | <img src="images/nf-md-magic-staff.svg" alt="Nerd Font magic staff glyph" height="16"> | A [Nerd Font](https://www.nerdfonts.com/) with `nf-md-magic_staff` (`U+F1844`) |
| Magic-wand emoji | 🪄 | An emoji fallback font |
| Text | `kantrip:` | None |

GitHub may render Nerd Font characters as boxes; the images show their terminal
appearance. See the [Nerd Fonts cheat sheet](https://www.nerdfonts.com/cheat-sheet)
for more glyphs.

### Starship

Add a custom module to `~/.config/starship.toml`:

```toml
[custom.kantrip]
command = 'printf %s "$KANTRIP_PROFILE"'
when = 'test -n "$KANTRIP_PROFILE"'
symbol = '󱀏 '
style = 'bold purple'
```

Like Starship's own modules, the custom module shows its `symbol` before its
output. For another style, set `symbol` to `'󱡄 '`, `'🪄 '`, or `'kantrip:'`.

Starship's default prompt includes custom modules. If you define a custom global
`format`, add `${custom.kantrip}` where the profile should appear.

### Zsh

For Zsh's own prompt, or an Oh My Zsh theme that uses the standard `PROMPT`
variable, add this to the end of `~/.zshrc`, after `source $ZSH/oh-my-zsh.sh`
when you use Oh My Zsh:

```zsh
kantrip_prompt_info() {
  [[ -n ${KANTRIP_PROFILE:-} ]] || return
  print -P -n '%F{magenta}󱀏 %f%F{cyan}'
  print -rn -- "$KANTRIP_PROFILE"
  print -P -n '%f '
}

setopt prompt_subst
PROMPT='$(kantrip_prompt_info)'"$PROMPT"
```

For another style, replace `󱀏 ` in the first `print` command with `󱡄 `, `🪄 `, or
`kantrip:`. If a theme or plugin sets `PROMPT` later in `~/.zshrc`, keep this
snippet after it.

### Powerlevel10k

Define a custom segment in `~/.p10k.zsh`:

```zsh
function prompt_kantrip() {
  [[ -n ${KANTRIP_PROFILE:-} ]] || return
  p10k segment -f 5 -i '󱀏' -t "${KANTRIP_PROFILE}"
}
```

Then add `kantrip` to either `POWERLEVEL9K_LEFT_PROMPT_ELEMENTS` or
`POWERLEVEL9K_RIGHT_PROMPT_ELEMENTS` in the same file.

The glyph is the segment's icon (`-i`), so Powerlevel10k places it like its
other icons: before the profile in the left prompt and after it in the right
prompt, unless `POWERLEVEL9K_ICON_BEFORE_CONTENT` says otherwise. For another
style, set the icon to `'󱡄'` or `'🪄'`; for text, remove `-i '󱀏'` and use
`-t "kantrip:${KANTRIP_PROFILE}"`.

### Bash

Add this to the end of `~/.bashrc`:

```bash
PS1='${KANTRIP_PROFILE:+󱀏 $KANTRIP_PROFILE }'"$PS1"
```

For another style, replace `󱀏 ` with `󱡄 `, `🪄 `, or `kantrip:`.

### Fish

Add this to `~/.config/fish/config.fish`:

```fish
functions --copy fish_prompt kantrip_original_prompt
function fish_prompt
    set -q KANTRIP_PROFILE; and printf '󱀏 %s ' $KANTRIP_PROFILE
    kantrip_original_prompt
end
```

For another style, replace `󱀏 ` with `󱡄 `, `🪄 `, or `kantrip:`.

### Verify the prompt

Start a new shell or reload the relevant prompt configuration, then run:

```bash
kantrip exec local
```

The profile should appear in the subshell and disappear after `exit`.

## Supported command-line tools

Inside `kantrip exec`, Kantrip configures these tools for the profile and
rejects arguments that would override it. It tells you about missing clients
but doesn't install them.

Before each launch, Kantrip checks the installed client's version against the
minimum listed in [Compatibility](COMPATIBILITY.md#client-commands). A client
that's too old, or whose version Kantrip can't read, doesn't start, and the
error names the release to install. In a subshell, all installed clients are
checked in parallel when the session starts.

### kcat

Each session points `KCAT_CONFIG` at a private librdkafka properties file.
Subshells also add a small `kcat` wrapper, so `PATH` changes, aliases, and
functions from your startup files can't bypass it. The wrapper adds no
arguments.

```bash
kantrip exec local -- kcat -L
kantrip exec local -- kcat -C -t orders
kantrip exec local -- kcat -P -t orders
kantrip exec local -- kcat -C -t avro-orders -s value=avro
```

With `-s avro`, `-s key=avro`, or `-s value=avro`, Kantrip passes the
profile's Confluent-compatible Registry URL with `-r`. kcat doesn't work with
native Apicurio profiles. Kantrip rejects `-F`, `-r`, and
`-X schema.registry.url=...`. Other modes don't need a Registry.

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

Kantrip adds `--bootstrap-server` and a private Java client properties file:
`--consumer.config` for console consumers, `--producer.config` for console
producers, and `--command-config` for the admin commands. You can't override
these, or pass older connection options instead. Use the `.sh` names with
Apache Kafka archives and the unsuffixed names with Confluent Platform.

In a subshell, temporary wrappers let you run the commands without repeating
`kantrip exec`:

```bash
kantrip exec local
kafka-topics --list
kafka-console-consumer --topic orders --from-beginning
exit
```

The wrappers work as described above and disappear when you exit. Kantrip
never installs commands or permanent aliases.

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

Each gets the profile's bootstrap servers, a private Java client file, and
`schema.registry.url`. Options that would change the connection or the Registry
endpoint are rejected, both in one-off commands and in subshells.

They need a plain Registry with the Confluent provider. Without a Registry, or
with a native Apicurio one, they fail before starting.

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

The default INI holds the Kafka properties. Registry decoding (`-k registry` or
`-v registry`) switches to a second INI with a `[registry]` section for the
profile's provider. Kantrip rejects `-b`/`--bootstrap-servers`, `--kafka`,
`--config-file`, and `--registry`, except the two `--kafka` settings listed in
[Compatibility](COMPATIBILITY.md#client-commands). Subshells work the same way.

Through Kantrip, Kaskade decodes Avro, JSON Schema, and Protobuf with both
Confluent Schema Registry and native Apicurio Registry. Native Apicurio uses its
default `contentId` framing, because Kantrip only configures the Registry URL.
Kantrip sets no Kaskade-specific environment variable.

### kaf

Kantrip gives [kaf](https://github.com/birdayz/kaf) a private configuration with
one cluster, `kantrip`, built from the profile, and selects it with `--config`:

```bash
kantrip exec local -- kaf topics
kantrip exec local -- kaf consume orders --offset oldest
echo hello | kantrip exec local -- kaf produce orders
kantrip exec local -- kaf groups
```

The first command becomes:

```bash
kaf --config /tmp/kantrip-SESSION/kaf.yaml topics
```

kaf supports plaintext, verified TLS, SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512,
and mTLS profiles. OAuth profiles fail before launch: kaf's token client can't
use the profile's token-endpoint CA. An encrypted mTLS key is decrypted into a
private session file, because kaf can't read encrypted keys.

Kantrip rejects `--config`, `-b`/`--brokers`, `-c`/`--cluster`, and
`--schema-registry`, and every `kaf config` command: kaf saves its cluster list
to `~/.kaf/config` whatever `--config` names, which would copy the profile's
credentials there. As a second guard, kaf runs with `HOME=/dev/null`, so any
save fails before a file exists. Your own `~/.kaf/config` is never read or
changed.

When the profile has a Registry, kaf decodes Avro records in `consume` and
encodes JSON input as Avro with `produce --avro-schema-id`:

```bash
echo '{"id":"42"}' | kantrip exec local -- kaf produce orders --avro-schema-id 7
```

kaf reaches the Registry with the system CA store, so it supports a
Confluent-compatible Registry with no authentication or Basic, over HTTP or
HTTPS that the system trusts. Any other Registry profile makes every kaf command
fail before launch: Apicurio's native API, a custom Registry CA, fixed token,
mTLS, or OAuth.

### kcl

Kantrip gives [kcl](https://github.com/twmb/kcl) a private TOML configuration
built from the profile and selects it with `KCL_CONFIG_PATH`. The command line
is not changed:

```bash
kantrip exec local -- kcl topic list
kantrip exec local -- kcl consume orders --offset :end
echo hello | kantrip exec local -- kcl produce orders
kantrip exec local -- kcl group list
```

Kantrip requires kcl 0.20.0 or newer and checks the installed version with
`kcl --version` before launch; development builds fail that check. kcl supports
plaintext, verified TLS, SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and mTLS
profiles. OAuth profiles fail before launch because kcl has no OAuth
mechanism. An encrypted mTLS key is decrypted into a private session file,
because kcl can't read encrypted keys.

Kantrip rejects `-B`/`--bootstrap-servers`, `-R`/`--registry`,
`-C`/`--profile`, `--config-path`, `--no-config-file`, `--config-env-prefix`,
and every `kcl profile` command. `-X`/`--config-opt` accepts only
`broker_timeout`, `dial_timeout`, `retry_timeout`, `help`, and `list`; every
other key would change the connection. kcl also reads `KCL_*` variables, such
as `KCL_SEED_BROKERS` or `KCL_PROFILE`, so Kantrip removes them before kcl
starts. Your own kcl configuration file is never read or changed.

When the profile has a Registry, `kcl registry` commands use it, `consume
--decode` decodes records, and `produce --schema` encodes JSON input:

```bash
kantrip exec local -- kcl registry subject list
kantrip exec local -- kcl consume orders --decode=value
echo '{"id":"42"}' | kantrip exec local -- kcl produce orders --schema id:7
```

kcl supports a Confluent-compatible Registry with no authentication, Basic, a
fixed token, or mTLS, with system trust or the profile's custom CA. OAuth and
Apicurio's native API make every kcl command fail before launch.

## Profile storage

Profiles live in a private local database. Change them with `add`, `edit`, and
`remove` rather than editing the database directly.

Kantrip looks for the database in this order:

1. `KANTRIP_DATABASE`.
2. `$XDG_DATA_HOME/kantrip/profiles.db`.
3. `~/.local/share/kantrip/profiles.db`.

The directory is readable only by you (`0700`), and so is the database
(`0600`). Before writing, Kantrip checks that SQLite runs in WAL mode with FULL
synchronization, and on macOS also with `fullfsync`. After creating a database
or a migration backup, Kantrip syncs its directory before reporting success.
All of this assumes a
local filesystem. Network filesystems, cloud-synced folders, and restored or
copied databases that something else writes to at the same time aren't
covered.

The first `add` creates the database; commands that only read never do.
Kantrip rejects unsafe permissions, corrupt contents, and database versions it
doesn't support. Backups hold references to credentials, not the credentials,
so they can't bring back a value that was deleted after the backup. Restoring
the database without the matching vault isn't supported. Instead of deleting or
making up references, use `doctor` to see which credentials are missing and
`edit --replace-secret` to store them again. Keep your existing data until
you've followed the recovery steps Kantrip prints; it never resets anything on
its own.

A profile can have one Registry. `--registry-url` without `--registry-provider`
means Confluent. To use Apicurio's Confluent-compatible API with the Confluent
console clients, kcat Avro, or Kaskade:

```bash
kantrip add compatible \
  --bootstrap-server kafka.example.com:9092 \
  --registry-provider confluent \
  --registry-url http://registry.example.com:8080/apis/ccompat/v7
```

For native Apicurio with Kaskade, use `--registry-provider apicurio` and the
`/apis/registry/v3` endpoint. If you need both APIs, create two profiles with
the same Kafka connection. [Compatibility](COMPATIBILITY.md) lists which
clients and formats each API works with.

## Credential vault

Kantrip keeps passwords, private keys, tokens, and client secrets in its own
vault named `kantrip`, not in your login keychain or default keyring, so your
login alone does not unlock them. The operating system owns the vault password:
Kantrip never reads, stores, or passes it. Each item is labeled like
`Kantrip kafka/password (profile UUID)`. `kantrip list` shows the first eight
characters of that UUID, and `kantrip describe PROFILE` shows all of it. In
scripts, look it up in structured output:

```bash
kantrip list -o json | jq -r '.[] | select(.id == "PROFILE-UUID") | .name'
```

Delete credentials with `kantrip remove` or replace them with `kantrip edit`,
not in the vault itself, or the profile can no longer connect.

Only commands that need a credential open the vault: `add` and `edit` when they
store a secret, `remove` of a profile with credentials, `exec`, `ping`, and
`doctor`. `list`, `describe`, `current`, and `--help` never touch it. A running
`kantrip exec` command or shell keeps working after the vault locks, because it
already has its credentials.

The first command that stores a credential creates the vault, and a locked
vault is unlocked when a command needs it. Both ask for the vault password, so
run them in a terminal. Without one, such as in a script, a scheduled job, or
CI, a missing or locked vault fails at once with guidance instead of waiting.

### macOS vault

The vault is a keychain file, `~/Library/Keychains/kantrip.keychain-db`. When
Kantrip creates it, macOS asks for the new password twice on the terminal:

```text
Kantrip keeps credentials in its own keychain, ~/Library/Keychains/kantrip.keychain-db.
Choose a password for it. Kantrip never sees or stores this password; macOS asks for it again after 15 idle minutes and after sleep.
password for new keychain:
retype password for new keychain:
```

Choose a password that differs from your login password. An empty password is
refused and the new vault is removed. Kantrip adds the vault to Keychain Access.
Long values, such as private keys, are split into several items marked
`part 1 of 2`.

The vault locks after 15 idle minutes and when the Mac sleeps. To change that,
select the `kantrip` keychain in Keychain Access and choose Edit > Change
Settings for Keychain "kantrip". `kantrip doctor` shows the current setting
while the vault is unlocked.

When the vault is locked, commands ask for its password on the terminal:

```text
Kantrip vault ~/Library/Keychains/kantrip.keychain-db is locked.
password to unlock /Users/you/Library/Keychains/kantrip.keychain-db:
```

You get three attempts; Ctrl-C cancels and leaves the vault locked. To use the
vault from a script, unlock it beforehand from a terminal:

```bash
security unlock-keychain ~/Library/Keychains/kantrip.keychain-db
```

Delete the vault and every credential in it with:

```bash
security delete-keychain ~/Library/Keychains/kantrip.keychain-db
```

Profiles that stored credentials in it then fail with guidance until you store
each credential again with `kantrip edit PROFILE --replace-secret FIELD`, which
creates a new vault; `kantrip describe PROFILE` lists the fields. If the vault
file disappears some other way, `kantrip doctor --repair` removes its leftover
Keychain Access entry.

Pre-release versions stored credentials in the login keychain under the service
name `kantrip`, and Kantrip no longer reads them. After you add your profiles
again, delete those items in Keychain Access (search the login keychain for
`kantrip`), or run this command until it reports that the item could not be
found:

```bash
security delete-generic-password -s kantrip ~/Library/Keychains/login.keychain-db
```

### Linux vault

The vault lives in your desktop's Secret Service. GNOME, COSMIC, and other
desktops that run GNOME Keyring keep it as the keyring `kantrip`
(`~/.local/share/keyrings/kantrip.keyring`); KDE Plasma keeps it as the wallet
`kantrip` (`~/.local/share/kwalletd/kantrip.kwl`). Kantrip refuses other Secret
Service providers, such as KeePassXC. Manage the vault in Passwords and Keys
(Seahorse) on GNOME or in KDE Wallet Manager on KDE.

Password windows are desktop windows, never terminal prompts. Kantrip prints
what the window is for and waits up to 60 seconds; after that it closes the
window and fails. When Kantrip creates the vault:

```text
Kantrip keeps credentials in its own keyring, 'kantrip'.
Choose a password for it in the window on your desktop; Kantrip waits up to 60 seconds. Kantrip never sees or stores this password.
```

Choose a password that differs from your login password. On GNOME, an empty
password is refused and the new keyring is removed. KDE's wallet wizard
preselects GPG encryption, which fails unless you have a GPG key: choose
Classic.

When the vault is locked, commands print
`Kantrip vault PATH is locked. Enter its password in the window on your desktop`
and the desktop shows the unlock window. Cancel it and the command fails with
the vault still locked. The window needs an unlocked desktop session: over SSH
without a desktop login, or while the screen is locked, it cannot be shown and
the command says so at once. To use the vault from a script, unlock the
`kantrip` keyring or wallet beforehand in Passwords and Keys or KDE Wallet
Manager. A vault that the desktop opens without asking for a password, as
described below, is used even without a terminal.

Neither desktop locks the vault on a timer by default:

- **GNOME and COSMIC** keep it unlocked until you log out, including while the
  screen is locked and after suspend. Lock it yourself in Passwords and Keys.
- **KDE** keeps it open until you log out unless you enable System Settings >
  KDE Wallet > Close when unused for, which takes effect at your next login.
  Neither KDE option closes it on suspend.

`kantrip doctor` reports whether the vault is locked; Linux has no lock setting
for it to report.

Two desktop settings undo the vault's protection, and Kantrip warns about both:

- GNOME's unlock window offers **Automatically unlock this keyring whenever I'm
  logged in**. Leave it unticked: it stores the vault password in your login
  keyring, so your login opens the vault again. When the vault opens without
  asking for its password, the command warns on the terminal and `doctor`
  reports it. To undo it, delete `Unlock password for: kantrip` from the Login
  keyring in Passwords and Keys.
- Never make the vault your **default** keyring or wallet: other applications
  would store their secrets in it. `doctor` warns when it is. On a KDE install
  where no application has opened a wallet yet, the first wallet opened becomes
  the default; Kantrip restores your previous default right away.

Delete the vault and every credential in it by deleting the `kantrip` keyring in
Passwords and Keys, or the `kantrip` wallet in KDE Wallet Manager. Profiles that
stored credentials in it then fail with guidance until you store each
credential again with `kantrip edit PROFILE --replace-secret FIELD`, which
creates a new vault. On KDE, log out and back in before that: KDE keeps listing
a deleted wallet until then.

Pre-release versions stored credentials in the default keyring or wallet, and
Kantrip no longer reads them. After you add your profiles again, delete them
with `secret-tool` (package `libsecret-tools` on Debian and Ubuntu), which
matches only those old items:

```bash
secret-tool clear service kantrip application 'Python keyring library'
```

## Application environment from `kantrip exec`

Kantrip sets these variables only for the programs it starts, never in your
own shell. Kafka has no standard environment variables shared across
languages, so your application has to read the `KAFKA_*` values below itself.
`KANTRIP_*` is reserved for session details.

A supported client run as a one-off command, such as
`kantrip exec local -- kcat -L`, gets only its own generated files, through its
usual options or variables, and none of the `*_CONFIG_FILE` variables below.
Other commands and subshells get all of them.

### Environment precedence

For a one-off command, Kantrip passes on your other exported variables,
removes every `KAFKA_*`, `SCHEMA_REGISTRY_*`, `APICURIO_*`, and
`KANTRIP_SANDBOX_*` variable, and then adds only the profile's public values and
the paths of its private config files. It also removes `KAFKA_OPTS`,
`JAVA_TOOL_OPTIONS`, `JDK_JAVA_OPTIONS`, and `_JAVA_OPTIONS`, so JVM options
can't swap in a different connection. Without a Registry, none of the Registry
variables are set. Your own shell doesn't change.

A subshell runs your usual startup files first. Then Kantrip removes the
reserved variables again, sets its own values back exactly (including
`KCAT_CONFIG`), and puts its wrappers first on `PATH`, so the profile wins over
anything your startup files set by accident. Startup files and the programs you
run are still trusted: they can read or change other variables on purpose.

### Kafka variables

| Variable | Meaning |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated broker addresses |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` or `SSL`, matching the selected profile |
| `KAFKA_JAVA_CONFIG_FILE` | Generated Java Kafka properties path |
| `KAFKA_LIBRDKAFKA_CONFIG_FILE` | Generated librdkafka properties path |
| `KCAT_CONFIG` | Generated librdkafka properties path read natively by kcat |

### Registry variables

Only the variables for the profile's provider are set. The generated
properties file holds that provider's official serializer and deserializer URL
property.

| Variable | Meaning |
| --- | --- |
| `SCHEMA_REGISTRY_URL` | Confluent-compatible registry URL |
| `SCHEMA_REGISTRY_CONFIG_FILE` | Generated file containing `schema.registry.url` |
| `SCHEMA_REGISTRY_KAFKA_CONFIG_FILE` | Private combined Kafka and prefixed Confluent Registry client properties |
| `APICURIO_REGISTRY_URL` | Native Apicurio Core Registry API v3 URL |
| `APICURIO_REGISTRY_CONFIG_FILE` | Generated file containing `apicurio.registry.url` |

### Kantrip session metadata

| Variable | Meaning |
| --- | --- |
| `KANTRIP_PROFILE` | Selected profile display name |
| `KANTRIP_SESSION_ID` | Opaque session identifier |
| `KANTRIP_SESSION_DIR` | Private temporary session directory |

In your own programs, prefer the generated config files, fall back to these
variables, and never log the whole environment or the properties.

## Output and color

Commands write results to stdout and messages to stderr. Styling is off
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

`kantrip list` shows short IDs, endpoints, and labels; `kantrip describe
PROFILE` prints sections. Both can print JSON or YAML with `--output`, which is
syntax-highlighted on a color terminal and plain with `--no-color`, `NO_COLOR`,
`TERM=dumb`, or when the output isn't a terminal. Everything reads fine without
color.

Sensitive values are masked before any styling is applied. Styling never
changes the exit status, and color never carries information you'd miss
without it.
