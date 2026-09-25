# Manual Testing

The unit suite and the sandbox E2E suite (`python -m scripts.tests --suite e2e`)
cover behavior that a program can check. This guide lists only what needs a
person: real OS credential stores, real terminals and shells, and readability.
Run it once per release candidate on macOS and on one Linux distribution,
using the built wheel. There is no upgrade check: no backward compatibility is
promised before v0.1.0.

Record for each scenario: candidate commit, OS, shell, client versions, result,
and (sanitized) evidence for any failure.

## 0. Setup

Build the candidate and install it in isolation:

```bash
uv build --clear
pipx install --suffix=-qa dist/kantrip-*.whl
alias kantrip=kantrip-qa
kantrip --version
```

Use a disposable state directory for every scenario:

```bash
export QA="$(mktemp -d "${TMPDIR:-/tmp}/kantrip-qa.XXXXXX")"
chmod 700 "$QA" && mkdir -m 700 "$QA/runtime"
export KANTRIP_DATABASE="$QA/profiles.db" XDG_RUNTIME_DIR="$QA/runtime"
```

For scenarios that need Kafka, start the sandbox and run the automated suite
first; the human checks below assume it is green:

```bash
uv run --locked python -m sandbox up
uv run --locked python -m scripts.tests --suite e2e
uv run --locked python -m sandbox credentials   # lists files and variable names, never values
```

Passwords and client secrets are in `sandbox/.state/credentials.env`. Open it
in an editor when a prompt needs a value; never print it in the terminal.

Clean up at the end: `rm -rf "$QA"`, `pipx uninstall kantrip-qa`.

## 1. First run on a clean machine

```bash
kantrip --help
kantrip list
kantrip doctor
test ! -e "$KANTRIP_DATABASE" && echo "no database created"
kantrip add local
kantrip list
```

Expect: help lists nine commands; `list` prints nothing and exits 0; `doctor`
warns about the missing database and missing clients without errors and does
not create the database; `add local` states the resulting connection and the
database path.

## 2. Credentials live only in the OS store

```bash
kantrip add qa-scram -b localhost:9094 --transport tls \
  --ca-file sandbox/.state/ca.crt --auth scram-sha-512 --username kantrip-scram
```

1. The password prompt does not echo. Enter the SCRAM password.
2. Open Keychain Access (macOS) or Seahorse/KWallet (Linux). Expect one item
   with service `kantrip` and an account like `profile/<uuid>/<uuid>/kafka/password`.
3. `sqlite3 "$KANTRIP_DATABASE" .dump | grep -c 'THE_PASSWORD'` prints `0`.
4. `kantrip ping qa-scram` succeeds.
5. Rotate with a wrong value, then the right one:
   `kantrip edit qa-scram --replace-secret kafka/password` (twice, with ping
   after each). Expect an authentication failure, then success, and exactly
   one keychain item after each rotation.
6. `kantrip remove qa-scram` → decline → nothing changes; again with
   `--force` → the keychain item is gone.
7. macOS only: the first access may show a Keychain permission dialog; record
   the wording and which executable it names (pipx venv Python).


## 3. No secrets leak during a session

Terminal A:

```bash
kantrip add qa-mtls -b localhost:9095 --transport tls --ca-file sandbox/.state/ca.crt \
  --auth mtls --client-certificate-file sandbox/.state/user.crt \
  --client-key-file sandbox/.state/user.key
kantrip exec qa-mtls -- sleep 300
```

Terminal B (same `QA` variables):

```bash
ps -ww -o pid,args -A | grep -E 'kantrip|sleep 300' | grep -v grep
ls -la "$XDG_RUNTIME_DIR/kantrip/sessions/"*/
kantrip doctor qa-mtls --sessions
```

Expect: no key material, password or token in any process argument; session
directory mode `0700`, files `0600`. Then `kill -9` the Kantrip supervisor PID
(not `sleep`), wait five minutes, and run `kantrip doctor` → reports a stale
session; `kantrip doctor --repair` removes it. In the normal case (Ctrl-C in
A), the session directory disappears immediately.

## 4. Interactive shells feel native

Run with your own dotfiles, once each for Bash, Zsh and Fish
(`SHELL=/bin/zsh kantrip exec local`, and so on):

- The prompt shows the profile if you configured an integration from `USAGE.md`.
- `alias kcat='echo shadowed'` in your rc file → `kcat -L` still runs kcat.
- `kafka-topics --list` and `kcat -L` work without extra flags.
- Ctrl-C interrupts a running `kcat -C -t qa` but not the shell.
- Ctrl-Z then `fg` resumes it; resizing the window works in `less` or `vim`.
- `exit 7` returns `7` to the parent shell; `echo $KANTRIP_PROFILE` is empty afterwards.
- `kantrip exec local` inside the session is refused with a clear message.

## 5. One-off commands, signals and exit codes

```bash
kantrip exec local -- sh -c 'exit 42'; echo $?          # 42
kantrip exec local -- sleep 100   # press Ctrl-C        # 130
kantrip exec local -- kafka-topics --help | head -3     # the tool's own help
kantrip exec local -- kcat -b other:9092 -L; echo $?    # refused before launch
kantrip exec local -- does-not-exist; echo $?           # clear not-found message
```

## 6. Output is readable

- In a color terminal: `kantrip list`, `describe`, `doctor` and `ping` are
  readable and aligned; status labels make sense without color.
- `NO_COLOR=1 kantrip describe local` and `kantrip describe local | cat` contain no ANSI escapes.
- `kantrip list -o json | jq .` and `kantrip describe local -o yaml` parse.
- Errors go to stderr: `kantrip describe missing 2>/dev/null` prints nothing.
- Error messages for common mistakes say what to do: missing `--username`,
  bootstrap server without a port, `--registry-provider` without URL,
  authentication on a plaintext profile.

## 7. Real authentication end to end (sandbox)

The E2E suite already covers every listener and Registry combination. Do one
human pass per family to judge the experience:

| Profile | Command |
| --- | --- |
| SCRAM (from §2) | `kantrip exec qa-scram -- kafka-console-producer --topic qa` then a consumer |
| mTLS (from §3) | `kantrip exec qa-mtls -- kcat -L` |
| OAuth | `kantrip add qa-oauth -b localhost:9096 --transport tls --ca-file sandbox/.state/ca.crt --auth oauth --oauth-token-url https://localhost:8443/realms/kantrip/protocol/openid-connect/token --oauth-client-id CLIENT_ID --oauth-ca-file sandbox/.state/ca.crt` then `ping` and `kafka-topics --list` |
| Confluent Registry Basic | `kantrip add qa-sr -b localhost:9092 --registry-url https://localhost:8083 --registry-ca-file sandbox/.state/ca.crt --registry-auth basic --registry-username USER` then `ping` and an Avro console producer/consumer round trip |
| Apicurio OAuth | `--registry-provider apicurio --registry-url https://localhost:8084/apis/registry/v3 --registry-auth oauth …` then `kantrip exec qa-apicurio -- kaskade consumer -t TOPIC -v registry` |

For each: `describe` shows the identity used (username / client ID) and no
secret; a wrong password or client secret gives an authentication error that
names the service, not a stack trace; `ping` prints one line per service.

## 8. Concurrent edits are safe

Terminal A: `kantrip exec qa-scram -- sleep 120`. Terminal B:
`kantrip edit qa-scram -d 'changed'` then `kantrip doctor qa-scram --sessions`.
Expect: the edit succeeds; the running session is reported with the older
revision; the running command keeps working.

## 9. Diagnose and repair

Break things one at a time and read `kantrip doctor` each time:

```bash
chmod 644 "$KANTRIP_DATABASE"      # expect: unsafe permissions error with the fix
chmod 600 "$KANTRIP_DATABASE"
# delete the qa-scram keychain item in Keychain Access / Seahorse
kantrip doctor                     # expect: missing credential named by profile and field
kantrip edit qa-scram --replace-secret kafka/password   # expect: recovers
```

Expect every message to name the problem and the next command to run.
