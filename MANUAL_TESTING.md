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
not create the database. On macOS without a vault, `doctor` passes
`Credential vault: ~/Library/Keychains/kantrip.keychain-db (not created yet)`
and creates nothing. `add local` stores no credential, so it creates no vault,
and a second `doctor` still reports no vault warning; `add` states the
resulting connection and the database path.

## 2. Credentials live only in the OS store

```bash
kantrip add qa-scram -b localhost:9094 --transport tls \
  --ca-file sandbox/.state/ca.crt --auth scram-sha-512 --username kantrip-scram
```

1. The password prompt does not echo. Enter the SCRAM password. On macOS
   without a Kantrip vault, `add` first creates one (see §10).
2. Open Keychain Access (macOS) or Seahorse/KWallet (Linux). Expect one item
   with service `kantrip` and an account like `profile/<uuid>/<uuid>/kafka/password`.
   On macOS it is in the `kantrip` keychain, not `login`, labeled
   `Kantrip kafka/password (profile <uuid>)`.
3. `sqlite3 "$KANTRIP_DATABASE" .dump | grep -c 'THE_PASSWORD'` prints `0`.
4. `kantrip ping qa-scram` succeeds.
5. Rotate with a wrong value, then the right one:
   `kantrip edit qa-scram --replace-secret kafka.auth.password` (twice, with ping
   after each). Expect an authentication failure, then success, and exactly
   one keychain item after each rotation.
6. `kantrip remove qa-scram` → decline → nothing changes; again with
   `--yes` → the keychain item is gone.
7. macOS only: no macOS permission or password window appears at any step;
   record the wording and the named executable if one does.


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
kantrip doctor qa-mtls
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
`kantrip edit qa-scram -d 'changed'` then `kantrip doctor qa-scram`.
Expect: the edit succeeds; the running session is reported with the older
revision; the running command keeps working.

## 9. Diagnose and repair

Break things one at a time and read `kantrip doctor` each time:

```bash
chmod 644 "$KANTRIP_DATABASE"      # expect: unsafe permissions error with the fix
chmod 600 "$KANTRIP_DATABASE"
# delete the qa-scram keychain item in Keychain Access / Seahorse
kantrip doctor                     # expect: missing credential named by profile and field
kantrip edit qa-scram --replace-secret kafka.auth.password   # expect: recovers
```

Expect every message to name the problem and the next command to run.

## 10. Vault lock (macOS)

This uses your real vault, `~/Library/Keychains/kantrip.keychain-db`. Run it on
a spare macOS account if you don't want to recreate yours. Keep `qa-scram` from
§2. Throughout, no macOS window may appear: every prompt is on the terminal.
`V` below is the vault path:

```bash
V=~/Library/Keychains/kantrip.keychain-db
```

1. **Creation.** On an account without the vault, run the §2 `kantrip add`.
   After the SCRAM password, it prints what the vault is and asks for a new
   vault password twice. Press Enter at both prompts: `add` fails with
   `must not be empty`, `test ! -e "$V"` succeeds, and `kantrip list` shows no
   `qa-scram`. Run the same `add` again and type two different vault passwords:
   macOS says `passwords don't match` and asks again; then type the same
   password twice. `add` prints `Created ~/Library/Keychains/kantrip.keychain-db.`
   and adds the profile. Keychain Access now lists a `kantrip` keychain whose
   item is labeled `Kantrip kafka/password (profile …)`.
2. **Report.** `kantrip doctor` shows
   `Credential vault: ~/Library/Keychains/kantrip.keychain-db (unlocked)` and
   `Credential vault locks after 15 minutes idle and on sleep`.
3. **Locked, read-only commands.** Run `security lock-keychain "$V"`.
   `kantrip list` and `kantrip describe qa-scram` finish without a prompt.
   `kantrip doctor` asks for the vault password before it prints anything;
   after you enter it, the report shows `(locked)` (the state before doctor
   unlocked it) and `Profile 'qa-scram' kafka.auth.password is stored`.
4. **Wrong password.** Run `security lock-keychain "$V"`, then
   `kantrip exec qa-scram -- true`. Type a wrong password: expect
   `Incorrect password; 2 attempts left.` and a new prompt. Type the right one:
   the command runs and exits 0.
5. **Cancel.** Run `security lock-keychain "$V"`, then `kantrip ping qa-scram`,
   and press Ctrl-C at the password prompt. Expect
   `Error: Kantrip vault unlock was cancelled`, exit status 1, no traceback,
   and the terminal still echoes what you type.
6. **No terminal.** Scripts, scheduled jobs, and CI have no terminal, so
   Kantrip cannot ask for the password and must fail at once instead of
   waiting. A command started from your shell always has your terminal, even
   with `< /dev/null`, so detach it: macOS has no `setsid` command, so
   Python's `start_new_session` starts Kantrip without a controlling terminal.
   Use the installed executable, because the `kantrip` alias from §0 does not
   exist inside Python:

   ```bash
   security lock-keychain "$V"
   python3 -c 'import subprocess; subprocess.run(["kantrip-qa", "exec", "qa-scram", "--", "true"], start_new_session=True)'
   ```

   Expect, within a second and with no prompt or window:
   `Error: Kantrip vault ~/Library/Keychains/kantrip.keychain-db is locked and
   there is no terminal to unlock it; run the command in a terminal, or first
   run 'security unlock-keychain ~/Library/Keychains/kantrip.keychain-db'`.
   Running that `security unlock-keychain` command, then the same Python line,
   succeeds silently.
7. **Running sessions survive a lock.** Start `kantrip exec qa-scram` (unlock
   it if asked). From another terminal, run `security lock-keychain "$V"`.
   Inside the session, `kcat -L` still works, because the session already has
   its credentials.
8. **Automatic lock.** Leave the vault unlocked and unused for 16 minutes, then
   run `kantrip doctor`: it asks for the password, and the report shows
   `(locked)`. Put the Mac to sleep for a minute, wake it, and run
   `kantrip doctor` again: it asks again and shows `(locked)`.
9. **User lock setting.** In Keychain Access, select the `kantrip` keychain and
   choose Edit > Change Settings for Keychain "kantrip". Set 5 minutes. With the
   vault unlocked, `kantrip doctor` shows
   `Credential vault locks after 5 minutes idle and on sleep`. Set it back to
   15 minutes.
10. **Missing vault.** Move the vault away:
    `mv "$V" ~/kantrip-qa.keychain-db`. Because `qa-scram` stores a credential,
    `kantrip doctor` reports an error,
    `Credential vault: ~/Library/Keychains/kantrip.keychain-db (not found)`,
    warns `Keychain Access still lists the missing credential vault; run
    'kantrip doctor --repair'`, and reports `Profile credentials were not checked: …
    does not exist; restore it, or store the credential again with 'kantrip
    edit PROFILE --replace-secret FIELD'`. `kantrip exec qa-scram -- true`
    fails with the same guidance. `kantrip doctor --repair` prints `Removed the
    missing credential vault from the keychain search list`, and a second
    `kantrip doctor` no longer shows the Keychain Access warning. Move the file
    back with `mv ~/kantrip-qa.keychain-db "$V"`; `kantrip exec qa-scram -- true`
    works again after you unlock it.
