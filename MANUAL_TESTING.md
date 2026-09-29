# Manual Testing

The unit suite and the sandbox E2E suite (`python -m scripts.tests --suite e2e`)
cover behavior that a program can check. This guide lists only what needs a
person: real OS credential stores, real terminals and shells, and readability.
Run it once per release candidate on macOS and on one Linux distribution,
using the built wheel; run §11 on each Linux desktop listed there. There is no
upgrade check: no backward compatibility is promised before v0.1.0.

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
not create the database. Without a vault, `doctor` passes
`Credential vault: … (not created yet)` (on macOS
`~/Library/Keychains/kantrip.keychain-db`, on GNOME
`/org/freedesktop/secrets/collection/kantrip in GNOME Keyring`, on KDE
`~/.local/share/kwalletd/kantrip.kwl in KDE Wallet`) and creates nothing. `add local` stores no credential, so it creates no vault,
and a second `doctor` still reports no vault warning; `add` states the
resulting connection and the database path.

## 2. Credentials live only in the OS store

```bash
kantrip add qa-scram -b localhost:9094 --transport tls \
  --ca-file sandbox/.state/ca.crt --auth scram-sha-512 --username kantrip-scram
```

1. The password prompt does not echo. Enter the SCRAM password. Without a
   Kantrip vault, `add` first creates one (see §10 on macOS, §11 on Linux).
2. Open Keychain Access (macOS), Passwords and Keys (GNOME), or KDE Wallet
   Manager (KDE). Expect one item labeled
   `Kantrip kafka/password (profile <uuid>)` with service `kantrip` and an
   account like `profile/<uuid>/<uuid>/kafka/password`, in the `kantrip`
   keychain, keyring, or wallet, not in `login` or `kdewallet`.
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
# delete the qa-scram item in Keychain Access, Passwords and Keys, or KDE Wallet Manager
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

## 11. Vault lock (Linux)

Run this on each Linux desktop the release claims: GNOME Keyring on Ubuntu
(GNOME) and on Pop!_OS (COSMIC), and KDE Wallet on Kubuntu (Plasma). It uses
your real vault; use a spare account or a live USB session if you don't want to
recreate yours. Sign in to the desktop, then run the commands from a terminal
there unless a step says otherwise. Keep `qa-scram` from §2. Every password
window is a desktop window; the terminal only explains it.

1. **Creation.** On an account without the vault, run the §2 `kantrip add`.
   After the SCRAM password, the terminal prints
   `Kantrip keeps credentials in its own keyring, 'kantrip'.` (KDE: `wallet`,
   plus `choose Classic unless you have a GPG key`) and a desktop window asks
   for the new password.
   - GNOME/COSMIC: leave both fields empty and accept storing unencrypted:
     `add` fails with `must not be empty; the new vault was removed`, and
     Passwords and Keys shows no `kantrip` keyring. Run `add` again and choose
     a password; `add` prints
     `Created /org/freedesktop/secrets/collection/kantrip.`
   - KDE: the wizard preselects GPG. Choose Classic, then a password twice.
     `add` prints `Created ~/.local/share/kwalletd/kantrip.kwl.`
2. **Report.** `kantrip doctor` shows `Credential vault: … (unlocked)` with the
   collection path (and on KDE the wallet file), no `Credential vault locks …`
   line, and no warning. On KDE, System Settings > KDE Wallet still shows your
   previous default wallet, not `kantrip`, even on a fresh install where
   `~/.config/kwalletrc` had no `First Use=false` before step 1.
3. **Locked, read-only commands.** Lock the vault in Passwords and Keys
   (right-click `kantrip` > Lock) or KDE Wallet Manager (Close). `kantrip list`
   and `kantrip describe qa-scram` finish without a window. `kantrip doctor`
   prints `Kantrip vault … is locked. Enter its password in the window on your
   desktop; Kantrip waits up to 60 seconds.` and a window asks for the password.
   Enter it: the report shows `(locked)` (the state before doctor unlocked it)
   and `Profile 'qa-scram' kafka.auth.password is stored`.
4. **Cancel and timeout.** Lock the vault, run `kantrip ping qa-scram`, and
   click Cancel: expect `Error: Kantrip vault unlock was cancelled`, exit status
   1, and the vault still locked. Run it again and ignore the window: after 60
   seconds expect `got no answer within 60 seconds, so Kantrip closed the
   window` and the window gone from the screen. Run it again and press Ctrl-C
   in the terminal: expect `unlock was cancelled`, no traceback, and the window
   gone.
5. **No terminal.** With the vault locked, run
   `setsid kantrip-qa exec qa-scram -- true < /dev/null`. Expect, within a
   second and with no window: `is locked and there is no terminal to unlock
   it`. Unlock the vault in the keyring or wallet manager and run the same
   command: it succeeds silently.
6. **No desktop session.** From another machine, `ssh` into this account while
   the desktop is logged in and its screen unlocked, lock the vault, and run
   `kantrip ping qa-scram`: the window appears on the laptop, not in SSH.
   Lock the screen (GNOME/COSMIC) and run it again: expect at once `window could
   not be shown; it needs an unlocked desktop session`. Log out of the desktop
   and run it again: GNOME fails the same way; KDE reports
   `no Secret Service is running for this user`.
7. **Automatic unlock (GNOME and COSMIC).** Lock the vault, run
   `kantrip doctor`, and tick "Automatically unlock this keyring whenever I'm
   logged in" in the window before unlocking. Lock the vault again and run
   `kantrip exec qa-scram -- true`: it opens with no window, prints
   `Warning: Kantrip vault … opened without asking for its password`, and
   `kantrip doctor` reports the same warning. In Passwords and Keys, confirm the
   Login keyring holds `Unlock password for: kantrip`, delete it, lock the vault,
   and check that the next command asks in a window again.
8. **Default keyring.** Make `kantrip` the default (Passwords and Keys:
   right-click > Set as Default; KDE: System Settings > KDE Wallet). `kantrip
   doctor` warns `Credential vault is the default keyring` (KDE: `wallet`).
   Set your previous default back; the warning disappears.
9. **Lock policy.** Unlock the vault, leave it idle for 16 minutes, lock the
   screen, and suspend for a minute: on GNOME and COSMIC `kantrip doctor` still
   shows `(unlocked)`. On KDE, enable Close when unused for 1 minute, log out
   and back in, unlock the vault, wait 2 minutes: `kantrip doctor` shows
   `(locked)`. Turn the setting off again. Log out and back in on every
   desktop: the vault is `(locked)` after login.
10. **Missing vault.** Move the vault file away
    (`~/.local/share/keyrings/kantrip.keyring` or
    `~/.local/share/kwalletd/kantrip.kwl`, plus its `.salt`). Because `qa-scram`
    stores a credential, `kantrip doctor` reports `(not found)` as an error; on
    KDE it also warns `KDE Wallet still lists the credential vault without its
    file`. `kantrip exec qa-scram -- true` fails with `does not exist; restore
    it`, and on KDE no create-wallet wizard appears. Move the files back: the
    vault is `(locked)` again and usable after you unlock it.
