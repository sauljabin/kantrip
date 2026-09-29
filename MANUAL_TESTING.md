# Manual Testing

The unit and E2E suites check what a program can check. These smoke tests
cover what needs a person: the real credential vault, a real terminal and
desktop, and whether the output reads well. Run them once per release candidate
with the built wheel. Each platform takes about 30 minutes.

| Section | macOS | Linux |
| --- | --- | --- |
| 0–6 | Yes | Yes |
| 7. macOS vault | Yes | No |
| 8. Linux vault | No | Yes, on each desktop: Ubuntu (GNOME), Pop!_OS (COSMIC), Kubuntu (KDE) |

Record the candidate commit, OS and desktop, shell, and the result of each
section. For a failure, add the command and its sanitized output.

## 0. Setup

Install the candidate in isolation, so your own Kantrip stays untouched:

```bash
uv build --clear
pipx install --suffix=-qa dist/kantrip-*.whl
alias kantrip=kantrip-qa
```

Keep the test profiles away from your own:

```bash
export QA="$(mktemp -d "${TMPDIR:-/tmp}/kantrip-qa.XXXXXX")"
chmod 700 "$QA" && mkdir -m 700 "$QA/runtime"
export KANTRIP_DATABASE="$QA/profiles.db" XDG_RUNTIME_DIR="$QA/runtime"
```

Start the sandbox for the sections that talk to Kafka:

```bash
uv run --locked python -m sandbox up
```

The SCRAM password is `KANTRIP_SANDBOX_KAFKA_SCRAM_PASSWORD` in
`sandbox/.state/credentials.env`. Copy it from an editor when a prompt asks;
never print it in the terminal.

The vault sections use your real vault. Use a spare account or a live USB
session if you don't want to recreate yours.

Clean up at the end:

```bash
rm -rf "$QA"
pipx uninstall kantrip-qa
uv run --locked python -m sandbox down
```

## 1. First run

A new user's first commands must work without creating any files.

```bash
kantrip --help
kantrip list
kantrip doctor
test ! -e "$KANTRIP_DATABASE" && echo "no database created"
kantrip add local
```

Expect:

- `--help` lists nine commands, and `list` prints nothing.
- `doctor` warns about the missing database and clients, with no errors, and
  reports `Credential vault: … (not created yet)` when you have no vault.
- The `test` line prints `no database created`.
- `add local` creates the profile but no vault, because it stores no secret.

## 2. Credentials stay in the vault

A password you type must never show on screen, and Kantrip must keep it only
in its own vault, never in your login keychain or keyring.

```bash
kantrip add qa-scram -b localhost:9094 --transport tls \
  --ca-file sandbox/.state/ca.crt --auth scram-sha-512 --username kantrip-scram
kantrip describe qa-scram
kantrip doctor qa-scram
kantrip ping qa-scram
```

Paste the SCRAM password (see section 0) when `add` asks. If you have no vault
yet, `add` creates one first (see section 7 or 8).

Expect:

- The password prompt shows nothing while you type or paste.
- `describe` shows the profile `id` and `kafka.auth.password` as `configured`,
  never the value.
- `doctor qa-scram` reports `kafka.auth.password is stored`, and `ping`
  succeeds.
- Your vault manager shows one item, `Kantrip kafka/password (profile …)` with
  the `id` from `describe`, in the `kantrip` keychain, keyring, or wallet:
  Keychain Access (macOS), Passwords and Keys (GNOME, COSMIC), or KDE Wallet
  Manager (KDE). `login` and `kdewallet` get nothing new.

Keep `qa-scram` for the next sections.

## 3. Nothing leaks during a session

A running session must not reveal the secret to other processes, and it must
leave nothing behind when it ends.

Terminal A:

```bash
kantrip exec qa-scram -- sleep 300
```

Terminal B, with the same `QA` variables:

```bash
ps -ww -o args -A | grep -E 'kantrip|sleep 300' | grep -v grep
ls -la "$XDG_RUNTIME_DIR"/kantrip/sessions/*/
```

Expect no password in any process argument, and a session directory with mode
`drwx------` and files with `-rw-------`. Press Ctrl-C in terminal A; running
the `ls` again finds no session.

## 4. Shells and commands behave natively

A Kantrip shell must feel like your own, with your dotfiles, while every client
in it connects through the profile.

Run it once each for Bash, Zsh, and Fish:

```bash
SHELL="$(command -v zsh)" kantrip exec qa-scram   # then bash, then fish
```

Inside the session:

```bash
kcat -L                 # works without connection flags
kafka-topics --list     # same
kantrip exec local      # refused: already inside a session
exit 7
```

Then, back in your shell:

```bash
echo $?                                      # 7
kantrip exec local -- sh -c 'exit 42'; echo $?   # 42
kantrip exec local -- kcat -b other:9092 -L      # refused before launch
```

Expect your prompt, aliases, and history to work inside the session, and the
profile to show in the prompt if you set up an integration from `USAGE.md`.
The sandbox lets `qa-scram` create only topics whose names start with
`kantrip-auth-`; any other name fails with `Authorization failed`.

## 5. Output reads well

Output must read well in a color terminal, and stay free of color codes and
easy to parse when piped.

```bash
kantrip describe qa-scram
kantrip describe qa-scram | cat
kantrip list -o json | jq .
kantrip describe missing 2>/dev/null; echo $?
```

Expect aligned, readable output in a color terminal; no color codes when piped;
valid JSON; and no output for the missing profile, only exit status `1`.

## 6. Doctor finds and explains problems

Every problem `doctor` reports must tell you how to fix it.

```bash
chmod 644 "$KANTRIP_DATABASE"
kantrip doctor
chmod 600 "$KANTRIP_DATABASE"
```

Expect an error about the database permissions that says how to fix it. Then
delete the `qa-scram` item in your vault manager (see section 2) and run:

```bash
kantrip doctor
kantrip edit qa-scram --replace-secret kafka.auth.password
kantrip doctor
```

Expect the first `doctor` to name the missing field, `edit` to ask for the
password again, and the second `doctor` to pass.

## 7. macOS vault

Only on macOS. Every prompt appears in the terminal; no macOS window may appear.
Record the wording if one does.

```bash
V=~/Library/Keychains/kantrip.keychain-db
```

**Create it.** The first stored secret creates the vault, which must refuse an
empty password.

```bash
security delete-keychain "$V"   # only if you have one and can recreate it
kantrip add qa-mac -b localhost:9094 --transport tls \
  --ca-file sandbox/.state/ca.crt --auth scram-sha-512 --username kantrip-scram
```

After the SCRAM password, press Enter at both vault prompts: `add` fails with
`must not be empty`. Run `add` again and choose a password:
`Created ~/Library/Keychains/kantrip.keychain-db.` appears, and `kantrip doctor`
shows `(unlocked)` and `Credential vault locks after 15 minutes idle and on
sleep`.

**Unlock it.** A locked vault asks for its password in the terminal, and only
when a command needs a secret.

```bash
security lock-keychain "$V"
kantrip list              # no prompt
kantrip ping qa-mac       # asks for the vault password
```

Type a wrong password once (`Incorrect password; 2 attempts left.`), then the
right one: `ping` succeeds. Lock it again, run `kantrip ping qa-mac`, and press
Ctrl-C: `Error: Kantrip vault unlock was cancelled`, with no traceback.

**No terminal.** A script must fail at once instead of waiting for a prompt
nobody sees.

```bash
security lock-keychain "$V"
python3 -c 'import subprocess; subprocess.run(["kantrip-qa", "ping", "qa-mac"], start_new_session=True)'
```

Expect, at once and with no prompt: `is locked and there is no terminal to
unlock it`.

**Sleep.** The vault must lock when the Mac sleeps. Unlock it with
`kantrip ping qa-mac`, sleep the Mac for a minute, wake it, and run
`kantrip doctor`: it asks for the password and reports `(locked)`.

## 8. Linux vault

Only on Linux. Run it on each desktop in the table at the top, logged in to the
desktop, from a terminal on that desktop. Password windows are desktop windows;
the terminal only explains them. Manage the vault in Passwords and Keys (GNOME,
COSMIC; install `seahorse` if it's missing) or KDE Wallet Manager (KDE). "Lock"
below means Lock in Passwords and Keys, or Close in KDE Wallet Manager.

**Create it.** The first stored secret creates the vault, which on KDE must not
replace your default wallet.

Delete your `kantrip` keyring or wallet first if you have one and can recreate
it. On KDE, note the default wallet in System Settings > KDE Wallet. Then:

```bash
kantrip add qa-linux -b localhost:9094 --transport tls \
  --ca-file sandbox/.state/ca.crt --auth scram-sha-512 --username kantrip-scram
```

After the SCRAM password, the terminal explains the window, and a desktop
window asks for a new password.

- GNOME and COSMIC: leave the password empty and accept it. `add` fails with
  `must not be empty; the new vault was removed`. Run `add` again with a real
  password: `Created /org/freedesktop/secrets/collection/kantrip.` appears.
- KDE: choose Classic in the wizard (GPG is preselected), then a password.
  `Created ~/.local/share/kwalletd/kantrip.kwl.` appears, and System Settings
  still shows your previous default wallet.

Expect `kantrip doctor` to show `Credential vault: … (unlocked)` with no
warning.

**Unlock it.** A locked vault opens in a desktop window that the terminal
explains, and a command never waits forever for it.

Lock `kantrip` in the vault manager, then:

```bash
kantrip list              # no window
kantrip ping qa-linux     # the terminal says a window is waiting; a window opens
```

- Enter the password: `ping` succeeds.
- Lock it and run `ping` again, then click Cancel:
  `Error: Kantrip vault unlock was cancelled`.
- Lock it, run `ping`, and leave the window alone. After 60 seconds, expect
  `got no answer within 60 seconds`, with the window gone.

**No terminal.** A script must fail at once instead of opening a window.

```bash
setsid -w kantrip-qa ping qa-linux < /dev/null   # with the vault locked
```

Expect, at once and with no window: `is locked and there is no terminal to
unlock it`.

**Locked screen.** When the desktop can't show the password window, the error
must say why. GNOME and COSMIC only. With the vault locked:

```bash
sleep 10; kantrip ping qa-linux
```

Lock the screen within 10 seconds, wait, then unlock it. Expect `window could
not be shown; it needs an unlocked desktop session`.

**Automatic unlock.** GNOME can open the vault with your login password, so
Kantrip must warn that the vault no longer asks for its own. GNOME and COSMIC
only.

Lock the vault, run `kantrip ping qa-linux`, and tick "Automatically unlock
this keyring whenever I'm logged in" before you unlock. Lock it again, then:

```bash
kantrip doctor
```

Expect no window and a warning that the vault `opened without asking for its
password`. To undo it, delete `Unlock password for: kantrip` from the Login
keyring in Passwords and Keys; the next locked command asks in a window again.

**Default vault.** Kantrip must warn when its vault becomes the default, because
other applications would then store their secrets in it. Make `kantrip` the
default keyring or wallet, then run `kantrip doctor`: it warns
`Credential vault is the default keyring` (KDE: `wallet`). Set your previous
default back.

**Missing wallet file.** KDE keeps listing a wallet after its file is gone, and
Kantrip must not open or recreate it. KDE only.

```bash
mv ~/.local/share/kwalletd/kantrip.kwl ~/kantrip-qa.kwl
kantrip doctor
kantrip ping qa-linux
mv ~/kantrip-qa.kwl ~/.local/share/kwalletd/kantrip.kwl
```

Expect `doctor` to report `(not found)` and warn that KDE Wallet still lists
the vault, and `ping` to fail with `does not exist; restore it`, with no
create-wallet wizard.
