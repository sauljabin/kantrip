# Development

Set up a working checkout, run the checks, and use the sandbox. Design
decisions are in [Architecture](ARCHITECTURE.md), security analysis in
[Threat Model](THREAT_MODEL.md), release smoke tests in
[Manual Testing](MANUAL_TESTING.md), and the rules AI agents follow in
[Agent Instructions](AGENT.md). Planned work lives in the
[milestone issues](https://github.com/sauljabin/kantrip/milestones).

## Contents

- [Setup](#setup)
- [Everyday checks](#everyday-checks)
- [Sandbox and E2E tests](#sandbox-and-e2e-tests)
  - [Start the sandbox](#start-the-sandbox)
  - [Install the released clients](#install-the-released-clients)
  - [Run the E2E suite](#run-the-e2e-suite)
  - [When E2E runs](#when-e2e-runs)
- [Vault tests](#vault-tests)
- [Build](#build)
- [Website](#website)
- [Release](#release)

## Setup

You need [uv](https://docs.astral.sh/uv/) on macOS or Linux; it installs the
right Python for you.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh   # or: brew install uv
uv sync --locked
uv run pre-commit install
uv run kantrip --help
```

`uv run kantrip` runs the checkout's code. The pre-commit hook formats, checks,
and, for changes that affect runtime behavior, builds a wheel from your staged
files and runs the E2E suite against the sandbox. A docs-only commit skips E2E.

## Everyday checks

```bash
uv run --locked python -m scripts.styles              # format (black and ruff)
uv run --locked python -m scripts.analyze             # types, lint, spelling, workflows
uv run --locked python -m scripts.tests --suite unit  # offline tests
```

The unit tests need no Kafka, Docker, or clients: shells, terminals, and the
credential vaults are faked.

## Sandbox and E2E tests

The E2E suite runs every supported client against a real Kafka laboratory with
the candidate wheel.

### Start the sandbox

Install Docker, [Kind](https://kind.sigs.k8s.io/), kubectl, and Helm, then:

```bash
uv run --locked python -m sandbox up           # create or reconcile
uv run --locked python -m sandbox status
uv run --locked python -m sandbox credentials  # file and variable names, never values
uv run --locked python -m sandbox down         # delete the cluster, keep credentials
```

The sandbox is one Kind cluster with Strimzi Kafka, Keycloak, Confluent Schema
Registry, and Apicurio Registry, all on loopback:

| Endpoint | Address |
| --- | --- |
| Kafka plaintext, TLS, SCRAM-SHA-512, mTLS, OAuth | `localhost:9092` to `9096` |
| Kafka PLAIN and SCRAM-SHA-256 over TLS | `localhost:9097`, `9098` |
| Schema Registry: plain, HTTPS Basic, OAuth, mTLS | `http://localhost:8081`, `https://localhost:8083`, `8085`, `8086` |
| Apicurio: plain, HTTPS Basic and OAuth | `http://localhost:8082`, `https://localhost:8084` |
| Keycloak | `https://localhost:8443` |

Credentials, the CA, and client properties are in `sandbox/.state` (private and
ignored). Open `sandbox/.state/credentials.env` in an editor when you need a
value; don't print it. To rotate the credentials, run `sandbox down` and delete
that directory. See [Verification](ARCHITECTURE.md#verification) for how the
laboratory is built.

### Install the released clients

Install the clients listed in `tests/e2e/versions.env` (newer releases work
locally), and make sure both `kcat` and its old name `kafkacat` exist:

```bash
mkdir -p ~/.local/bin
ln -s "$(command -v kcat)" ~/.local/bin/kafkacat   # if kafkacat is missing
```

### Run the E2E suite

The suite tests an installed wheel, not the checkout, and runs Kantrip without
a terminal, so unlock your vault first. On Linux, unlock the `kantrip` keyring
or wallet in Passwords and Keys or KDE Wallet Manager; macOS asks for the
password once on the terminal.

```bash
uv build --wheel --out-dir /tmp/kantrip-dist
uv venv /tmp/kantrip-candidate
uv pip install --python /tmp/kantrip-candidate/bin/python /tmp/kantrip-dist/*.whl
export KANTRIP_E2E_KANTRIP=/tmp/kantrip-candidate/bin/kantrip
uv run --locked python -m scripts.tests --suite e2e
```

The suite never starts or stops the sandbox, and it removes only the profiles,
topics, and schemas it creates. A missing client or an unready sandbox is
reported as a setup failure, not a test failure. The full run takes about ten
minutes.

### When E2E runs

- **Pre-commit:** for runtime, dependency, packaging, sandbox, or E2E changes;
  force it with `KANTRIP_E2E_FORCE=1 git commit …`.
- **Pull requests:** only when the `run-e2e` label is newly applied, or on a
  manual workflow run. Remove and reapply the label to run it again.
- **`main` and releases:** on every runtime change, and always against the exact
  release wheel.

## Vault tests

`--suite vault` tests the real credential vault without the sandbox. It locks,
unlocks, and hides the vault, so run it against a throwaway vault, never your
own. Build the candidate and set `KANTRIP_E2E_KANTRIP` as above, then use the
project's Python directly (uv keeps its cache below `HOME`).

On macOS:

```bash
vault_home="$(mktemp -d)"
mkdir -p "$vault_home/Library/Keychains"
security create-keychain -p vault-e2e \
  "$vault_home/Library/Keychains/kantrip.keychain-db" < /dev/null
security set-keychain-settings -l -u -t 3600 "$vault_home/Library/Keychains/kantrip.keychain-db"
HOME="$vault_home" KANTRIP_E2E_VAULT_PASSWORD=vault-e2e \
  .venv/bin/python -m unittest -v tests.e2e.vault_acceptance
```

On Linux, the run starts its own D-Bus session and GNOME Keyring daemon with an
empty-password `kantrip` keyring, the same as CI:

```bash
vault_home="$(mktemp -d)"
mkdir -m 700 "$vault_home/runtime"
mkdir -p -m 700 "$vault_home/.local/share/keyrings"
printf '%s\n' '[keyring]' 'display-name=kantrip' 'ctime=0' 'mtime=0' \
  'lock-on-idle=false' 'lock-after=false' > "$vault_home/.local/share/keyrings/kantrip.keyring"
chmod 600 "$vault_home/.local/share/keyrings/kantrip.keyring"
HOME="$vault_home" XDG_DATA_HOME="$vault_home/.local/share" \
  XDG_RUNTIME_DIR="$vault_home/runtime" dbus-run-session -- bash -c '
    eval "$(printf %s vault-e2e | gnome-keyring-daemon --unlock --components=secrets)"
    .venv/bin/python -m unittest -v tests.e2e.vault_acceptance'
```

The separate runtime directory keeps the new daemon from joining your
desktop's.

## Build

```bash
uv build --clear
uv run --locked python -m scripts.verify_release dist
```

`verify_release` checks the version, entry points, bundled schema and docs, and
runs the sdist's own tests. Versions come from Git tags; an untagged build gets
a development version.

## Website

The site in `site/` is plain HTML, CSS, and JavaScript with no build step.

```bash
uv run --locked python -m scripts.website check    # validate the site and demo commands
uv run --locked python -m scripts.website render   # after editing site/demo.json
uv run --locked python -m scripts.website capture  # rerecord the demo; needs the sandbox
uv run --locked python -m scripts.banner           # regenerate images/banner.svg
```

Preview it under the same `/kantrip/` path GitHub Pages uses:

```bash
pages="$(mktemp -d)" && ln -s "$PWD/site" "$pages/kantrip"
python3 -m http.server 8000 --bind 127.0.0.1 --directory "$pages"
```

Then open `http://127.0.0.1:8000/kantrip/`. Pushes to `main` deploy it.

## Release

Follow the [release checklist](RELEASE_CHECKLIST.md). In short: on a clean,
current `main` with passing checks, push an annotated tag `vMAJOR.MINOR.PATCH`
(or a pre-release `aN`, `bN`, `rcN`). The protected release workflow builds and
verifies the wheel once, runs E2E against it, waits for approval, publishes to
PyPI through trusted publishing, and creates the GitHub Release with generated
notes.
