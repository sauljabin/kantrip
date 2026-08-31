<!--
Use a Conventional Commit title such as `feat(profiles): add profile creation`
or `fix(security): redact registry credentials`. The squash commit title becomes
a release-note entry, so describe one clear outcome in imperative mood.
-->

## Summary

<!-- Explain the problem, motivation, and resulting behavior. -->

Closes #

## Verification

<!-- List the automated and manual checks performed. -->

- [ ] `uv run --locked python -m scripts.analyze`
- [ ] `uv run --locked python -m scripts.tests`
- [ ] Relevant manual or end-to-end checks

## Security and compatibility

<!-- Describe effects on secrets, argv, environment, files, cleanup, public contracts, supported clients, or platforms. Write "None" when not applicable. -->

## User-facing changes

<!-- Describe documentation, output, or compatibility changes. Write "None" when not applicable. -->

## Checklist

- [ ] Tests cover the changed behavior.
- [ ] Documentation, schemas, fixtures, and examples reflect the change.
- [ ] No secrets or private infrastructure details are included.
- [ ] The title follows Conventional Commits and is suitable for release notes.

<!-- Replace the values below with the actual assisting model and version. -->
Assisted-by: <AI model> <version>
