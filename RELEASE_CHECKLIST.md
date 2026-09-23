# AI Agent Release Checklist

Use this checklist for every release. Complete the shared preparation checks,
the section for the selected release type, then publishing and verification.

Keep this file reusable and unchecked. Record release-specific results in the
agent's task report: candidate version and commit, command results, failures,
justified exceptions, and unavailable checks. Mark a check complete only when
verified; record non-applicable items with a reason. Resolve blockers before
publishing and do not report readiness while required checks remain unverified.

This checklist does not authorize tagging or publication. Use existing explicit
release authorization, requesting it only if absent.

## First Release — Additional Gate

- [ ] Complete the [first-release manual QA](MVP.md#manual-qa--first-release-checklist),
  or its implemented scenarios moved to [Manual Testing](MANUAL_TESTING.md),
  against the candidate wheel on Linux and macOS. Record actual results;
  automated unit and E2E tests do not replace the human checklist.
- [ ] Confirm every required CLI/protocol/authentication/input-format cell in
  [Compatibility](COMPATIBILITY.md) has versioned integration evidence. Record
  conditional and unsupported cells explicitly, including ping proof limits.
- [ ] Complete the final roadmap cleanup: preserve technical decisions in
  `ARCHITECTURE.md`, keep durable agent conventions in `AGENT.md`, align
  `THREAT_MODEL.md`, and move all human QA into `MANUAL_TESTING.md`. Remove the
  temporary roadmap and its links/packaging requirements only after all its
  implementation and verification obligations are complete.
- [ ] Check documentation audiences: `USAGE.md` and `COMPATIBILITY.md` contain
  installed commands and current support, without sandbox, `uv run`, internal-only
  capabilities, or future flags. Developer workflows and agent release steps
  remain in their assigned guides.
- [ ] Confirm the issue #8 site artifact passed PR validation, deployment ran
  only from `main`, and the published site passed the manual accessibility,
  content, asset, and link checks. Record the URL and deployed commit.
- [ ] Establish the first published compatibility boundary. Earlier development
  commits need no compatibility layer or upgrade path; test clean installation
  and safe rejection of incompatible state. Shared prior-release comparison
  checks are not applicable when no earlier release exists; record that reason.

## Shared Preparation — Every Release

- [ ] Record the proposed version, candidate commit, previous release tag, and
  comparison range. Review the complete release diff and Conventional Commit
  history, not just changes from the current task.
- [ ] Confirm the version classification, backward compatibility, known issues,
  and relevant security advisories. Identify breaking changes and required
  migration guidance before choosing the release type.
- [ ] Compare bundled database migrations with the previous release. Confirm
  that no released sequence, name, or payload changed; new sequences are ordered
  and checksummed; and product SemVer is used only as application metadata.
- [ ] Test database creation from empty state and upgrades from every supported
  prior released sequence. Verify rollback, uniquely timestamped private
  backups, preservation of earlier backups, migration history, and
  `PRAGMA user_version` before publishing a schema change.
- [ ] Verify lockfile consistency, code analysis, the offline unit suite, and the
  sandbox E2E suite using the workflows in
  [Development](DEVELOPMENT.md#development-scripts). Confirm E2E used the
  candidate wheel, pinned released clients, and the native platform credential
  store rather than Kantrip's development environment.
- [ ] Confirm successful [CI](.github/workflows/main.yml) for the final candidate,
  including the supported Python and Linux/macOS matrix and packaging jobs.
  Confirm a separate full [E2E acceptance](.github/workflows/e2e.yml) run for
  the candidate commit; pull-request CI does not run it. Refresh evidence for
  changes made during release preparation.
- [ ] Build and verify the wheel and source distribution using
  [Build artifacts](DEVELOPMENT.md#build-artifacts). Install the wheel in an
  isolated environment and smoke-test `kantrip --version`, `kantrip --help`, and
  `kantrip doctor`. An untagged candidate has a development version; the release
  workflow verifies the exact tag version.
- [ ] Review release-note coverage against the comparison range, including
  user-facing fixes, features, breaking changes, and known limitations. Keep
  GitHub Releases as the canonical changelog; do not add a maintained changelog
  or a static version field.

## Pre-release — Focused Review

- [ ] Confirm the release uses the next unused PEP 440 `aN`, `bN`, or `rcN` tag
  for its stage and is intentionally not production-ready. Record the unstable
  or incomplete areas that need tester feedback.
- [ ] Review the README, usage guide, command-compatibility matrix, examples, and
  security guidance for claims affected by this prerelease.
- [ ] Run focused manual sandbox checks for changed adapters and shell behavior.
  Record unavailable external clients or Docker rather than claiming a pass.
- [ ] Confirm the GitHub release will be marked as a prerelease and that install
  guidance uses an explicit prerelease version.

## Major Release — Comprehensive Review

- [ ] Review all documentation for accuracy, outdated or contradictory guidance,
  broken links, and duplication. Keep detailed instructions in their canonical
  document and link to them.
- [ ] Update README capabilities, installation instructions, examples, and the
  command-compatibility matrix to reflect the release.
- [ ] Review runtime dependencies, development tools, GitHub Actions, pre-commit
  hooks, and sandbox images against their latest stable releases. Document
  unresolved upgrade blockers and do not raise minimum versions without need.
- [ ] Compact `AGENT.md` by removing duplication and obsolete guidance without
  losing unique instructions.
- [ ] Review CLI, configuration, profile schema, adapters, shells, and security
  compatibility. Document breaking changes and actionable migration steps.
- [ ] Run the documented [manual sandbox checks](MANUAL_TESTING.md),
  including the end-to-end adapter workflow with supported clients and shells.

## Minor Release — Focused Feature Review

- [ ] Confirm new functionality is backward compatible. Reclassify breaking
  changes as a major release and use the major checklist.
- [ ] Review and update documentation and agent instructions for affected
  behavior; avoid an unrelated repository-wide audit.
- [ ] Update dependencies needed for features, fixes, security, or compatibility.
  A wholesale dependency upgrade is not required.
- [ ] Run additional manual checks for changed behavior and its integration paths,
  using the development guide and agent conventions.
- [ ] Review release notes for new features, fixes, and relevant limitations.

## Patch Release — Focused Correction Review

- [ ] Confirm changes are backward-compatible fixes, security corrections, or
  maintenance. Reclassify new features as minor and breaking changes as major.
- [ ] Verify each fix with a focused regression test or a reproducible
  before/after check, recording the evidence.
- [ ] Review affected behavior and nearby integration paths for regressions; run
  focused manual checks where applicable.
- [ ] Correct documentation and agent instructions where the patch makes them
  inaccurate. Skip broad documentation audits and full `AGENT.md` compaction.
- [ ] Limit dependency updates to those required by fixes, security, or
  compatibility; skip wholesale upgrades.
- [ ] Summarize corrected behavior and remaining known issues in release notes.

## Publishing and Post-release Verification

- [ ] Present preparation results, resolve blockers, and confirm release
  authorization before creating or pushing a tag. Follow
  [Release](DEVELOPMENT.md#release) for a clean, current `main`, tag creation,
  protected approvals, and failure recovery.
- [ ] Follow the [release workflow](.github/workflows/release.yml) through tag
  validation, artifact verification, complete E2E against the exact built wheel,
  attestation, and protected publishing. Preserve its build-once distribution
  bundle and review generated release notes.
- [ ] Verify the expected version is available on PyPI and installs successfully
  in an isolated environment; check version reporting, CLI help, and diagnostics.
- [ ] Verify the GitHub release tag, prerelease status when applicable,
  downloadable wheel and source distribution, release notes, and links match the
  intended release.
- [ ] If publication fails, record partial publication accurately. Do not move or
  reuse release tags or replace immutable published artifacts.
