---
status: approved
createdAt: 2026-11-23T08:02:44Z
updatedAt: 2026-11-23T08:05:01Z
approvedAt: 2026-11-23T08:05:01Z
---

# Requirements: Adopt pernix-ci CI/CD

**Spec ID:** 2026-11-23-cicd-adoption
**Author:** Architect (Orca task task_dcc7978760bc)

## Introduction

P7. Base `origin/main` at `26f7207` (PR #3). Replace every platform-cicd assumption with shared `Pernix-Labs/pernix-ci` at `v1.0.0` (commit `50c1228ba4f77d3219a270fbf3dcfee7ccf84781`, same as moving `v1`). Version comes from release tag only. Human rulings 2026-11-22 are binding and restated as requirements below.

Current state (facts):
- No `.github/` in repo. No GitHub environment, no repo variable (`gh api` lists none).
- Version files: `.version` (`1.0`), read by `src/RelayPackage.props` `VersionPrefix`, copied by `Dockerfile` (`COPY ... ".version"`). `client/PackageVersion.props` (`VersionPrefix` `2.0.0`), imported by 3 client csprojs.
- `Directory.Build.props`: `TreatWarningsAsErrors` plus `WarningsNotAsErrors` `NU1507` with platform-cicd comment. No `NuGetAudit*` properties.
- `client/Directory.Build.props` comment and 3 client csprojs: `TreatAsLocalProperty="TargetFramework"` plus comment, workaround for platform-cicd global `-p:TargetFramework=net10.0`.
- Docs naming version files / old pipeline: `docs/development.md` (`BUILD_VERSION` default), `docs/embedding.md:19`, `docs/client-packages.md:769`, `docs/architecture/outbox-relay-host.md:66`, ADR 0008 (`VersionPrefix`, `publish-client-nuget*.yml` to GitHub Packages), ADR 0011 (`.version`, `client/PackageVersion.props`, NG8 follow-up). `docs/runbook.md` names no version.

pernix-ci v1.0.0 facts (README, `templates/`, `docs/workflows/`, `actions/`):
- F1. No block or reusable workflow passes a global `TargetFramework`. `dotnet-build` rejects build args that override `TargetFramework`, warning or `NuGetAudit` settings. So `TreatAsLocalProperty` workaround is dead.
- F2. Blocks never write `nuget.config`. Repo `NuGet.Config` has `packageSourceMapping`, so `NU1507` never fires.
- F3. `nuget-pack` passes `-p:Version` and `-p:PackageVersion` = tag version, and checks each `.nuspec` version.
- F4. Image `BUILD_VERSION` build arg and OCI `version` label = CI version `0.0.0-ci.g<L[:7]>`, never release version. Promote retags `sha-<L>` to `X.Y.Z[-pre]` without rebuild (pernix-ci ADR 0010). Relay `SERVICE_VERSION` (`RelayIdentity`) in a released image therefore reads `0.0.0-ci.g<L[:7]>`.
- F5. `release-units` auto-detects one `*.slnx` (`OutboxRelay.slnx`). Test project joins every unit whose non-test closure project it references directly. `tests/Pernix.OutboxRelay.Unit.Tests` refs `Pernix.OutboxRelay.Client`, `tests/Pernix.OutboxRelay.Integration.Tests` refs `Pernix.OutboxRelay.Client.PostgreSql`: both join `client` unit too, pulling relay projects into `client` closure and client projects into `relay` inputs.
- F6. Benchmark projects (`benchmarks/`, `client/benchmarks/`) set `IsTestProject=false` and are referenced by nothing: in no unit, never built by CI.
- F7. All test projects target `net10.0`: `dotnet-setup` from `global.json` suffices. Integration tests use Testcontainers. `dotnet-test` requires Docker, present on `ubuntu-latest`.
- F8. Image unit inputs: closure project dirs, ancestor `Directory.Build.*`, `Directory.Packages.props`, `global.json`, `nuget.config`, `dockerfile`, `<context>/.dockerignore`. `src/RelayPackage.props` is outside these: needs `extra-inputs`.
- F9. Caller setup also needs repo variable `NUGET_USER` (nuget.org policy owner profile) and GHCR package Admin for the repo (default when this repo's workflow pushes first).
- F10. No CD dry-run path in v1.0.0: reusable `cd.yml` has no dry-run input. Only `image-retention.yml` has `dry-run` (`workflow_dispatch`, runnable only once on default branch).
- F11. README: callers with Trusted Publishing pin `vX.Y.Z` or full sha, never `v1`, same ref on every pernix-ci line. Templates already pin `@v1.0.0`.

## Alignment with Product Vision

Relay image and two package lines become releasable from this repo with the org-standard gates (secret scan, NuGet vulnerability gate, image scan, tests). No bespoke pipeline to maintain.

## Goals

- **G1.** `.version`, `client/PackageVersion.props` and every reference to them are gone. Release version comes only from tag `<unit>/vX.Y.Z[-pre]`.
- **G2.** `.github/release-units.yml` declares units `relay` and `client`. `.github/workflows/ci.yml`, `cd.yml`, `retention.yml` follow pernix-ci templates pinned `@v1.0.0`.
- **G3.** `relay/vX.Y.Z` tag promotes relay image in GHCR and pushes 4 relay packages to nuget.org. `client/vX.Y.Z` pushes 3 client packages. Push uses Trusted Publishing only.
- **G4.** No platform-cicd assumption left: no `NU1507` exception, no `TreatAsLocalProperty` workaround, no comment naming platform-cicd or a CI global `TargetFramework`.
- **G5.** `Directory.Build.props` sets `TreatWarningsAsErrors` with no exception, `NuGetAuditMode=all`, `NuGetAuditLevel=high`.
- **G6.** GitHub environment `release` exists, deployments limited to `relay/v*` and `client/v*` tags. Repo variable `NUGET_USER` set.
- **G7.** CI green on the spec PR. `cd.yml`, `ci.yml`, `retention.yml` pass actionlint.
- **G8.** Docs and new ADR 0013 describe the CI/CD and release flow. ADR 0013 supersedes version-file statements in ADR 0008 and 0011.

## Non-goals

- **NG1.** Pushing a release tag or publishing any package/image release. Ruling: do not push a release tag.
- **NG2.** Changes to pernix-ci. Consumed as-is at `v1.0.0`.
- **NG3.** Making released image report release version in `SERVICE_VERSION` (F4). pernix-ci design: promote never rebuilds. See Q1.
- **NG4.** Restructuring test projects to stop cross-unit membership (F5). Effect is extra CI work only: `client` leg runs relay tests, client edits move relay `L` and rebuild relay image. No correctness impact.
- **NG5.** CI build of benchmark projects (F6). pernix-ci gates units only. See Q3.
- **NG6.** Migration or "legacy" guard code, compatibility shims for old version files. Ruling.
- **NG7.** Package metadata changes (license, readme, description) beyond version removal. Not CI/CD.
- **NG8.** Branch protection / required status checks. Not requested.

## Requirements

### Requirement 1: Tag-only versioning

**User Story:** As a maintainer, I want the release tag to be the only version source, so that no file drifts from what ships.

#### Acceptance Criteria

1. WHEN the change merges THEN repo SHALL contain neither `.version` nor `client/PackageVersion.props`.
2. WHEN `src/RelayPackage.props` is evaluated THEN it SHALL set no `VersionPrefix`, `Version` or `PackageVersion`.
3. WHEN any client csproj is evaluated THEN it SHALL NOT import `PackageVersion.props` and SHALL set no `VersionPrefix`, `Version` or `PackageVersion`.
4. WHEN `Dockerfile` is built THEN it SHALL NOT `COPY` `.version`.
5. WHEN repo is searched (excluding `docs/specs/`) for `.version` (file reference), `PackageVersion.props` or `VersionPrefix` THEN no match SHALL remain except in ADR 0008/0011 text marked superseded by ADR 0013.
6. WHEN `nuget-pack` packs a project at version `V` THEN `.nupkg` version SHALL equal `V` (F3).

### Requirement 2: Release units

**User Story:** As a maintainer, I want two release units, so that relay and client release independently.

#### Acceptance Criteria

1. WHEN `release-units` reads `.github/release-units.yml` THEN it SHALL resolve exactly units `relay` and `client` with no schema error.
2. Unit `relay` SHALL declare `image.project` `src/Pernix.OutboxRelay.Host/Pernix.OutboxRelay.Host.csproj`, `image.dockerfile` `Dockerfile`, `image.context` `.`, `image.name` `pernix-outbox-relay` (Q2), and `packages` `src/Pernix.OutboxRelay.Core/Pernix.OutboxRelay.Core.csproj`, `src/Pernix.OutboxRelay.Hosting/Pernix.OutboxRelay.Hosting.csproj`, `src/Pernix.OutboxRelay.Outbox.PostgreSql/Pernix.OutboxRelay.Outbox.PostgreSql.csproj`, `src/Pernix.OutboxRelay.Destination.Kafka/Pernix.OutboxRelay.Destination.Kafka.csproj`.
3. Unit `relay` SHALL list in `extra-inputs` every `Dockerfile` `COPY` source outside its resolved inputs, at least `src/RelayPackage.props` (F8).
4. Unit `client` SHALL declare no `image` and `packages` `client/src/Pernix.OutboxRelay.Client/Pernix.OutboxRelay.Client.csproj`, `client/src/Pernix.OutboxRelay.Client.PostgreSql/Pernix.OutboxRelay.Client.PostgreSql.csproj`, `client/src/Pernix.OutboxRelay.Client.EntityFrameworkCore/Pernix.OutboxRelay.Client.EntityFrameworkCore.csproj`.
5. WHEN either unit's projects are packed THEN only the 4 (relay) or 3 (client) listed packages SHALL be produced. No test, benchmark or host project SHALL be packable.

### Requirement 3: CI workflow

**User Story:** As a contributor, I want org-standard gates on every PR and main push, so that broken or vulnerable code does not merge.

#### Acceptance Criteria

1. `.github/workflows/ci.yml` SHALL match `templates/ci.yml` of pernix-ci `v1.0.0`: triggers `push` to `main` and `pull_request`, concurrency group, job `ci` with `contents: read`, `packages: write`, `uses: Pernix-Labs/pernix-ci/.github/workflows/ci.yml@v1.0.0`.
2. WHEN a PR is opened or updated THEN `ci` SHALL run secret scan, and per unit restore, vulnerability gate, deprecation report, build, test, and for `relay` image build + scan, for both units `nuget-pack` dry check, and SHALL push nothing to GHCR.
3. WHEN a commit is pushed to `main` THEN `ci` SHALL push `ghcr.io/pernix-labs/pernix-outbox-relay:sha-<L>` or reuse it when it exists.
4. WHEN the spec PR's CI completes THEN every job SHALL succeed.

### Requirement 4: CD workflow and Trusted Publishing

**User Story:** As a maintainer, I want a tag push to release one unit, so that releasing is one command with no secrets.

#### Acceptance Criteria

1. `.github/workflows/cd.yml` SHALL match `templates/cd.yml` of pernix-ci `v1.0.0`: trigger `push` tags `v*` and `*/v*`, jobs `ci` → `release` → `publish`, all pernix-ci refs `@v1.0.0`.
2. Job `publish` SHALL live in `.github/workflows/cd.yml`, declare `environment: release`, `permissions` `contents: read` and `id-token: write`, and run `nuget-pack` then `nuget-push` with `user: ${{ vars.NUGET_USER }}`.
3. No job in any workflow SHALL reference a NuGet API key secret or pass `api-key`.
4. Only job `publish` SHALL hold `id-token: write`.
5. WHEN tag `relay/vX.Y.Z[-pre]` is pushed THEN cd SHALL promote `sha-<L>` to `X.Y.Z[-pre]` and push the 4 relay packages at `X.Y.Z[-pre]` to nuget.org.
6. WHEN tag `client/vX.Y.Z[-pre]` is pushed THEN cd SHALL skip image promotion and push the 3 client packages at `X.Y.Z[-pre]`.
7. IF tag commit is not on `main` or tag does not parse THEN cd SHALL fail before any push (pernix-ci `release-tag`).

### Requirement 5: Image retention

**User Story:** As a maintainer, I want low GHCR retention, so that sha images do not accumulate.

#### Acceptance Criteria

1. `.github/workflows/retention.yml` SHALL match `templates/retention.yml` of pernix-ci `v1.0.0`: daily `schedule`, `workflow_dispatch` input `dry-run`, `uses: Pernix-Labs/pernix-ci/.github/workflows/image-retention.yml@v1.0.0`, pernix-ci default ages (14 days sha/prerelease, keep 5 stable).

### Requirement 6: Pinning

**User Story:** As a maintainer, I want pernix-ci pinned immutably, so that code running with `id-token: write` changes only when I change the pin.

#### Acceptance Criteria

1. Every `Pernix-Labs/pernix-ci` reference in `.github/` SHALL use ref `v1.0.0` (F11). None SHALL use `v1`, a branch, or a different ref.
2. Every third-party action reference in caller workflows SHALL be pinned by full commit sha with version comment, as in templates.

### Requirement 7: Build settings without platform-cicd assumptions

**User Story:** As a maintainer, I want strict warnings and NuGet audit with no CI-specific exceptions, so that local and CI builds behave the same.

#### Acceptance Criteria

1. `Directory.Build.props` SHALL set `TreatWarningsAsErrors` `true`, `NuGetAuditMode` `all`, `NuGetAuditLevel` `high`, and no `WarningsNotAsErrors` or `NoWarn`.
2. No file outside `docs/specs/` and superseded ADR text SHALL contain `platform-cicd`.
3. No client csproj SHALL set `TreatAsLocalProperty`. `client/Directory.Build.props` SHALL hold no comment about a CI global `TargetFramework` (F1).
4. WHEN `dotnet build OutboxRelay.slnx -c Release` runs locally THEN client packages SHALL still target `net8.0` and build with zero warnings.
5. IF restore finds a high or critical advisory in any package (direct or transitive) THEN restore SHALL fail.

### Requirement 8: Repo settings

**User Story:** As a maintainer, I want the environment and variable the publish job needs, so that the first release tag works.

#### Acceptance Criteria

1. GitHub environment `release` SHALL exist with deployment policy limited to tags matching `relay/v*` and `client/v*`.
2. Repo variable `NUGET_USER` SHALL equal `avataraps` (policy owner, F9).
3. IF the token cannot create either via `gh api` THEN implementer SHALL escalate to coordinator and SHALL NOT skip.

### Requirement 9: Documentation and ADR

**User Story:** As a maintainer, I want docs that state how CI/CD works and how to release, so that nobody reads stale version-file guidance.

#### Acceptance Criteria

1. `docs/development.md` SHALL gain a CI/CD section: units, what `ci.yml`/`cd.yml`/`retention.yml` do, pin `v1.0.0`, and release steps: `git tag relay/vX.Y.Z` or `client/vX.Y.Z` on a `main` commit, then `git push origin <tag>`. It SHALL state `BUILD_VERSION` in CI images is `0.0.0-ci.g<sha7>` and release version exists only as image tag (F4).
2. `README.md` SHALL name image `ghcr.io/pernix-labs/pernix-outbox-relay:<version>` and state packages are on nuget.org.
3. `docs/embedding.md`, `docs/client-packages.md`, `docs/architecture/outbox-relay-host.md` SHALL replace version-file statements with tag-based versioning. `docs/client-packages.md` SHALL drop `-preview.<run>` guidance in favour of `client/vX.Y.Z-<pre>` tags.
4. `docs/runbook.md` SHALL map `SERVICE_VERSION` / `service.version` `0.0.0-ci.g<sha7>` to release via image tag (F4).
5. New `docs/adr/0013-*.md` SHALL record: pernix-ci adoption, tag-only versions, two units, GHCR + retention, nuget.org Trusted Publishing, and SHALL state it supersedes version-file statements in ADR 0008 and 0011. ADR 0008 and 0011 SHALL link 0013 at the superseded statements. `docs/adr/README.md` index SHALL list 0013.
6. WHEN client doc tests (`client/tests/Pernix.OutboxRelay.Client.Docs.Unit.Tests`) run after doc edits THEN they SHALL pass.

### Requirement 10: Verification

**User Story:** As a reviewer, I want evidence the pipeline works without releasing, so that approval is grounded.

#### Acceptance Criteria

1. WHEN actionlint runs on `.github/workflows/*.yml` THEN it SHALL report no error.
2. WHEN spec PR is open THEN `ci` check on it SHALL be green (Req 3.4) and PR evidence SHALL link the run.
3. Since pernix-ci `v1.0.0` has no CD dry-run (F10), PR evidence SHALL state cd/retention validated by actionlint and template diff only.
4. No release tag (`relay/v*`, `client/v*`, `v*`) SHALL be pushed by this work.

## Non-Functional Requirements

### Code Architecture and Modularity
- Workflows are template copies. Deviations from pernix-ci templates only where a requirement names them.
- One spec, one PR.

### Performance
- No requirement. CI duration set by pernix-ci.

### Security
- No long-lived NuGet credential anywhere. OIDC only, `id-token: write` on `publish` only.
- Least privilege per template: `contents: read`, `packages: write` only where pernix-ci requires.
- Release environment restricted to unit version tags.

### Reliability
- Re-running cd on same tag succeeds (pernix-ci `image-promote` idempotent).
- `sha-<L>` deleted by retention is rebuilt by cd `ci` job.

### Usability
- Release = one tag push, documented in `docs/development.md`.

## Open questions

1. [ ] Q1. Released relay image reports `SERVICE_VERSION` / OTel `service.version` `0.0.0-ci.g<sha7>`, not release version (F4, pernix-ci design). Accept and document (Req 9.4), or require a pernix-ci change first (out of this spec)?
2. [ ] Q2. GHCR image name: `pernix-outbox-relay` (repo name, pernix-ci template example) assumed. Default without `image.name` would be `relay`. Confirm.
3. [ ] Q3. Benchmarks drop out of CI build coverage (F6). Accept, or add an out-of-template build step (deviates from pure template)?
4. [ ] Q4. Deleted files hold current lines: client `2.0.0` (`client/PackageVersion.props`, matches `docs/client-packages.md` release notes), relay `1.0` (`.version`). Should ADR 0013 / `docs/development.md` name `client/v2.0.0` and `relay/v1.0.0` as first tags, or leave first versions to release time?
