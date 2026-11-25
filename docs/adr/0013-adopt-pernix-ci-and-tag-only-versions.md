# 0013. Adopt pernix-ci for CI/CD and take every release version from the tag

**Status:** Accepted
**Date:** 2026-11-23
**Deciders:** Apostolos Stephanou
**Related specs:** [2026-11-23-cicd-adoption](../specs/2026-11-23-cicd-adoption/)
**Supersedes:** in [0008](./0008-producer-client-packages.md), the clause that client packages are "versioned in lockstep by one `VersionPrefix`" and the Follow-up 2026-11-08 publish workflows to GitHub Packages. In [0011](./0011-relay-config-section-and-hosting-package.md), D7 bullets 3 and 4 (`VersionPrefix` from `.version`, `client/PackageVersion.props`, publish job as follow-up). The rest of 0008 and 0011 stands.
**Superseded in part by:** [0014](./0014-clean-slate-schema-and-client-1-0-0.md) (D6: first client tag is `client/v1.0.0`).

## Context

The repo had no pipeline in `.github/`. Two files held versions: `.version` (`1.0`), read by `src/RelayPackage.props` `VersionPrefix` and copied by the `Dockerfile`, and `client/PackageVersion.props` (`VersionPrefix` `2.0.0`), imported by the three client csprojs. Nothing tied either file to the image `BUILD_VERSION` (default `1.0.0`) or to a published package.

The build carried workarounds for platform-cicd assumptions: a `WarningsNotAsErrors` exception for `NU1507` and `TreatAsLocalProperty="TargetFramework"` in the client csprojs against a global `-p:TargetFramework=net10.0`. Shared `Pernix-Labs/pernix-ci` replaces platform-cicd. It passes no global `TargetFramework`, never writes `nuget.config`, rejects build args that override `TargetFramework`, warning or `NuGetAudit` settings, and passes the tag version to pack.

Human rulings 2026-11-22: version comes from the release tag only, Trusted Publishing only (no API key), do not push a release tag in this work, no compatibility shims for the old version files.

## Decision

CI/CD runs on pernix-ci `v1.0.1` templates, the repo holds no version, and each release unit takes its version from a `<unit>/vX.Y.Z[-pre]` tag.

- **Pin (D1).** Every pernix-ci line in `.github/workflows/ci.yml`, `cd.yml` and `retention.yml` pins `@v1.0.1`, tag commit `1bf0faeb0e4c529e190692e15f68ee9c4e42f8a7`. v1.0.1 replaces the first pernix-ci release, whose reusable `ci.yml` default working-directory `.` broke `dotnet-restore`. The workflows are template copies, only header comments and the pernix-ci ref differ (D2), because the v1.0.1 templates still reference the first release tag. A bump changes every line to the same ref.
- **Units.** `.github/release-units.yml` declares `relay` (image `src/Pernix.OutboxRelay.Host`, packages Core, Hosting, `Outbox.PostgreSql`, `Destination.Kafka`, extra input `src/RelayPackage.props`) and `client` (packages `Pernix.OutboxRelay.Client`, `.Client.PostgreSql`, `.Client.EntityFrameworkCore`).
- **Tag-only versions.** Tag grammar `<unit>/vX.Y.Z[-pre]`, enforced by pernix-ci `release-tag` on a `main` commit. `nuget-pack` passes `-p:Version` and `-p:PackageVersion` from the tag. `.version` and `client/PackageVersion.props` are deleted, and `src/RelayPackage.props` sets no `VersionPrefix`.
- **Image (D3).** `ghcr.io/pernix-labs/pernix-outbox-relay`. CI on `main` pushes `sha-<L>`. A `relay/v*` tag promotes that image to `X.Y.Z[-pre]` without rebuild. `retention.yml` runs daily with template defaults: sha and prerelease tags kept 14 days, the 5 newest stable tags kept.
- **Packages.** nuget.org Trusted Publishing from `cd.yml` job `publish`, environment `release`, OIDC `id-token: write` on that job only, owner from repo variable `NUGET_USER`. Environment `release` accepts deployments from `relay/v*` and `client/v*` tags only.
- **Build gates.** `TreatWarningsAsErrors` with no exception, `NuGetAuditMode=all`, `NuGetAuditLevel=high` in `Directory.Build.props`. The `NU1507` exception and the `TreatAsLocalProperty` workaround are removed (D10).
- **`SERVICE_VERSION` (D4).** A released image reports the CI version `0.0.0-ci.g<sha7>`, because promote never rebuilds. A deployment that needs the release version sets env `SERVICE_VERSION` to the promoted image tag. Env vars layer above appsettings, so the deployment value wins. `SERVICE_COMMIT` stays correct.
- **Benchmarks (D5).** Benchmark projects are in no unit and stay out of CI.
- **First tags (D6).** `relay/v1.0.0` and `client/v2.0.0`, documented and not pushed by this work.

## Alternatives considered

- **Pin pernix-ci by full sha.** Allowed by pernix-ci, but the templates pin by tag, so a tag pin keeps caller files template-identical apart from the ref. A sha pin adds a deviation per line.
- **Pin the moving `v1`.** pernix-ci forbids it for Trusted Publishing callers, because the workflow ref is part of the trust claim.
- **Keep the version files and check them against the tag.** Two sources for one value, and a mismatch fails only at release time.
- **NuGet API key secret.** Rejected by ruling. A long-lived credential where OIDC works.
- **One unit for relay and client.** The relay and the client packages have separate version lines ([0011](./0011-relay-config-section-and-hosting-package.md) D7) and release cadences.
- **Rebuild the image at release so `SERVICE_VERSION` carries the release version.** pernix-ci promotes the tested digest and never rebuilds. Changing that is out of this repo's scope.

## Consequences

- Release is one tag push. `relay/vX.Y.Z` promotes the image and pushes 4 relay packages. `client/vX.Y.Z` pushes 3 client packages. Re-running cd on the same tag is idempotent.
- No file in the repo states a version. Local builds and packs without `-p:Version` get the SDK default `1.0.0`. Only CI packs.
- Image `BUILD_VERSION` and the OCI `version` label are the CI version, never the release version. Release version exists only as the image tag and the package version.
- A high-severity advisory in any package, direct or transitive, fails restore until pinned in `Directory.Packages.props`.
- Test projects that reference both relay and client projects join both units (D8). The `client` leg runs relay tests, and client edits rebuild the relay image. Extra CI work, no correctness impact.
- Benchmark projects have no CI build coverage. A benchmark that stops compiling is found locally.
- `cd.yml` file name and environment `release` are part of the nuget.org Trusted Publishing policy. Renaming either breaks publish.
- cd and retention are validated by actionlint and template diff only, because pernix-ci `v1.0.1` has no CD dry-run (D9).
