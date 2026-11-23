---
status: approved
createdAt: 2026-11-23T08:08:09Z
updatedAt: 2026-11-23T08:10:16Z
approvedAt: 2026-11-23T08:10:16Z
completedAt: null
---

# Tasks: Adopt pernix-ci CI/CD

**Spec ID:** 2026-11-23-cicd-adoption
**Requirements:** [requirements.md](./requirements.md)
**Design:** [design.md](./design.md)

<!--
DAG grammar — parsed by scripts/dag/*.sh in the orca-spec-workflow skill. One task:

  - [ ] <coord> <title>          coord = 1, 2.1, 4.1.2 … unique, dotted numeric
  - Deps / Files / Agent / Verify / Isolation — see skill template.

Checkboxes = plan notation, never ticked. Doc FROZEN once approved; runtime state lives in
orca (task-list) and docs/specs/<id>/journal.md.

Shape: roots 1 (.github files), 2 (version + build settings), 3 (repo settings), 4 (ADRs) run in parallel.
5 (docs) after 4 (links ADR 0013). 6 (local gate) after 1, 2, 5. PR, push and CI-green-on-PR are coordinator closeout, not tasks.
Files tokens: exact repo-relative paths, directory tokens end with `/`. Same token used consistently across tasks.
Common rules for every task: base = this branch (from origin/main 26f7207); pernix-ci = Pernix-Labs/pernix-ci tag v1.0.0
(commit 50c1228ba4f77d3219a270fbf3dcfee7ccf84781), clone with `gh repo clone Pernix-Labs/pernix-ci <scratch> -- --branch v1.0.0` when a template is needed;
no C# change; no "legacy"/migration guard code (NG6); never push any tag (Req 10.4); do not edit `docs/specs/`;
commit only own task's files (`git commit -- <paths>`).
-->

- [ ] 1 Add `.github/` release units and pernix-ci workflows
  - Files: .github/release-units.yml, .github/workflows/ci.yml, .github/workflows/cd.yml, .github/workflows/retention.yml
  - Verify: `actionlint .github/workflows/*.yml` clean; for each workflow `diff <(sed '/^name:/,$!d' <pernix-ci>/templates/<f>) <(sed '/^name:/,$!d' .github/workflows/<f>)` empty (body identical to template from `name:` on); `git grep -n 'pernix-ci' -- .github | grep -v '@v1.0.0'` shows only comment lines
  - Purpose: Req 2.1-2.4, 3.1, 4.1-4.4, 5.1, 6.1-6.2. Design C1-C4, D1, D2, D3.
  - Steps:
    - `.github/release-units.yml`: exactly design C1 block (units `relay` with `image.name: pernix-outbox-relay`, `extra-inputs: [src/RelayPackage.props]`, 4 relay packages; unit `client` with 3 client packages, no image).
    - `.github/workflows/ci.yml`, `retention.yml`: copy pernix-ci `templates/ci.yml`, `templates/retention.yml` at `v1.0.0`. Replace header comment with 2 lines: purpose + `Copied from Pernix-Labs/pernix-ci templates/<f> @ v1.0.0. Keep the pin equal on every pernix-ci line.` Body unchanged.
    - `.github/workflows/cd.yml`: copy `templates/cd.yml` at `v1.0.0`. Keep header comment, fill placeholders: Repository owner `avataraps`, Repository `pernix-outbox-relay`. Body unchanged: `publish` keeps `environment: release`, `permissions: contents: read, id-token: write`, `nuget-push` `user: ${{ vars.NUGET_USER }}`. No API key, no `api-key` input anywhere.
    - Every pernix-ci `uses:` ref = `@v1.0.0`. Third-party actions stay sha-pinned as in templates.
  - Acceptance: 4 files present, lint clean, bodies template-identical, file name `cd.yml` kept.

- [ ] 2 Remove version files and platform-cicd build workarounds
  - Files: .version, client/PackageVersion.props, src/RelayPackage.props, Directory.Build.props, client/Directory.Build.props, client/src/Pernix.OutboxRelay.Client/Pernix.OutboxRelay.Client.csproj, client/src/Pernix.OutboxRelay.Client.PostgreSql/Pernix.OutboxRelay.Client.PostgreSql.csproj, client/src/Pernix.OutboxRelay.Client.EntityFrameworkCore/Pernix.OutboxRelay.Client.EntityFrameworkCore.csproj, Dockerfile
  - Verify: re-run every Acceptance command yourself and confirm output
  - Purpose: Req 1.1-1.4, 1.6, 7.1, 7.3-7.5. Design C5, C6, D10.
  - Steps:
    - `git rm .version client/PackageVersion.props`.
    - `src/RelayPackage.props`: delete `VersionPrefix` line; comment -> `One package metadata set for all relay library packages. Version comes from the release tag (ADR 0013).`
    - `Directory.Build.props`: delete platform-cicd `NU1507` comment and `WarningsNotAsErrors` line; add `<NuGetAuditMode>all</NuGetAuditMode>` and `<NuGetAuditLevel>high</NuGetAuditLevel>` after `TreatWarningsAsErrors`. No `NoWarn`/`WarningsNotAsErrors` anywhere.
    - 3 client csprojs: delete line-1 `TreatAsLocalProperty` comment, `TreatAsLocalProperty="TargetFramework"` attribute (`<Project Sdk="Microsoft.NET.Sdk">`), and `<Import Project="..\..\PackageVersion.props" />`.
    - `client/Directory.Build.props`: delete the `TreatAsLocalProperty` / CI global `TargetFramework` comment block. Keep `CheckEolTargetFramework=false` and its comment.
    - `Dockerfile`: first `COPY` -> `COPY ["global.json", "Directory.Build.props", "Directory.Packages.props", "NuGet.Config", "./"]`. Rest unchanged.
  - Acceptance:
    - `dotnet build OutboxRelay.slnx -c Release` 0 warnings 0 errors; `client/src/*/bin/Release/net8.0/` exist (client stays `net8.0`).
    - `dotnet pack client/src/Pernix.OutboxRelay.Client/Pernix.OutboxRelay.Client.csproj -c Release -o <scratch>/c -p:Version=9.9.9-test -p:PackageVersion=9.9.9-test` and same for `src/Pernix.OutboxRelay.Core/Pernix.OutboxRelay.Core.csproj` -> nupkg named `*.9.9.9-test.nupkg`.
    - `docker build -t outbox-relay:cicd-check .` succeeds.
    - `git grep -nE '\.version\b|PackageVersion\.props|VersionPrefix|platform-cicd|TreatAsLocalProperty|NU1507' -- ':!docs'` returns nothing.

- [ ] 3 Create GitHub environment `release` and variable `NUGET_USER`
  - Verify: `gh api repos/Pernix-Labs/pernix-outbox-relay/environments/release/deployment-branch-policies --jq '.branch_policies[]|[.name,.type]'` lists `relay/v*` tag and `client/v*` tag only; `gh api repos/Pernix-Labs/pernix-outbox-relay/actions/variables/NUGET_USER --jq .value` prints `avataraps`
  - Purpose: Req 8.1-8.3. Design C7, D7. Repo settings only, no repo files.
  - Steps:
    - Run design C7 commands exactly: `PUT environments/release` with `{"deployment_branch_policy":{"protected_branches":false,"custom_branch_policies":true}}`; `POST .../deployment-branch-policies` `name='relay/v*' type=tag` and `name='client/v*' type=tag`; `POST actions/variables` `name=NUGET_USER value=avataraps`. Re-running: a policy or variable that already exists with the right value is fine (use `PATCH actions/variables/NUGET_USER` if it exists with another value).
    - Any 403/404/permission error: do NOT skip. Send escalation to coordinator with the exact C7 commands and the error text, then stop.
  - Acceptance: both verify commands print expected values; verify output in worker_done body (coordinator puts it in PR body).

- [ ] 4 ADR 0013 and supersede annotations in ADR 0008 / 0011
  - Files: docs/adr/0013-adopt-pernix-ci-and-tag-only-versions.md, docs/adr/0008-producer-client-packages.md, docs/adr/0011-relay-config-section-and-hosting-package.md, docs/adr/README.md
  - Verify: ADR 0013 follows `docs/adr/_template.md` sections; 0008/0011 diffs are annotations only (no decision text removed); every relative link in changed files resolves
  - Purpose: Req 9.5. Design C9, C8 ADR rows, D11.
  - Steps:
    - New `docs/adr/0013-adopt-pernix-ci-and-tag-only-versions.md` per design C9: Status Accepted, Date 2026-11-23, Deciders as ADR 0012, Related specs `../specs/2026-11-23-cicd-adoption/`, Supersedes line (0008 `VersionPrefix` lockstep clause + Follow-up 2026-11-08 GitHub Packages workflows; 0011 D7 bullets 3-4). Context / Decision / Alternatives / Consequences content from design C9 and D1-D9 (pin `v1.0.0` = `50c1228`, units, tag grammar `<unit>/vX.Y.Z[-pre]`, GHCR `ghcr.io/pernix-labs/pernix-outbox-relay` + retention, nuget.org Trusted Publishing from `cd.yml` env `release`, `TreatWarningsAsErrors` no exceptions + `NuGetAuditMode=all`/`NuGetAuditLevel=high`, `SERVICE_VERSION` = CI version unless deployment sets it, benchmarks out of CI, first tags `relay/v1.0.0` / `client/v2.0.0` documented not pushed).
    - `docs/adr/0008-producer-client-packages.md`: after the "versioned in lockstep by one `VersionPrefix`" sentence and after the Follow-up 2026-11-08 bullets, add `Superseded by [0013](./0013-adopt-pernix-ci-and-tag-only-versions.md): ...` one-liners (version from `client/v*` tag; publish via `cd.yml` to nuget.org).
    - `docs/adr/0011-relay-config-section-and-hosting-package.md`: status line add `and in part (D7 versioning) by [0013](...)`; annotate D7 bullets 3-4 with superseded one-liners.
    - `docs/adr/README.md`: add 0013 entry in existing style; extend 0008 and 0011 entries with "version statements superseded by 0013".
  - Acceptance: files written, links resolve, `git grep -n '\.version\|PackageVersion\.props\|VersionPrefix' -- docs/adr` hits only lines that carry a supersede annotation or ADR 0013 itself.

- [ ] 5 Docs: CI/CD section, release how-to, version references
  - Deps: 4
  - Files: docs/development.md, docs/runbook.md, README.md, docs/embedding.md, docs/client-packages.md, docs/architecture/outbox-relay-host.md
  - Verify: `dotnet test --project client/tests/Pernix.OutboxRelay.Client.Docs.Unit.Tests -c Release` green; `git grep -nE '\.version\b|PackageVersion\.props|VersionPrefix|platform-cicd|-preview\.<run>|GitHub Packages' -- README.md docs ':!docs/adr' ':!docs/specs'` returns nothing; every relative link in changed files resolves
  - Purpose: Req 9.1-9.4, 9.6. Design C8, D4, D6.
  - Steps:
    - First grep `client/tests/Pernix.OutboxRelay.Client.Docs.Unit.Tests` (`ReadmeLinkTests`, `GuideErrorTextTests`, `GuideSnippetTests`, `RepoFiles`) for strings/anchors asserted in README / `docs/client-packages.md`; keep them valid.
    - `docs/development.md`: Local docker build note adds "CI builds set `BUILD_VERSION` to `0.0.0-ci.g<sha7>`". New `## CI/CD` section: units table (`relay` = image `ghcr.io/pernix-labs/pernix-outbox-relay` + Core, Hosting, Outbox.PostgreSql, Destination.Kafka; `client` = Client, Client.PostgreSql, Client.EntityFrameworkCore), one line each for `.github/workflows/ci.yml` (PR + main: secret scan, gates, build, test, image build+scan, pack dry check; PR never pushes), `cd.yml` (tag: ci -> promote `sha-<L>` -> publish nuget.org via Trusted Publishing, env `release`), `retention.yml` (daily, 14 d sha/prerelease, keep 5 stable, manual `dry-run`); pernix-ci pin `v1.0.0`, bump every pernix-ci line together. `### Release`: tag a commit on `main`, `git tag relay/vX.Y.Z && git push origin relay/vX.Y.Z` (or `client/vX.Y.Z`, `-pre` suffix allowed); version comes only from the tag; same-tag re-run idempotent, digest mismatch -> new patch; first tags `relay/v1.0.0` and `client/v2.0.0`. `SERVICE_VERSION` note: released image reports `0.0.0-ci.g<sha7>`; deployments set env `SERVICE_VERSION=<image tag>` (env wins over image `ENV`), `SERVICE_COMMIT` already correct. Prerequisites: env `release` (tags `relay/v*`, `client/v*`), variable `NUGET_USER`, nuget.org policy (owner `avataraps`, `cd.yml`, env `release`, glob `Pernix.OutboxRelay.*`).
    - `docs/runbook.md` deploy section: image `ghcr.io/pernix-labs/pernix-outbox-relay:<version>`; set `SERVICE_VERSION=<version>` in deployment env (else telemetry `service.version` = `0.0.0-ci.g<sha7>`); `SERVICE_COMMIT` from image.
    - `README.md`: Configure and deploy names image `ghcr.io/pernix-labs/pernix-outbox-relay:<version>` and nuget.org packages; client install section says nuget.org.
    - `docs/embedding.md:19`: replace `.version` + NG8 sentences with "Relay packages and image share one version per `relay/vX.Y.Z` tag."
    - `docs/client-packages.md` ~769: "independent of the relay version line (tags `client/v*` vs `relay/v*`)"; replace `-preview.<run>` guidance with `client/vX.Y.Z-<pre>` tags; any GitHub Packages feed instruction -> nuget.org.
    - `docs/architecture/outbox-relay-host.md`: `:66` drop `client/PackageVersion.props` `VersionPrefix`, version from `client/v*` tag; `:52` CI `BUILD_VERSION` = CI version, deployment overrides `SERVICE_VERSION`.
    - Link ADR 0013 as `docs/adr/0013-adopt-pernix-ci-and-tag-only-versions.md` (relative path per file).
  - Acceptance: verify commands pass; no doc names a version file.

- [ ] 6 Local full gate
  - Deps: 1, 2, 5
  - Verify: re-run every command in Steps yourself and confirm output matches
  - Purpose: Req 1.5, 7.2, 10.1, 10.3; design Testing Strategy (Integration). Read-only: report failures to coordinator, do not fix.
  - Steps:
    - `dotnet build OutboxRelay.slnx -c Release` 0 warnings.
    - `dotnet test --solution OutboxRelay.slnx -c Release` green (Docker running; opt-in tests skip).
    - `docker build -t outbox-relay:cicd-check .` succeeds.
    - `actionlint .github/workflows/*.yml` clean; template body diff (task 1 Verify) empty.
    - `git grep -nE '\.version\b|PackageVersion\.props|VersionPrefix|platform-cicd|TreatAsLocalProperty|NU1507' -- ':!docs/specs'` hits only annotated ADR 0008/0011 lines and ADR 0013.
    - `git tag -l 'relay/*' 'client/*'` empty locally and `git ls-remote --tags origin 'relay/*' 'client/*'` empty.
  - Acceptance: all pass; command outputs summarized in worker_done body (coordinator puts it in PR body). Any failure -> worker_done `--outcome failed` naming the failing command and owning task (1, 2, 4 or 5).
