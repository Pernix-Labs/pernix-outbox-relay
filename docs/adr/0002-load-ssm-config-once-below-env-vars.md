# 0002. Load SSM configuration once, below environment variables

**Status:** Accepted
**Date:** 2026-11-02
**Deciders:** Apostolos Stephanou

## Context

The relay is configured only: appsettings, AWS SSM Parameter Store / Secrets Manager, env vars from Helm, and the command line. `Pernix.Common.Configurations.AWS` `AddSystemsManager` builds the SSM sources and resolves secret references, but it has two defaults that conflict with that model:

- It appends SSM at the end of the configuration sources, so SSM beats env vars. A Helm env var could not override a shared SSM value.
- It sets `ReloadAfter` from `AWS_SSM_RELOAD_INTERVAL_SECONDS` (default 300). The Amazon provider then reloads on a timer through a `ChangeToken` that is never disposed. Raising the interval still reloads.

`ConfigurationManager` loads each source as it is added, so the order and reload setting must be fixed before the sources reach it.

## Decision

SSM configuration is loaded once at startup and sits between appsettings and env vars.

`SsmConfigurationLayer` calls `AddSystemsManager` on `StagingConfigurationManager`, a non-building `IConfigurationManager`. It keeps the staged `SystemsManagerConfigurationSource` entries, sets each `ReloadAfter = null`, appends them to the host configuration, then re-appends env vars and command line. Effective order, last wins: appsettings < SSM < env vars < command line. `AWS_SSM_ENABLED=false` stages nothing and leaves the default chain unchanged. The user-secrets source the package adds under `Development` is dropped, because the host builder already adds it. Any other source type from the package fails startup.

## Alternatives considered

- **Use `AddSystemsManager` directly on `builder.Configuration`** — SSM beats env vars, and SSM reloads on a timer.
- **Write an in-house SSM provider** — duplicates path building and secret resolution the package already does.
- **Keep reload with a long interval** — still reloads, and `CancellationTokenSource` caps the interval at about 24.8 days.

## Consequences

- Env vars and command line always override SSM, so Helm values are the final word per deployment.
- A configuration change, including a rotated secret, needs a pod restart.
- `AWS_SSM_RELOAD_INTERVAL_SECONDS` is ignored.
- The layer depends on the package's internal source types. A package bump that adds a source type fails startup and the `Stage` unit tests, so it is caught before release.
- 2026-11-07: The Host reads `AWS_SSM_ENABLED` itself with `GetValue<bool>` and default `true` before calling `AddSystemsManager`, so SSM-off loads no `AWSSDK.*` or `Amazon.*` assembly (`Pernix.Common.Configurations.AWS` still loads for `^` marker expansion). The human chose the gate in the Host on 2026-11-07 (PR #15) and rejected a separate AWS project, a no-op under image-only delivery, and a second image, which saves about 1 MB gzip at the cost of a second pipeline. A package bump that changes the key, default or accepted values must update the Host gate. `Apply_with_invalid_ssm_value_throws_like_the_package` guards invalid values. Moving the gate into `Pernix.Common.Configurations.AWS` itself (removes the duplicated `AWS_SSM_ENABLED` parse) is a follow-up for the package owners.
