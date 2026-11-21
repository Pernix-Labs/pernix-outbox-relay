using System.Reflection;
using System.Text.Json;

namespace Pernix.OutboxRelay.Host.Configuration;

public sealed record RelayIdentity(
    string ServiceName,
    string ComponentName,
    string Version,
    string Commit,
    string EnvironmentName)
{
    private const string DefaultServiceName = "outbox-relay";
    private const string Unknown = "unknown";

    public static RelayIdentity From(IConfiguration config)
    {
        var serviceName = NonEmpty(config["SERVICE_NAME"]) ?? DefaultServiceName;

        return new RelayIdentity(
            ServiceName: serviceName,
            ComponentName: JsonNamingPolicy.KebabCaseLower.ConvertName(serviceName),
            Version: NonEmpty(config["SERVICE_VERSION"]) ?? EntryAssemblyVersion(),
            Commit: NonEmpty(config["SERVICE_COMMIT"]) ?? Unknown,
            EnvironmentName: NonEmpty(config["EnvironmentName"]) ?? Unknown);
    }

    private static string? NonEmpty(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

    private static string EntryAssemblyVersion() =>
        NonEmpty(Assembly.GetEntryAssembly()?
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion) ?? Unknown;
}
