using System.Diagnostics.CodeAnalysis;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Microsoft.Extensions.Configuration;

namespace Pernix.OutboxRelay.Destination.Http;

internal sealed class HttpCredentialSection : IRelaySectionValidator
{
    internal const string Key = "HttpCredentials";
    private readonly Dictionary<string, HttpCredential> credentials = new(StringComparer.OrdinalIgnoreCase);
    private readonly IReadOnlyList<string> failures;

    public HttpCredentialSection(RelayConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        // Elements are bound one by one: binding a whole dictionary silently drops elements that fail to convert.
        var bindFailures = new List<string>();
        foreach (var credential in configuration.Section.GetSection(Key).GetChildren())
        {
            credentials[credential.Key] = RelayOptionsBinder.BindSection<HttpCredential>(credential, $"{Key}[{credential.Key}]", bindFailures);
        }

        failures = bindFailures.Distinct(StringComparer.Ordinal).ToList();
    }

    public IReadOnlyList<string> Validate(IConfiguration section) => failures;

    internal bool TryGet(string name, [NotNullWhen(true)] out HttpCredential? credential) => credentials.TryGetValue(name, out credential);
}
