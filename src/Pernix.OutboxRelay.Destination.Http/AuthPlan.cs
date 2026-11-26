using System.Text;
using Pernix.OutboxRelay.Destination.Http.Options;

namespace Pernix.OutboxRelay.Destination.Http;

internal sealed class AuthPlan
{
    internal const string AuthorizationHeader = "Authorization";

    private AuthPlan(AuthenticationScheme scheme, string? headerName, string? headerValue, OAuth2TokenCache? tokenCache)
    {
        Scheme = scheme;
        HeaderName = headerName;
        HeaderValue = headerValue;
        TokenCache = tokenCache;
    }

    public static AuthPlan None { get; } = new(AuthenticationScheme.None, null, null, null);

    public AuthenticationScheme Scheme { get; }

    public string? HeaderName { get; }
    public string? HeaderValue { get; }

    public OAuth2TokenCache? TokenCache { get; }

    public static AuthPlan Create(AuthenticationOptions? options, HttpCredential? credential, OAuth2TokenCache? tokenCache = null)
    {
        var scheme = options?.Scheme ?? AuthenticationScheme.None;
        switch (scheme)
        {
            case AuthenticationScheme.None:
                return None;

            case AuthenticationScheme.Basic:
                var username = Required(credential?.Username, nameof(HttpCredential.Username));
                var password = Required(credential?.Password, nameof(HttpCredential.Password));
                return new(scheme, AuthorizationHeader, "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(username + ":" + password)), null);

            case AuthenticationScheme.Bearer:
                return new(scheme, AuthorizationHeader, "Bearer " + Required(credential?.Token, nameof(HttpCredential.Token)), null);

            case AuthenticationScheme.ApiKey:
                var headerName = Required(options!.HeaderName, nameof(AuthenticationOptions.HeaderName));
                var key = Required(credential?.ApiKey, nameof(HttpCredential.ApiKey));
                return new(scheme, headerName, options.ValuePrefix + key, null);

            case AuthenticationScheme.OAuth2ClientCredentials:
                ArgumentNullException.ThrowIfNull(tokenCache);
                return new(scheme, AuthorizationHeader, null, tokenCache);

            default:
                throw new ArgumentOutOfRangeException(nameof(options), scheme, "unknown authentication scheme");
        }
    }

    private static string Required(string? value, string field) =>
        string.IsNullOrEmpty(value) ? throw new ArgumentException($"authentication needs {field}", field) : value;
}
