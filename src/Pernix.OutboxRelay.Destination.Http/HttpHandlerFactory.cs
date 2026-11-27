using System.Collections;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Pernix.OutboxRelay.Destination.Http;

internal static class HttpHandlerFactory
{
    internal static readonly TimeSpan PooledConnectionLifetime = TimeSpan.FromMinutes(5);
    internal static readonly TimeSpan PooledConnectionIdleTimeout = TimeSpan.FromSeconds(90);
    internal static readonly TimeSpan ResponseDrainTimeout = TimeSpan.FromSeconds(2);
    internal const int MaxResponseDrainSize = 64 * 1024;

    // Row header values are UTF-8 by contract (MessageHeaders); the default ASCII-only encoding would fault such rows forever.
    internal static readonly HeaderEncodingSelector<HttpRequestMessage> Utf8RequestHeaders = static (_, _) => Encoding.UTF8;

    private static readonly Oid ServerAuthentication = new("1.3.6.1.5.5.7.3.1");
    // Exact upper- and lower-case names only, as read by .NET's environment proxy.
    private static readonly string[] ProxyVariables =
        ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"];

    public static SocketsHttpHandler Create(HandlerSettings settings, IDictionary? environment = null)
    {
        ArgumentNullException.ThrowIfNull(settings);

        var handler = new SocketsHttpHandler
        {
            // The default system proxy is consulted per request (~539 B/req measured in task 1), so only env-configured proxies are used.
            UseProxy = ProxyConfigured(environment ?? Environment.GetEnvironmentVariables()),
            AllowAutoRedirect = false,
            UseCookies = false,
            AutomaticDecompression = DecompressionMethods.None,
            ActivityHeadersPropagator = null,
            PreAuthenticate = false,
            Credentials = null,
            MaxConnectionsPerServer = settings.MaxRequestsInFlight,
            PooledConnectionLifetime = PooledConnectionLifetime,
            PooledConnectionIdleTimeout = PooledConnectionIdleTimeout,
            ConnectTimeout = settings.ConnectTimeout,
            MaxResponseDrainSize = MaxResponseDrainSize,
            ResponseDrainTimeout = ResponseDrainTimeout,
            EnableMultipleHttp2Connections = true,
            RequestHeaderEncodingSelector = Utf8RequestHeaders,
        };

        if (settings.TrustedRoots is { } roots)
        {
            handler.SslOptions.RemoteCertificateValidationCallback = (_, certificate, chain, errors) =>
                ValidateAgainstRoots(roots, certificate, chain, errors);
        }

        if (settings.ClientCertificate is { } client)
        {
            handler.SslOptions.ClientCertificates = [client];
        }

        return handler;
    }

    internal static bool ProxyConfigured(IDictionary environment)
    {
        ArgumentNullException.ThrowIfNull(environment);

        foreach (DictionaryEntry variable in environment)
        {
            if (variable.Key is string name
                && variable.Value is string { Length: > 0 }
                && ProxyVariables.Contains(name, StringComparer.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    internal static bool ValidateAgainstRoots(
        X509Certificate2Collection roots, X509Certificate? certificate, X509Chain? presented, SslPolicyErrors errors)
    {
        if (certificate is not X509Certificate2 typed || (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != SslPolicyErrors.None)
        {
            return false;
        }

        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.AddRange(roots);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.ApplicationPolicy.Add(ServerAuthentication);
        if (presented is not null)
        {
            foreach (var element in presented.ChainElements)
            {
                chain.ChainPolicy.ExtraStore.Add(element.Certificate);
            }
        }

        return chain.Build(typed);
    }
}
