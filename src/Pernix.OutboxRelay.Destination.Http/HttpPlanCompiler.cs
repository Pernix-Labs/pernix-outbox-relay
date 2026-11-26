using System.Buffers;
using System.Net.Http.Headers;
using System.Text;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Http.Options;

namespace Pernix.OutboxRelay.Destination.Http;

internal sealed class HttpHookLookup(Func<string, bool> hasRequestMapper, Func<string, bool> hasResponseClassifier)
{
    public static HttpHookLookup None { get; } = new(static _ => false, static _ => false);

    public bool HasRequestMapper(string name) => hasRequestMapper(name);

    public bool HasResponseClassifier(string name) => hasResponseClassifier(name);
}

internal delegate OAuth2TokenCache OAuth2TokenCacheFactory(AuthenticationOptions options, HttpCredential credential, HandlerSettings settings);

internal static partial class HttpPlanCompiler
{
    internal const string DefaultContentType = "application/octet-stream";

    private const string ContentTypeHeader = "Content-Type";
    private const string HeaderPlaceholder = "{header:";

    // {outbox} needs a name to fold; a destination no outbox references is only probed.
    private const string UnreferencedOutbox = "outbox";

    private static readonly SearchValues<char> TokenChars =
        SearchValues.Create("!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

    private static readonly SearchValues<char> ControlChars = SearchValues.Create("\r\n\0");

    private static readonly bool[] DefaultProbeExpected = StatusTable.ParseClass(["2xx"], string.Empty, [])!;

    public static HttpDestinationPlan? Compile(
        HttpDestinationOptions options,
        string name,
        string path,
        IReadOnlyList<string> outboxes,
        HttpCredentialSection credentials,
        HttpHookLookup hooks,
        OAuth2TokenCacheFactory tokenCacheFactory,
        List<string> failures)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(outboxes);
        ArgumentNullException.ThrowIfNull(credentials);
        ArgumentNullException.ThrowIfNull(hooks);
        ArgumentNullException.ThrowIfNull(tokenCacheFactory);
        ArgumentNullException.ThrowIfNull(failures);

        var start = failures.Count;
        var compiler = new Compilation(options, name, path, outboxes.Count == 0 ? [UnreferencedOutbox] : outboxes, credentials, hooks, failures);
        var plan = compiler.Run(tokenCacheFactory);
        return failures.Count > start ? null : plan;
    }

    internal static bool IsToken(string? name) => !string.IsNullOrEmpty(name) && !name.AsSpan().ContainsAnyExcept(TokenChars);

    internal static bool IsContentHeader(string name) =>
        HeaderForwarding.ContentHeaders.Contains(name) || name.StartsWith("Content-", StringComparison.OrdinalIgnoreCase);

    [GeneratedRegex("^[a-z0-9][a-z0-9._-]{0,62}$", RegexOptions.CultureInvariant)]
    private static partial Regex RouteKey();

    private sealed class Compilation(
        HttpDestinationOptions o,
        string name,
        string path,
        IReadOnlyList<string> outboxes,
        HttpCredentialSection credentials,
        HttpHookLookup hooks,
        List<string> failures)
    {
        private readonly Dictionary<string, string> relayOwned = new(StringComparer.OrdinalIgnoreCase);
        private readonly List<(string Key, string Scheme)> schemes = [];
        private readonly HashSet<string> reported = new(StringComparer.Ordinal);
        private readonly int start = failures.Count;
        private string? host;

        private bool Failed => failures.Count > start;

        public HttpDestinationPlan? Run(OAuth2TokenCacheFactory tokenCacheFactory)
        {
            var (sendTimeout, connectTimeout, maxInFlight) = Limits();
            var version = o.HttpVersion;
            if (!HandlerSettings.TryParseVersion(version, out _, out _))
            {
                Fail("HttpVersion", $"must be one of \"{HandlerSettings.Http11}\", \"{HandlerSettings.Http2}\", \"{HandlerSettings.Http2Cleartext}\"");
                version = null;
            }

            var method = Method(o.Method, "Method") ?? HttpMethod.Post;
            var contentType = ContentType(o.ContentType, "ContentType") ?? DefaultContentType;
            var (authCredential, authHeader) = Authentication();
            var signing = Signing(authHeader);
            AddDeadLetterNames();
            var status = StatusTable.TryCreate(o.StatusCodes, Key("StatusCodes"), failures);
            var forwarding = Forwarding();
            var (roots, clientCertificate) = Tls();
            var probe = HealthProbe();
            Hooks();
            var endpoints = Endpoints(method, contentType, status ?? StatusTable.Default, signing);
            HttpVersionSchemes(version);
            if (Failed || endpoints is null)
            {
                clientCertificate?.Dispose();
                return null;
            }

            var handler = new HandlerSettings(connectTimeout, maxInFlight, version, roots, clientCertificate);
            var auth = o.Authentication?.Scheme == AuthenticationScheme.OAuth2ClientCredentials
                ? AuthPlan.Create(o.Authentication, authCredential, tokenCacheFactory(o.Authentication, authCredential!, handler))
                : AuthPlan.Create(o.Authentication, authCredential);
            return new HttpDestinationPlan
            {
                Name = name,
                Endpoints = endpoints,
                Forwarding = forwarding,
                Auth = auth,
                Signing = signing,
                Handler = handler,
                SendTimeout = sendTimeout,
                RequestMapper = Blank(o.RequestMapper) ? null : o.RequestMapper,
                ResponseClassifier = Blank(o.ResponseClassifier) ? null : o.ResponseClassifier,
                Probe = probe,
                Host = host ?? string.Empty,
            };
        }

        private (TimeSpan Send, TimeSpan Connect, int MaxInFlight) Limits()
        {
            var send = o.SendTimeout ?? HttpDestinationPlan.DefaultSendTimeout;
            var connect = o.ConnectTimeout ?? HttpDestinationPlan.DefaultConnectTimeout;
            var maxInFlight = o.MaxRequestsInFlight ?? HttpDestinationPlan.DefaultMaxRequestsInFlight;
            if (send <= TimeSpan.Zero)
            {
                Fail("SendTimeout", "must be greater than 00:00:00");
            }

            if (connect <= TimeSpan.Zero)
            {
                Fail("ConnectTimeout", "must be greater than 00:00:00");
            }
            else if (send > TimeSpan.Zero && connect > send)
            {
                Fail("ConnectTimeout", "must not exceed SendTimeout");
            }

            if (maxInFlight < 1)
            {
                Fail("MaxRequestsInFlight", "must be at least 1");
            }

            return (send, connect, maxInFlight);
        }

        private HttpMethod? Method(string? value, string key)
        {
            if (value is null)
            {
                return null;
            }

            if (value.Equals("POST", StringComparison.OrdinalIgnoreCase))
            {
                return HttpMethod.Post;
            }

            if (value.Equals("PUT", StringComparison.OrdinalIgnoreCase))
            {
                return HttpMethod.Put;
            }

            if (value.Equals("PATCH", StringComparison.OrdinalIgnoreCase))
            {
                return HttpMethod.Patch;
            }

            Fail(key, "must be one of POST, PUT, PATCH");
            return null;
        }

        private string? ContentType(string? value, string key)
        {
            if (value is null)
            {
                return null;
            }

            if (value.AsSpan().ContainsAny(ControlChars) || !MediaTypeHeaderValue.TryParse(value, out _))
            {
                Fail(key, "must be a media type such as application/json");
                return null;
            }

            return value;
        }

        private (HttpCredential? Credential, string? Header) Authentication()
        {
            var a = o.Authentication;
            var scheme = a?.Scheme ?? AuthenticationScheme.None;
            if (!Enum.IsDefined(scheme))
            {
                Fail("Authentication.Scheme", $"must be one of {string.Join(", ", Enum.GetNames<AuthenticationScheme>())}");
                return (null, null);
            }

            if (a is null)
            {
                return (null, null);
            }

            var apiKey = scheme == AuthenticationScheme.ApiKey;
            var oauth = scheme == AuthenticationScheme.OAuth2ClientCredentials;
            NotApplicable("Authentication.Credential", a.Credential, scheme != AuthenticationScheme.None, scheme);
            NotApplicable("Authentication.HeaderName", a.HeaderName, apiKey, scheme);
            NotApplicable("Authentication.ValuePrefix", a.ValuePrefix, apiKey, scheme);
            NotApplicable("Authentication.TokenUrl", a.TokenUrl, oauth, scheme);
            NotApplicable("Authentication.Scope", a.Scope, oauth, scheme);
            NotApplicable("Authentication.Audience", a.Audience, oauth, scheme);
            NotApplicable("Authentication.ClientCredentialsIn", a.ClientCredentialsIn, oauth, scheme);
            if (scheme == AuthenticationScheme.None)
            {
                return (null, null);
            }

            if (a.ClientCredentialsIn is { } location && !Enum.IsDefined(location))
            {
                Fail("Authentication.ClientCredentialsIn", $"must be one of {string.Join(", ", Enum.GetNames<ClientCredentialsLocation>())}");
            }

            var credential = scheme switch
            {
                AuthenticationScheme.Basic => Credential("Authentication.Credential", a.Credential, "Username and Password", static c => c.Username, static c => c.Password),
                AuthenticationScheme.Bearer => Credential("Authentication.Credential", a.Credential, "Token", static c => c.Token),
                AuthenticationScheme.ApiKey => Credential("Authentication.Credential", a.Credential, "ApiKey", static c => c.ApiKey),
                _ => Credential("Authentication.Credential", a.Credential, "ClientId and ClientSecret", static c => c.ClientId, static c => c.ClientSecret),
            };

            var header = AuthPlan.AuthorizationHeader;
            if (apiKey)
            {
                header = a.HeaderName;
                if (Blank(header))
                {
                    Fail("Authentication.HeaderName", "required for Scheme ApiKey");
                    header = null;
                }
                else if (!IsToken(header))
                {
                    Fail("Authentication.HeaderName", "must be an RFC 9110 token");
                    header = null;
                }
                else if (IsContentHeader(header!)
                         || (HeaderForwarding.Reserved.Contains(header!) && !header!.Equals(AuthPlan.AuthorizationHeader, StringComparison.OrdinalIgnoreCase)))
                {
                    Fail("Authentication.HeaderName", "must not be a reserved or content header");
                    header = null;
                }

                if (a.ValuePrefix is { } prefix && prefix.AsSpan().ContainsAny(ControlChars))
                {
                    Fail("Authentication.ValuePrefix", "contains CR, LF or NUL");
                }
            }

            if (oauth)
            {
                TokenUrl(a.TokenUrl);
            }

            if (header is not null)
            {
                relayOwned[header] = "Authentication";
            }

            return (credential, header);
        }

        private void TokenUrl(string? text)
        {
            const string key = "Authentication.TokenUrl";
            if (Blank(text))
            {
                Fail(key, "required for Scheme OAuth2ClientCredentials");
                return;
            }

            if (LiteralUrl(text!, key) is { } uri && uri.Scheme != Uri.UriSchemeHttps && !uri.IsLoopback)
            {
                Fail(key, "must be https; http is allowed only for a loopback host");
            }
        }

        private SigningPlan? Signing(string? authHeader)
        {
            var s = o.Signing;
            if (s is null)
            {
                return null;
            }

            var count = failures.Count;
            if (s.Scheme is { } scheme && !Enum.IsDefined(scheme))
            {
                Fail("Signing.Scheme", $"must be one of {string.Join(", ", Enum.GetNames<SigningScheme>())}");
                return null;
            }

            if (s.Encoding is { } encoding && !Enum.IsDefined(encoding))
            {
                Fail("Signing.Encoding", $"must be one of {string.Join(", ", Enum.GetNames<SignatureEncoding>())}");
            }

            HttpCredential? credential = null;
            if (Blank(s.Credential))
            {
                Fail("Signing.Credential", "required");
            }
            else if (!credentials.TryGet(s.Credential!, out credential))
            {
                Fail("Signing.Credential", "not defined in HttpCredentials");
            }

            if (s.WebhookId is { } webhookId)
            {
                HeaderPlaceholders(webhookId, "Signing.WebhookId");
            }

            if (s.Scheme == SigningScheme.HmacSha256 && IsToken(s.HeaderName))
            {
                var header = s.HeaderName!;
                if (HeaderForwarding.Reserved.Contains(header) || IsContentHeader(header))
                {
                    Fail("Signing.HeaderName", "must not be a reserved or content header");
                }
                else if (authHeader is not null && header.Equals(authHeader, StringComparison.OrdinalIgnoreCase))
                {
                    Fail("Signing.HeaderName", "conflicts with the header set by Authentication");
                }
            }

            if (credential is null)
            {
                return null;
            }

            if (!SigningPlan.TryCreate(s, credential, outboxes[0], name, out var plan, out var error))
            {
                failures.Add($"{path}.{error}");
                return null;
            }

            if (plan.Scheme == SigningScheme.HmacSha256)
            {
                relayOwned.TryAdd(plan.HeaderName!, "Signing");
            }
            else
            {
                relayOwned.TryAdd(RequestSigner.WebhookIdHeader, "Signing");
                relayOwned.TryAdd(RequestSigner.WebhookTimestampHeader, "Signing");
                relayOwned.TryAdd(RequestSigner.WebhookSignatureHeader, "Signing");
            }

            return failures.Count > count ? null : plan;
        }

        private void AddDeadLetterNames()
        {
            foreach (var header in DeadLetterHeaderNames.All)
            {
                relayOwned.TryAdd(header, "dead-letter metadata");
            }
        }

        private HeaderForwarding Forwarding()
        {
            var f = o.ForwardHeaders;
            if (f is null)
            {
                return new HeaderForwarding(null);
            }

            var mode = f.Mode ?? ForwardHeadersMode.All;
            if (!Enum.IsDefined(mode))
            {
                Fail("ForwardHeaders.Mode", $"must be one of {string.Join(", ", Enum.GetNames<ForwardHeadersMode>())}");
            }
            else if (mode == ForwardHeadersMode.Listed && f.Names is not { Length: > 0 })
            {
                Fail("ForwardHeaders.Names", "required when Mode is Listed");
            }
            else if (mode != ForwardHeadersMode.Listed && f.Names is not null)
            {
                Fail("ForwardHeaders.Names", "applies only when Mode is Listed");
            }

            Tokens(f.Names, "ForwardHeaders.Names");
            Tokens(f.Exclude, "ForwardHeaders.Exclude");
            if (f.NamePrefix is { Length: > 0 } prefix && !IsToken(prefix))
            {
                Fail("ForwardHeaders.NamePrefix", "must be RFC 9110 token characters");
            }

            return new HeaderForwarding(f);
        }

        private void Tokens(string[]? names, string key)
        {
            for (var i = 0; i < (names?.Length ?? 0); i++)
            {
                if (!IsToken(names![i]))
                {
                    Fail($"{key}[{i}]", "must be an RFC 9110 token");
                }
            }
        }

        private (X509Certificate2Collection? Roots, X509Certificate2? Client) Tls()
        {
            var t = o.Tls;
            if (t is null)
            {
                return (null, null);
            }

            X509Certificate2Collection? roots = null;
            if (t.CaCertificatePath is { } ca)
            {
                try
                {
                    roots = HandlerSettings.LoadCaBundle(ca);
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or CryptographicException or InvalidDataException or ArgumentException)
                {
                    Fail("Tls.CaCertificatePath", "file is missing or holds no PEM certificate");
                }
            }

            var c = t.ClientCertificate;
            if (c is null)
            {
                return (roots, null);
            }

            var pem = c.CertificatePath is not null || c.KeyPath is not null;
            var pfx = c.PfxPath is not null;
            if (pem == pfx)
            {
                Fail("Tls.ClientCertificate", "set CertificatePath and KeyPath, or PfxPath");
                return (roots, null);
            }

            if (pem && (c.CertificatePath is null || c.KeyPath is null))
            {
                Fail("Tls.ClientCertificate", "CertificatePath and KeyPath are both required");
                return (roots, null);
            }

            string? password = null;
            if (pem && c.Credential is not null)
            {
                Fail("Tls.ClientCertificate.Credential", "applies only to PfxPath");
                return (roots, null);
            }

            if (pfx && c.Credential is not null)
            {
                if (Credential("Tls.ClientCertificate.Credential", c.Credential, "CertificatePassword", static x => x.CertificatePassword) is not { } credential)
                {
                    return (roots, null);
                }

                password = credential.CertificatePassword;
            }

            try
            {
                var certificate = HandlerSettings.LoadClientCertificate(c, password);
                if (!certificate.HasPrivateKey)
                {
                    certificate.Dispose();
                    Fail("Tls.ClientCertificate", "certificate has no private key");
                    return (roots, null);
                }

                return (roots, certificate);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or CryptographicException or ArgumentException)
            {
                Fail("Tls.ClientCertificate", "file is missing or does not parse");
                return (roots, null);
            }
        }

        private HealthProbePlan? HealthProbe()
        {
            var p = o.HealthProbe;
            if (p is null)
            {
                return null;
            }

            var method = HttpMethod.Head;
            if (p.Method is { } m)
            {
                if (m.Equals("HEAD", StringComparison.OrdinalIgnoreCase))
                {
                    method = HttpMethod.Head;
                }
                else if (m.Equals("GET", StringComparison.OrdinalIgnoreCase))
                {
                    method = HttpMethod.Get;
                }
                else
                {
                    Fail("HealthProbe.Method", "must be one of HEAD, GET");
                }
            }

            var expected = StatusTable.ParseClass(p.Expected, Key("HealthProbe.Expected"), failures) ?? DefaultProbeExpected;
            if (Blank(p.Url))
            {
                Fail("HealthProbe.Url", "required");
                return null;
            }

            if (LiteralUrl(p.Url!, "HealthProbe.Url") is not { } uri)
            {
                return null;
            }

            schemes.Add(("HealthProbe.Url", uri.Scheme));
            return new HealthProbePlan(uri, method, expected);
        }

        private void Hooks()
        {
            if (!Blank(o.RequestMapper) && !hooks.HasRequestMapper(o.RequestMapper!))
            {
                Fail("RequestMapper", "no request mapper is registered under this name");
            }

            if (!Blank(o.ResponseClassifier) && !hooks.HasResponseClassifier(o.ResponseClassifier!))
            {
                Fail("ResponseClassifier", "no response classifier is registered under this name");
            }
        }

        private HttpEndpointMap? Endpoints(HttpMethod method, string contentType, StatusTable status, SigningPlan? signing)
        {
            var hasUrl = !Blank(o.Url);
            var hasRoutes = o.Routes is { Count: > 0 };
            if (hasUrl == hasRoutes)
            {
                failures.Add(hasUrl ? $"{path}: Url and Routes are both set; set exactly one" : $"{path}: Url or Routes is required");
                return null;
            }

            HeaderNames(o.Headers, "Headers");
            var routes = new List<(string Route, HttpRouteOptions Options, HttpMethod Method, string ContentType)>();
            foreach (var (route, entry) in o.Routes ?? [])
            {
                var key = $"Routes.{route}";
                if (!RouteKey().IsMatch(route))
                {
                    Fail(key, "route key must match ^[a-z0-9][a-z0-9._-]{0,62}$");
                }

                if (entry is null || Blank(entry.Url))
                {
                    Fail($"{key}.Url", "required");
                    continue;
                }

                HeaderNames(entry.Headers, $"{key}.Headers");
                routes.Add((route, entry, Method(entry.Method, $"{key}.Method") ?? method, ContentType(entry.ContentType, $"{key}.ContentType") ?? contentType));
            }

            var staticUris = new Dictionary<string, Uri>(StringComparer.Ordinal);
            var built = new Dictionary<string, HttpEndpoint>(StringComparer.OrdinalIgnoreCase);
            var builtRoutes = new Dictionary<string, IReadOnlyDictionary<string, HttpEndpoint>>(StringComparer.OrdinalIgnoreCase);
            foreach (var outbox in outboxes.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                var webhookId = WebhookId(signing, outbox);
                if (hasUrl)
                {
                    if (Endpoint(outbox, null, o.Url!, "Url", method, contentType, o.Headers, null, status, webhookId, staticUris) is { } e)
                    {
                        built[outbox] = e;
                    }

                    continue;
                }

                var byRoute = new Dictionary<string, HttpEndpoint>(StringComparer.Ordinal);
                foreach (var (route, entry, routeMethod, routeContentType) in routes)
                {
                    if (Endpoint(outbox, route, entry.Url!, $"Routes.{route}.Url", routeMethod, routeContentType, o.Headers, entry.Headers, status, webhookId, staticUris) is { } e)
                    {
                        byRoute[route] = e;
                    }
                }

                builtRoutes[outbox] = byRoute;
            }

            if (Failed)
            {
                return null;
            }

            return hasUrl ? HttpEndpointMap.ForUrl(built) : HttpEndpointMap.ForRoutes(builtRoutes);
        }

        private ValueTemplate? WebhookId(SigningPlan? signing, string outbox)
        {
            if (signing?.Scheme != SigningScheme.StandardWebhooks)
            {
                return null;
            }

            ValueTemplate.TryCompile(
                o.Signing!.WebhookId ?? SigningPlan.DefaultWebhookId, TemplateTarget.HeaderValue, outbox, name, route: null, out var template, out _);
            return template;
        }

        private HttpEndpoint? Endpoint(
            string outbox,
            string? route,
            string url,
            string urlKey,
            HttpMethod method,
            string contentType,
            Dictionary<string, string>? shared,
            Dictionary<string, string>? own,
            StatusTable status,
            ValueTemplate? webhookId,
            Dictionary<string, Uri> staticUris)
        {
            var uri = Url(url, urlKey, outbox, route);
            var entries = new List<(string Name, ValueTemplate Value)>();
            var ok = uri is not null;
            foreach (var (headers, prefix) in new[] { (shared, "Headers"), (own, $"Routes.{route}.Headers") })
            {
                foreach (var (header, value) in headers ?? [])
                {
                    var key = $"{prefix}.{header}";
                    if (!ValueTemplate.TryCompile(value ?? string.Empty, TemplateTarget.HeaderValue, outbox, name, route, out var template, out var error))
                    {
                        Fail(key, error);
                        ok = false;
                        continue;
                    }

                    entries.Add((header, template));
                }
            }

            if (!ok)
            {
                return null;
            }

            ValueTemplate.TryCompile(url, TemplateTarget.Url, outbox, name, route, out var urlTemplate, out _);
            Uri? staticUri = null;
            if (urlTemplate!.IsConstant)
            {
                if (!staticUris.TryGetValue(urlTemplate.Constant, out staticUri))
                {
                    staticUri = new Uri(urlTemplate.Constant, UriKind.Absolute);
                    staticUris.Add(urlTemplate.Constant, staticUri);
                }

                urlTemplate = null;
            }

            return new HttpEndpoint(
                method,
                staticUri,
                urlTemplate,
                new HeaderPlan(entries, relayOwned.Keys),
                contentType,
                route,
                status,
                webhookId);
        }

        private void HeaderNames(Dictionary<string, string>? headers, string prefix)
        {
            foreach (var (header, value) in headers ?? [])
            {
                var key = $"{prefix}.{header}";
                if (!IsToken(header))
                {
                    Fail(key, "header name must be an RFC 9110 token");
                }
                else if (header.Equals(ContentTypeHeader, StringComparison.OrdinalIgnoreCase))
                {
                    Fail(key, "Content-Type is set by ContentType; use ContentType");
                }
                else if (header.Equals(AuthPlan.AuthorizationHeader, StringComparison.OrdinalIgnoreCase))
                {
                    Fail(key, "Authorization is set by Authentication; use Authentication");
                }
                else if (HeaderForwarding.Reserved.Contains(header))
                {
                    Fail(key, "is a hop-by-hop or framing header and cannot be set");
                }
                else if (relayOwned.TryGetValue(header, out var owner))
                {
                    Fail(key, $"conflicts with the relay-owned header set by {owner}");
                }

                HeaderPlaceholders(value ?? string.Empty, key);
            }
        }

        private void HeaderPlaceholders(string text, string key)
        {
            var at = text.IndexOf(HeaderPlaceholder, StringComparison.Ordinal);
            while (at >= 0)
            {
                var start = at + HeaderPlaceholder.Length;
                var close = text.IndexOf('}', start);
                if (close < 0)
                {
                    return;
                }

                if (!IsToken(text[start..close]))
                {
                    Fail(key, "a {header:<name>} placeholder must name an RFC 9110 token");
                    return;
                }

                at = text.IndexOf(HeaderPlaceholder, close, StringComparison.Ordinal);
            }
        }

        private Uri? Url(string text, string key, string outbox, string? route)
        {
            if (!ValueTemplate.TryCompile(text, TemplateTarget.Url, outbox, name, route, out _, out var error))
            {
                Fail(key, error);
                return null;
            }

            HeaderPlaceholders(text, key);
            var schemeEnd = text.IndexOf("://", StringComparison.Ordinal);
            if (schemeEnd <= 0)
            {
                Fail(key, "must be an absolute http or https URL");
                return null;
            }

            var authorityStart = schemeEnd + 3;
            var authorityEnd = text.AsSpan(authorityStart).IndexOfAny('/', '?', '#');
            authorityEnd = authorityEnd < 0 ? text.Length : authorityStart + authorityEnd;
            if (text.AsSpan(0, authorityEnd).Contains('{'))
            {
                Fail(key, "placeholders are allowed only in path and query");
                return null;
            }

            if (text.AsSpan(authorityStart, authorityEnd - authorityStart).Contains('@'))
            {
                Fail(key, "must not carry user info");
                return null;
            }

            if (text.Contains('#'))
            {
                Fail(key, "must not carry a fragment");
                return null;
            }

            var sample = Sample(text, outbox, route);
            if (!Uri.TryCreate(sample, UriKind.Absolute, out var uri)
                || (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps)
                || uri.Host.Length == 0)
            {
                Fail(key, "must be an absolute http or https URL");
                return null;
            }

            var pathAndQuery = sample[authorityEnd..];
            var expected = pathAndQuery.Length == 0 || pathAndQuery[0] == '?' ? "/" + pathAndQuery : pathAndQuery;
            if (!string.Equals(uri.PathAndQuery, expected, StringComparison.Ordinal))
            {
                Fail(key, "path and query must be canonical: percent-encode reserved and non-ASCII characters, no dot segments");
                return null;
            }

            host ??= uri.Host;
            if (outbox == outboxes[0])
            {
                schemes.Add((key, uri.Scheme));
            }

            return uri;
        }

        private string Sample(string text, string outbox, string? route)
        {
            var sample = new StringBuilder(text.Length);
            var position = 0;
            while (position < text.Length)
            {
                var open = text.IndexOf('{', position);
                if (open < 0)
                {
                    sample.Append(text, position, text.Length - position);
                    break;
                }

                var close = text.IndexOf('}', open);
                sample.Append(text, position, open - position);
                sample.Append(text[(open + 1)..close] switch
                {
                    "outbox" => Uri.EscapeDataString(outbox),
                    "destination" => Uri.EscapeDataString(name),
                    "route" => Uri.EscapeDataString(route!),
                    _ => "x",
                });
                position = close + 1;
            }

            return sample.ToString();
        }

        private Uri? LiteralUrl(string text, string key)
        {
            if (text.Contains('{') || text.Contains('}'))
            {
                Fail(key, "must not contain placeholders");
                return null;
            }

            if (!Uri.TryCreate(text, UriKind.Absolute, out var uri)
                || (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps)
                || uri.Host.Length == 0)
            {
                Fail(key, "must be an absolute http or https URL");
                return null;
            }

            if (uri.UserInfo.Length > 0)
            {
                Fail(key, "must not carry user info");
                return null;
            }

            if (uri.Fragment.Length > 0 || text.Contains('#'))
            {
                Fail(key, "must not carry a fragment");
                return null;
            }

            return uri;
        }

        private void HttpVersionSchemes(string? version)
        {
            if (version == HandlerSettings.Http2 && schemes.Any(static s => s.Scheme != Uri.UriSchemeHttps))
            {
                Fail("HttpVersion", "\"2\" requires https URLs; use \"2-cleartext\" for prior-knowledge h2c");
            }
            else if (version == HandlerSettings.Http2Cleartext && schemes.Any(static s => s.Scheme != Uri.UriSchemeHttp))
            {
                Fail("HttpVersion", "\"2-cleartext\" requires http URLs; use \"2\" for HTTP/2 over TLS");
            }
        }

        private HttpCredential? Credential(string key, string? credentialName, string fields, params Func<HttpCredential, string?>[] needed)
        {
            if (Blank(credentialName))
            {
                Fail(key, "required");
                return null;
            }

            if (!credentials.TryGet(credentialName!, out var credential))
            {
                Fail(key, "not defined in HttpCredentials");
                return null;
            }

            if (needed.Any(get => Blank(get(credential))))
            {
                Fail(key, $"credential needs {fields}");
                return null;
            }

            return credential;
        }

        private void NotApplicable(string key, object? value, bool applies, AuthenticationScheme scheme)
        {
            if (value is not null && !applies)
            {
                Fail(key, $"does not apply to Scheme {scheme}");
            }
        }

        private string Key(string key) => $"{path}.{key}";

        private void Fail(string key, string rule)
        {
            var failure = $"{path}.{key}: {rule}";
            if (reported.Add(failure))
            {
                failures.Add(failure);
            }
        }

        private static bool Blank(string? value) => string.IsNullOrWhiteSpace(value);
    }
}
