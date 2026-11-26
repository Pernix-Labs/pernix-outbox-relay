using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Pernix.OutboxRelay.Integration.Tests.Delivery;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Http;

internal sealed record RecordedRequest(
    long Sequence,
    string Method,
    string Target,
    string Protocol,
    IReadOnlyList<KeyValuePair<string, string>> Headers,
    byte[] Body,
    string ConnectionId,
    string? ClientCertificateThumbprint)
{
    public string Path => Target.Split('?', 2)[0];

    public IReadOnlyList<string> Values(string name) =>
        [.. Headers.Where(h => string.Equals(h.Key, name, StringComparison.OrdinalIgnoreCase)).Select(h => h.Value)];

    public string? Header(string name) => Values(name) is [var only] ? only : null;
}

internal sealed record EndpointResponse(int Status, TimeSpan Delay = default)
{
    public static readonly EndpointResponse Ok = new(200);
}

internal sealed record EndpointOptions
{
    public HttpProtocols Protocols { get; init; } = HttpProtocols.Http1;

    public X509Certificate2? ServerCertificate { get; init; }

    public X509Certificate2Collection? ClientCertificateRoots { get; init; }
}

internal sealed class HttpTestEndpoint : IAsyncDisposable
{
    private static readonly byte[] ReferenceBody = Encoding.ASCII.GetBytes("{\"status\":\"accepted\",\"detail\":\"" + new string('x', 68) + "\"}");

    private readonly WebApplication app;
    private readonly ConcurrentQueue<RecordedRequest> requests = new();
    private long sequence;
    private int connects;
    private int inFlight;
    private int peakInFlight;

    private HttpTestEndpoint(WebApplication app, Uri baseUri)
    {
        this.app = app;
        BaseUri = baseUri;
    }

    public Uri BaseUri { get; }

    public Func<RecordedRequest, EndpointResponse> Respond { get; set; } = static _ => EndpointResponse.Ok;

    public IReadOnlyList<RecordedRequest> Requests => [.. requests.OrderBy(r => r.Sequence)];

    public int Connects => Volatile.Read(ref connects);

    public int PeakInFlight => Volatile.Read(ref peakInFlight);

    public string Url(string pathAndQuery) => BaseUri.GetLeftPart(UriPartial.Authority) + pathAndQuery;

    public IReadOnlyList<RecordedRequest> RequestsTo(string path) => [.. Requests.Where(r => r.Path == path)];

    public static async Task<HttpTestEndpoint> StartAsync(EndpointOptions? options = null)
    {
        options ??= new EndpointOptions();
        HttpTestEndpoint? endpoint = null;
        var builder = WebApplication.CreateSlimBuilder();
        // The output folder holds the relay image's appsettings.json, whose Kestrel endpoint would bind here too.
        builder.Configuration.Sources.Clear();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            // Row header values are UTF-8, Kestrel's default is ASCII only.
            kestrel.RequestHeaderEncodingSelector = static _ => Encoding.UTF8;
            kestrel.Listen(IPAddress.Loopback, 0, listen => Configure(listen, options, () => endpoint!));
        });

        var app = builder.Build();
        app.Run(context => endpoint!.HandleAsync(context));
        await app.StartAsync();

        var address = app.Services.GetRequiredService<IServer>().Features.GetRequiredFeature<IServerAddressesFeature>().Addresses.Single();
        endpoint = new HttpTestEndpoint(app, new Uri(address));
        return endpoint;
    }

    private static void Configure(ListenOptions listen, EndpointOptions options, Func<HttpTestEndpoint> endpoint)
    {
        listen.Protocols = options.Protocols;

        // Before TLS: counts TCP connections, not requests.
        listen.Use(next => connection =>
        {
            Interlocked.Increment(ref endpoint().connects);
            return next(connection);
        });

        if (options.ServerCertificate is { } certificate)
        {
            listen.UseHttps(https =>
            {
                https.ServerCertificate = certificate;
                if (options.ClientCertificateRoots is { } roots)
                {
                    https.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
                    https.ClientCertificateValidation = (client, _, _) => TestCertificates.ChainsTo(client, roots, TestCertificates.ClientAuthentication);
                }
            });
        }
    }

    public async ValueTask DisposeAsync()
    {
        await app.StopAsync();
        await app.DisposeAsync();
    }

    private async Task HandleAsync(HttpContext context)
    {
        var current = Interlocked.Increment(ref inFlight);
        try
        {
            int peak;
            while (current > (peak = Volatile.Read(ref peakInFlight)) && Interlocked.CompareExchange(ref peakInFlight, current, peak) != peak)
            {
            }

            using var body = new MemoryStream();
            await context.Request.Body.CopyToAsync(body, context.RequestAborted);

            var recorded = new RecordedRequest(
                Interlocked.Increment(ref sequence),
                context.Request.Method,
                context.Features.GetRequiredFeature<IHttpRequestFeature>().RawTarget,
                context.Request.Protocol,
                [.. context.Request.Headers.SelectMany(h => h.Value.Select(v => KeyValuePair.Create(h.Key, v ?? string.Empty)))],
                body.ToArray(),
                context.Connection.Id,
                context.Connection.ClientCertificate?.Thumbprint);
            requests.Enqueue(recorded);

            var response = Respond(recorded);
            if (response.Delay > TimeSpan.Zero)
            {
                await Task.Delay(response.Delay, context.RequestAborted);
            }

            context.Response.StatusCode = response.Status;
            context.Response.ContentType = "application/json";
            context.Response.ContentLength = ReferenceBody.Length;
            context.Response.Headers["X-Request-Id"] = recorded.Sequence.ToString(CultureInfo.InvariantCulture);
            context.Response.Headers.CacheControl = "no-store";
            await context.Response.Body.WriteAsync(ReferenceBody, context.RequestAborted);
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
        }
        finally
        {
            Interlocked.Decrement(ref inFlight);
        }
    }
}

internal sealed class TestCertificates : IDisposable
{
    public static readonly Oid ServerAuthentication = new("1.3.6.1.5.5.7.3.1");
    public static readonly Oid ClientAuthentication = new("1.3.6.1.5.5.7.3.2");

    private readonly string directory;

    private TestCertificates(string directory, X509Certificate2 ca, X509Certificate2 server, X509Certificate2 client)
    {
        this.directory = directory;
        Ca = ca;
        Server = server;
        Client = client;
        CaPath = Path.Combine(directory, "ca.pem");
        ClientCertificatePath = Path.Combine(directory, "client.crt");
        ClientKeyPath = Path.Combine(directory, "client.key");
    }

    public X509Certificate2 Ca { get; }

    public X509Certificate2 Server { get; }

    public X509Certificate2 Client { get; }

    public string CaPath { get; }

    public string ClientCertificatePath { get; }

    public string ClientKeyPath { get; }

    public X509Certificate2Collection Roots => [Ca];

    public static TestCertificates Create()
    {
        var now = DateTimeOffset.UtcNow;
        using var caKey = RSA.Create(2048);
        var caRequest = new CertificateRequest("CN=Outbox Relay Test CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        caRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        caRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(caRequest.PublicKey, false));
        var ca = caRequest.CreateSelfSigned(now.AddDays(-1), now.AddDays(30));

        var san = new SubjectAlternativeNameBuilder();
        san.AddIpAddress(IPAddress.Loopback);
        san.AddDnsName("localhost");
        var server = Issue(ca, "CN=127.0.0.1", ServerAuthentication, san.Build(), now);
        var client = Issue(ca, "CN=outbox-relay-client", ClientAuthentication, extension: null, now);

        var directory = Path.Combine(Path.GetTempPath(), $"relay-tls-{Guid.NewGuid():N}");
        Directory.CreateDirectory(directory);
        var certificates = new TestCertificates(directory, ca, server, client);
        File.WriteAllText(certificates.CaPath, ca.ExportCertificatePem());
        File.WriteAllText(certificates.ClientCertificatePath, client.ExportCertificatePem());
        using (var key = client.GetRSAPrivateKey()!)
        {
            File.WriteAllText(certificates.ClientKeyPath, key.ExportPkcs8PrivateKeyPem());
        }

        return certificates;
    }

    public static bool ChainsTo(X509Certificate2 certificate, X509Certificate2Collection roots, Oid usage)
    {
        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.AddRange(roots);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.ApplicationPolicy.Add(usage);
        return chain.Build(certificate);
    }

    public void Dispose()
    {
        Ca.Dispose();
        Server.Dispose();
        Client.Dispose();
        Directory.Delete(directory, recursive: true);
    }

    private static X509Certificate2 Issue(X509Certificate2 ca, string subject, Oid usage, X509Extension? extension, DateTimeOffset now)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([usage], false));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(ca, true, false));
        if (extension is not null)
        {
            request.CertificateExtensions.Add(extension);
        }

        var serial = RandomNumberGenerator.GetBytes(16);
        serial[0] &= 0x7F;
        using var issued = request.Create(ca, now.AddDays(-1), now.AddDays(30), serial);

        // A PKCS#12 round trip gives a persisted key, which SslStream needs on some platforms.
        using var withKey = issued.CopyWithPrivateKey(key);
        return X509CertificateLoader.LoadPkcs12(withKey.Export(X509ContentType.Pkcs12), null, X509KeyStorageFlags.Exportable);
    }
}

internal sealed record HttpRow(string? Key, string Payload, IReadOnlyDictionary<string, string>? Headers = null, string? Route = null)
{
    public byte[] PayloadBytes => Encoding.UTF8.GetBytes(Payload);
}

internal sealed class HttpOutbox
{
    public const string DestinationName = RelayFactory.DestinationName;
    public const string DeadLetterDestinationName = DeliverySetup.DeadLetterDestinationName;

    private readonly string connectionString;

    private HttpOutbox(string connectionString, string schema, Dictionary<string, string?> settings)
    {
        this.connectionString = connectionString;
        Table = $"{schema}.outbox";
        Settings = settings;
        Outbox = settings["Outboxes:0:Name"]!;
    }

    public string Table { get; }

    public string Outbox { get; }

    public Dictionary<string, string?> Settings { get; }

    // Root keys added as given, for sections RelayFactory does not prefix (HttpCredentials).
    public Dictionary<string, string?> RootSettings { get; } = [];

    public RelayLogCapture Logs { get; } = new();

    public static async Task<HttpOutbox> CreateAsync(PostgreSqlFixture postgres, string orderingMode = "None", string? url = null)
    {
        var schema = await postgres.CreateOutboxAsync();
        var settings = new Dictionary<string, string?>
        {
            ["ConnectionStrings:Main"] = postgres.ConnectionString,
            ["Destinations:0:Name"] = DestinationName,
            ["Destinations:0:Transport"] = "Http",
            ["Outboxes:0:Name"] = RelayFactory.NewOutboxName(),
            ["Outboxes:0:Destination"] = DestinationName,
            ["Outboxes:0:Database:Provider"] = "PostgreSql",
            ["Outboxes:0:Database:Connection"] = "Main",
            ["Outboxes:0:Database:Table"] = $"{schema}.outbox",
            ["Outboxes:0:Ordering:Mode"] = orderingMode,
            ["Outboxes:0:Polling:Interval"] = "00:00:00.200",
            ["Outboxes:0:Retry:InitialDelay"] = "00:00:01",
            ["Outboxes:0:Retry:MaxDelay"] = "00:00:02",
        };

        if (url is not null)
        {
            settings["Destinations:0:Http:Url"] = url;
        }

        return new HttpOutbox(postgres.ConnectionString, schema, settings);
    }

    public void Http(string key, string? value) => Settings[$"Destinations:0:Http:{key}"] = value;

    public void DeadLetterTo(string transport, IReadOnlyDictionary<string, string?> transportSettings, int maxRetryAttempts = 1)
    {
        Settings["Destinations:1:Name"] = DeadLetterDestinationName;
        Settings["Destinations:1:Transport"] = transport;
        foreach (var (key, value) in transportSettings)
        {
            Settings[$"Destinations:1:{transport}:{key}"] = value;
        }

        Settings["Outboxes:0:DeadLetter:Destination"] = DeadLetterDestinationName;
        Settings["Outboxes:0:Retry:MaxRetryAttempts"] = maxRetryAttempts.ToString(CultureInfo.InvariantCulture);
    }

    public RelayFactory Relay() => new RelayFactory()
        .WithRelaySettings(Settings)
        .WithRootSettings(RootSettings)
        .WithServices(s => s.AddSingleton<ILoggerProvider>(Logs));

    public async Task<IReadOnlyList<long>> InsertAsync(params HttpRow[] rows)
    {
        await using var connection = await OpenAsync();
        var ids = new List<long>();
        foreach (var row in rows)
        {
            await using var command = new NpgsqlCommand(
                $"INSERT INTO {Table} (message_key, payload, headers, route) VALUES (@key, @payload, @headers::jsonb, @route) RETURNING id",
                connection);
            command.Parameters.Add(new NpgsqlParameter("key", NpgsqlTypes.NpgsqlDbType.Bytea) { Value = row.Key is null ? DBNull.Value : Encoding.UTF8.GetBytes(row.Key) });
            command.Parameters.AddWithValue("payload", row.PayloadBytes);
            command.Parameters.AddWithValue("headers", JsonSerializer.Serialize(row.Headers ?? new Dictionary<string, string>()));
            command.Parameters.Add(new NpgsqlParameter("route", NpgsqlTypes.NpgsqlDbType.Text) { Value = (object?)row.Route ?? DBNull.Value });
            ids.Add((long)(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken))!);
        }

        return ids;
    }

    public async Task<long> CountAsync()
    {
        await using var connection = await OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM {Table}", connection);
        return (long)(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    public async Task<IReadOnlyList<RowFailureState>> FailureStatesAsync()
    {
        await using var connection = await OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT id, retry_count, last_error FROM {Table} ORDER BY id", connection);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        var states = new List<RowFailureState>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            states.Add(new RowFailureState(reader.GetInt64(0), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
        }

        return states;
    }

    public Task<bool> DrainedAsync(TimeSpan timeout) =>
        LeadershipTests.EventuallyAsync(async () => await CountAsync() == 0, timeout);

    private async Task<NpgsqlConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }
}

internal sealed record LogEntry(string Category, LogLevel Level, int EventId, string Message, IReadOnlyDictionary<string, string> State);

internal sealed class RelayLogCapture : ILoggerProvider
{
    public const int DeliveryPaused = 110;
    public const int DeliveryResumed = 112;

    private readonly ConcurrentQueue<LogEntry> entries = new();

    public IReadOnlyList<LogEntry> Entries => [.. entries];

    public IReadOnlyList<string> PauseErrors =>
        [.. entries.Where(e => e.EventId == DeliveryPaused && e.State.GetValueOrDefault("Kind") == "DestinationFault").Select(e => e.State["Error"])];

    public ILogger CreateLogger(string categoryName) => new Logger(categoryName, entries);

    public void Dispose()
    {
    }

    private sealed class Logger(string category, ConcurrentQueue<LogEntry> entries) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var values = (state as IEnumerable<KeyValuePair<string, object?>> ?? [])
                .GroupBy(kv => kv.Key, StringComparer.Ordinal)
                .ToDictionary(g => g.Key, g => g.Last().Value?.ToString() ?? string.Empty, StringComparer.Ordinal);
            entries.Enqueue(new LogEntry(category, logLevel, eventId.Id, formatter(state, exception) + exception, values));
        }
    }
}
