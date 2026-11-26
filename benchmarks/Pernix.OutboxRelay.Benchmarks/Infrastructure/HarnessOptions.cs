using System.Globalization;
using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

internal sealed record HarnessOptions(
    string Verb,
    OrderingMode[] Modes,
    string[] Dests,
    int[] Headers,
    int[] Batches,
    long? Count,
    long? Warm,
    int Rounds,
    int Seconds,
    string? Target,
    int Sessions,
    bool Profile,
    bool Otel,
    string[] Urls,
    int Response,
    bool Socket,
    bool Http2)
{
    public static readonly string[] Verbs = ["term", "kafka", "kafka-dlq", "session", "fixed", "idle", "flat", "churn", "loopback", "http-floor", "http", "http-dlq", "http-stub", "all"];

    public const string Usage = """
        usage: dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- <verb> [options]
          bdn [BenchmarkDotNet args]   BenchmarkDotNet suites, e.g. bdn --filter '*DeliveryTerm*'
          term | kafka | kafka-dlq | session | fixed | idle | flat | churn | loopback | http-floor | http | http-dlq | http-stub | all (default)
                                       GC-delta scenarios
        options:
          --modes None,PerKey,Strict      --dests null,kafka,dlq,http,http-tpl,http-sat,http-dlq,http2 (idle: null,kafka,http)
          --headers 0,1,3,4 (term, kafka, kafka-dlq, http-floor, http, http-dlq, http-stub, flat --target term; session: 1,3)
          --batches 500,100,10,1          --n <count per round>
          --warm <warm-up count>          --rounds <n> (default 3)          --seconds <idle window> (default 30)
          --target term|session           --sessions <churn sessions> (default 4000)
          --urls static,tpl,tpl-raw (http-floor; http, http-dlq, http-stub: static,tpl)
          --response 200|204 (loopback, http-floor, http, http-dlq, http-stub)
          --socket (http: child-process socket server instead of the in-memory loopback)
          --http2 (loopback: prior-knowledge h2c fake instead of HTTP/1.1)
          --profile                       --otel
        """;

    private static readonly OrderingMode[] AllModes = [OrderingMode.None, OrderingMode.PerKey, OrderingMode.Strict];
    private static readonly string[] TermDests = ["null", "kafka", "dlq", "http", "http-tpl", "http-sat", "http-dlq", "http2"];
    private static readonly string[] IdleDests = ["null", "kafka"];
    private static readonly string[] Targets = ["term", "session"];
    private static readonly int[] HeaderCounts = [0, 1, 3, 4];
    private static readonly string[] UrlKinds = ["static", "tpl", "tpl-raw"];
    private static readonly int[] Responses = [200, 204];

    // `session` takes 1 or 3. PgEnvironment.SeedAsync can also seed 0 headers, which is excluded by choice.
    private static readonly int[] SeededHeaderCounts = [1, 3];
    private static readonly string[] ValueOptions =
        ["--modes", "--dests", "--headers", "--batches", "--n", "--warm", "--rounds", "--seconds", "--target", "--sessions", "--urls", "--response"];

    public static HarnessOptions Parse(string verb, IReadOnlyList<string> args)
    {
        if (!Verbs.Contains(verb, StringComparer.Ordinal))
        {
            throw new UsageException($"unknown verb '{verb}'");
        }

        var options = Defaults(verb);
        var headersGiven = false;
        for (var i = 0; i < args.Count; i++)
        {
            var name = args[i];
            switch (name)
            {
                case "--profile":
                    options = options with { Profile = true };
                    continue;
                case "--otel":
                    options = options with { Otel = true };
                    continue;
                case "--socket" when verb == "http":
                    options = options with { Socket = true };
                    continue;
                case "--http2" when verb == "loopback":
                    options = options with { Http2 = true };
                    continue;
            }

            if (!ValueOptions.Contains(name, StringComparer.Ordinal))
            {
                throw new UsageException($"unknown option '{name}'");
            }

            if (i + 1 >= args.Count)
            {
                throw new UsageException($"option '{name}' needs a value");
            }

            var value = args[++i];
            headersGiven |= name == "--headers";
            options = name switch
            {
                "--modes" => options with { Modes = List(name, value, ParseMode) },
                "--dests" => options with { Dests = List(name, value, v => OneOf(name, v, TermDests)) },
                "--headers" => options with { Headers = List(name, value, v => OneOf(name, ParseInt(name, v, 0), HeaderCounts)) },
                "--batches" => options with { Batches = List(name, value, v => ParseInt(name, v, 1)) },
                "--n" => options with { Count = ParseLong(name, value) },
                "--warm" => options with { Warm = ParseLong(name, value, min: 0) },
                "--rounds" => options with { Rounds = ParseInt(name, value, 1) },
                "--seconds" => options with { Seconds = ParseInt(name, value, 1) },
                "--target" => options with { Target = OneOf(name, value, Targets) },
                "--sessions" => options with { Sessions = ParseInt(name, value, 1) },
                "--urls" => options with { Urls = List(name, value, v => OneOf(name, v, UrlKinds)) },
                "--response" => options with { Response = OneOf(name, ParseInt(name, value, 0), Responses) },
                _ => throw new UsageException($"unknown option '{name}'"),
            };
        }

        if (headersGiven)
        {
            var allowed = HeadersFor(verb, options.Target);
            if (allowed.Length == 0)
            {
                throw new UsageException(verb == "flat"
                    ? "option '--headers (flat)': only with --target term, session uses 1 header"
                    : $"option '--headers': '{verb}' does not read it");
            }

            foreach (var headers in options.Headers)
            {
                OneOf($"--headers ({verb})", headers, allowed);
            }
        }

        return options;
    }

    private static int[] HeadersFor(string verb, string? target) => verb switch
    {
        "term" or "kafka" or "kafka-dlq" or "http-floor" or "http" or "http-dlq" or "http-stub" => HeaderCounts,
        "session" => SeededHeaderCounts,
        "flat" when target == "term" => HeaderCounts,
        _ => [],
    };

    private static HarnessOptions Defaults(string verb) => new(
        verb,
        AllModes,
        verb == "idle" ? IdleDests : TermDests,
        verb switch
        {
            "kafka" or "kafka-dlq" or "http-floor" or "http" or "http-dlq" or "http-stub" => [0, 1, 4],
            "session" => [1, 3],
            _ => [1],
        },
        [500, 100, 10, 1],
        Count: null,
        Warm: null,
        Rounds: 3,
        Seconds: 30,
        Target: null,
        Sessions: 4_000,
        Profile: false,
        Otel: false,
        Urls: verb switch
        {
            "http" or "http-dlq" => ["static"],
            "http-stub" => ["static", "tpl"],
            _ => ["static", "tpl", "tpl-raw"],
        },
        Response: 200,
        Socket: false,
        Http2: false);

    private static T[] List<T>(string name, string value, Func<string, T> parse)
    {
        var parts = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 0)
        {
            throw new UsageException($"option '{name}' needs a value");
        }

        return [.. parts.Select(parse).Distinct()];
    }

    private static OrderingMode ParseMode(string value) =>
        Enum.TryParse<OrderingMode>(value, ignoreCase: true, out var mode) && AllModes.Contains(mode)
            ? mode
            : throw new UsageException($"option '--modes': '{value}' is not None, PerKey or Strict");

    private static T OneOf<T>(string name, T value, T[] allowed) =>
        allowed.Contains(value) ? value : throw new UsageException($"option '{name}': '{value}' is not one of {string.Join(", ", allowed)}");

    private static int ParseInt(string name, string value, int min) =>
        int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var n) && n >= min
            ? n
            : throw new UsageException($"option '{name}': '{value}' is not an integer >= {min}");

    private static long ParseLong(string name, string value, long min = 1) =>
        long.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var n) && n >= min
            ? n
            : throw new UsageException($"option '{name}': '{value}' is not an integer >= {min}");
}

internal sealed class UsageException(string message) : Exception(message);
