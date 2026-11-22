using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using BenchmarkDotNet.Attributes;

namespace Pernix.OutboxRelay.Benchmarks.Suites;

public enum HeadersCase
{
    One,
    Three,
    Escaped,
}

[Config(typeof(BenchConfig))]
public class HeadersParserBenchmarks
{
    private HeadersParser parser = null!;
    private byte[] json = null!;

    [Params(HeadersCase.One, HeadersCase.Three, HeadersCase.Escaped)]
    public HeadersCase Case { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        parser = new HeadersParser();
        json = Case switch
        {
            HeadersCase.One => """{"idempotency-key": "123456"}"""u8.ToArray(),
            HeadersCase.Three =>
                """{"idempotency-key": "123456", "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "content-type": "application/json"}"""u8.ToArray(),
            HeadersCase.Escaped => """{"idempotency\u002Dkey": "123456"}"""u8.ToArray(),
            _ => throw new ArgumentOutOfRangeException(nameof(Case), Case, "unknown headers case"),
        };

        var parsed = parser.Parse(json)
            ?? throw new InvalidOperationException($"headers parser: case {Case} did not parse");
        if (parsed[0].Name != "idempotency-key")
        {
            throw new InvalidOperationException($"headers parser: case {Case} parsed name {parsed[0].Name}, expected idempotency-key");
        }
    }

    [Benchmark]
    public MessageHeaders? Parse() => parser.Parse(json);
}
