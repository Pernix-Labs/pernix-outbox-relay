using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.Core;

public sealed class OutboxMessageTests
{
    private const string Secret = "SECRETVALUE";

    [Fact]
    public void Create_with_string_payload_writes_utf8_bytes_of_key_and_payload()
    {
        var message = OutboxMessage.Create("kéy-😀", "päyload-Ω");

        Assert.Equal(Encoding.UTF8.GetBytes("kéy-😀"), message.Key!.Value.ToArray());
        Assert.Equal(Encoding.UTF8.GetBytes("päyload-Ω"), message.Payload.ToArray());
    }

    [Fact]
    public void Create_with_byte_payload_keeps_the_given_memory()
    {
        var buffer = new byte[] { 0xFF, 0x00, 0xC3, 0x28, 0x7F };
        var payload = new ReadOnlyMemory<byte>(buffer, 1, 3);

        var message = OutboxMessage.Create("k", payload);

        Assert.True(payload.Equals(message.Payload));
        Assert.Equal(Encoding.UTF8.GetBytes("k"), message.Key!.Value.ToArray());
    }

    [Fact]
    public void Json_payload_equals_the_type_info_utf8_output()
    {
        var value = new Sample("Ελληνικά \"quoted\" <tag>", 42);

        var message = OutboxMessage.Json("k", value, CoreTestJsonContext.Default.Sample);

        Assert.Equal(JsonSerializer.SerializeToUtf8Bytes(value, CoreTestJsonContext.Default.Sample), message.Payload.ToArray());
        Assert.Equal(Encoding.UTF8.GetBytes("k"), message.Key!.Value.ToArray());
    }

    [Fact]
    public void From_bytes_with_memory_key_keeps_key_and_payload_memory_in_place()
    {
        var buffer = new byte[] { 9, 1, 2, 3, 4, 5, 9 };
        var payload = new ReadOnlyMemory<byte>(buffer, 1, 3);
        var key = new ReadOnlyMemory<byte>(buffer, 4, 2);

        var message = OutboxMessage.FromBytes(key, payload);

        Assert.True(payload.Equals(message.Payload));
        Assert.True(key.Equals(message.Key!.Value));
    }

    // Key first, not swapped.
    [Fact]
    public void From_bytes_with_array_key_takes_key_first_and_does_not_copy()
    {
        byte[] key = [4, 5];
        byte[] payload = [1, 2, 3];

        var message = OutboxMessage.FromBytes(key, payload);

        Assert.True(MemoryExtensions.SequenceEqual(key.AsSpan(), message.Key!.Value.Span));
        Assert.True(MemoryExtensions.SequenceEqual(payload.AsSpan(), message.Payload.Span));
        Assert.True(new ReadOnlyMemory<byte>(key).Equals(message.Key!.Value));
    }

    // A null byte[] key is a null key, not an empty one.
    [Fact]
    public void From_bytes_with_null_array_key_is_null_key()
    {
        byte[]? key = null;

        Assert.Null(OutboxMessage.FromBytes(key, "p"u8.ToArray()).Key);
        Assert.Null(OutboxMessage.FromBytes(null, "p"u8.ToArray()).Key);
        Assert.Null(OutboxMessage.FromBytes(default, "p"u8.ToArray()).Key);
        Assert.Null(OutboxMessage.FromBytes((ReadOnlyMemory<byte>?)null, "p"u8.ToArray()).Key);
    }

    [Fact]
    public void From_bytes_with_empty_key_is_empty_non_null_key()
    {
        var fromArray = OutboxMessage.FromBytes(Array.Empty<byte>(), "p"u8.ToArray()).Key;
        Assert.True(fromArray.HasValue);
        Assert.Equal(0, fromArray.Value.Length);

        var fromMemory = OutboxMessage.FromBytes(ReadOnlyMemory<byte>.Empty, "p"u8.ToArray()).Key;
        Assert.True(fromMemory.HasValue);
        Assert.Equal(0, fromMemory.Value.Length);
    }

    [Fact]
    public void Null_key_is_null_and_empty_key_is_empty_bytes()
    {
        Assert.Null(OutboxMessage.Create(null, "p").Key);
        Assert.Null(OutboxMessage.Create(null, ReadOnlyMemory<byte>.Empty).Key);
        Assert.Null(OutboxMessage.Json(null, new Sample("a", 1), CoreTestJsonContext.Default.Sample).Key);
        Assert.Null(OutboxMessage.FromBytes(null, ReadOnlyMemory<byte>.Empty).Key);

        var empty = OutboxMessage.Create(string.Empty, "p").Key;
        Assert.True(empty.HasValue);
        Assert.Equal(0, empty.Value.Length);

        var emptyBytes = OutboxMessage.FromBytes(ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty).Key;
        Assert.True(emptyBytes.HasValue);
        Assert.Equal(0, emptyBytes.Value.Length);
    }

    [Fact]
    public void Empty_string_payload_is_empty_bytes()
    {
        Assert.Equal(0, OutboxMessage.Create("k", string.Empty).Payload.Length);
    }

    [Fact]
    public void Null_string_payload_throws_argument_null()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => OutboxMessage.Create("k", (string)null!));
        Assert.Equal("payload", ex.ParamName);
    }

    [Fact]
    public void Null_type_info_throws_argument_null()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => OutboxMessage.Json("k", new Sample("a", 1), null!));
        Assert.Equal("typeInfo", ex.ParamName);
    }

    [Fact]
    public void Headers_default_to_empty_and_keep_the_given_instance()
    {
        Assert.Same(OutboxHeaders.Empty, default(OutboxMessage).Headers);
        Assert.Same(OutboxHeaders.Empty, OutboxMessage.FromBytes(null, ReadOnlyMemory<byte>.Empty).Headers);
        Assert.Same(OutboxHeaders.Empty, OutboxMessage.Create("k", "p").Headers);

        var headers = OutboxHeaders.CreateBuilder().Add("a", "b").Build();
        Assert.Same(headers, OutboxMessage.FromBytes(null, ReadOnlyMemory<byte>.Empty, headers).Headers);
        Assert.Same(headers, OutboxMessage.FromBytes(ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, headers).Headers);
        Assert.Same(headers, OutboxMessage.Create("k", "p", headers).Headers);
        Assert.Same(headers, OutboxMessage.Create("k", ReadOnlyMemory<byte>.Empty, headers).Headers);
        Assert.Same(headers, OutboxMessage.Json("k", new Sample("a", 1), CoreTestJsonContext.Default.Sample, headers).Headers);
    }

    [Fact]
    public void Default_message_has_empty_payload_and_null_key()
    {
        var message = default(OutboxMessage);

        Assert.Equal(0, message.Payload.Length);
        Assert.Null(message.Key);
    }

    public static TheoryData<string, Func<OutboxMessage>> LoneSurrogateInputs => new()
    {
        { "key", () => OutboxMessage.Create(Secret + "\uD800", "p") },
        { "key", () => OutboxMessage.Create("\uDC00" + Secret, ReadOnlyMemory<byte>.Empty) },
        { "key", () => OutboxMessage.Json(Secret + "\uD800", new Sample("a", 1), CoreTestJsonContext.Default.Sample) },
        { "payload", () => OutboxMessage.Create("k", Secret + "\uDC00x") },
        { "payload", () => OutboxMessage.Create("k", "\uD800\uD800" + Secret) },
    };

    [Theory]
    [MemberData(nameof(LoneSurrogateInputs))]
    public void Lone_surrogate_throws_without_value_text_or_inner_exception(string paramName, Func<OutboxMessage> create)
    {
        var ex = Assert.Throws<ArgumentException>(() => create());

        Assert.Equal(paramName, ex.ParamName);
        Assert.DoesNotContain(Secret, ex.Message, StringComparison.Ordinal);
        Assert.Null(ex.InnerException);
    }

    [Fact]
    public void Route_is_null_for_every_factory_and_default()
    {
        Assert.Null(default(OutboxMessage).Route);
        Assert.Null(OutboxMessage.FromBytes(new byte[] { 1 }, "p"u8.ToArray()).Route);
        Assert.Null(OutboxMessage.FromBytes((ReadOnlyMemory<byte>?)new byte[] { 1 }, "p"u8.ToArray()).Route);
        Assert.Null(OutboxMessage.Create("k", ReadOnlyMemory<byte>.Empty).Route);
        Assert.Null(OutboxMessage.Create("k", "p").Route);
        Assert.Null(OutboxMessage.Json("k", new Sample("a", 1), CoreTestJsonContext.Default.Sample).Route);
    }

    [Fact]
    public void With_route_returns_a_copy_and_leaves_the_receiver_unchanged()
    {
        var headers = OutboxHeaders.CreateBuilder().Add("a", "b").Build();
        var buffer = new byte[] { 1, 2, 3, 4 };
        var key = new ReadOnlyMemory<byte>(buffer, 0, 1);
        var payload = new ReadOnlyMemory<byte>(buffer, 1, 3);
        var message = OutboxMessage.FromBytes(key, payload, headers);

        var routed = message.WithRoute("alerts");

        Assert.Equal("alerts", routed.Route);
        Assert.True(key.Equals(routed.Key!.Value));
        Assert.True(payload.Equals(routed.Payload));
        Assert.Same(headers, routed.Headers);
        Assert.Null(message.Route);
    }

    [Fact]
    public void With_route_keeps_a_null_key_and_empty_headers()
    {
        var routed = OutboxMessage.Create(null, "p").WithRoute("a");

        Assert.Null(routed.Key);
        Assert.Same(OutboxHeaders.Empty, routed.Headers);
    }

    [Fact]
    public void With_route_replaces_an_existing_route()
    {
        var first = OutboxMessage.Create("k", "p").WithRoute("first");

        var second = first.WithRoute("second");

        Assert.Equal("second", second.Route);
        Assert.Equal("first", first.Route);
    }

    [Fact]
    public void With_route_allocates_nothing_after_warm_up()
    {
        const int warmUp = 3;
        const int iterations = 1000;
        var message = OutboxMessage.Create("k", "p");
        var route = new string('a', 63);
        var sink = 0;

        for (var i = 0; i < warmUp; i++)
        {
            sink += message.WithRoute(route).Route!.Length;
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < iterations; i++)
        {
            sink += message.WithRoute(route).Route!.Length;
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        TestContext.Current.TestOutputHelper?.WriteLine($"WithRoute: {allocated} B over {iterations} calls");
        Assert.Equal((warmUp + iterations) * 63, sink);
        Assert.Equal(0, allocated);
    }

    [Fact]
    public void Surrogate_pair_is_encoded()
    {
        Assert.Equal(Encoding.UTF8.GetBytes("😀"), OutboxMessage.Create("😀", "p").Key!.Value.ToArray());
    }
}

public sealed record Sample(string Name, int Count);

[JsonSerializable(typeof(Sample))]
internal sealed partial class CoreTestJsonContext : JsonSerializerContext;
