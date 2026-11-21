using Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;
using Xunit;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests;

// A compiler change to conditional target-typing would flip the guide's frombytes-trap statement.
public sealed class FromBytesTargetTypingTests
{
    private static readonly ReadOnlyMemory<byte> Key = new byte[] { 1, 2 };
    private static readonly ReadOnlyMemory<byte> Payload = new byte[] { 3 };

    [Fact]
    public void Trap_form_writes_an_empty_key_not_null()
    {
        var message = MessageSnippets.FromBytesTrap(noKey: true, Key, Payload);

        Assert.True(message.Key.HasValue && message.Key.Value.IsEmpty);
    }

    [Fact]
    public void Cast_form_writes_a_null_key()
    {
        var message = MessageSnippets.FromBytesNullKey(noKey: true, Key, Payload);

        Assert.Null(message.Key);
    }

    [Fact]
    public void Both_forms_keep_a_present_key()
    {
        var trap = MessageSnippets.FromBytesTrap(noKey: false, Key, Payload);
        var cast = MessageSnippets.FromBytesNullKey(noKey: false, Key, Payload);

        Assert.Equal(Key.ToArray(), trap.Key!.Value.ToArray());
        Assert.Equal(Key.ToArray(), cast.Key!.Value.ToArray());
    }
}
