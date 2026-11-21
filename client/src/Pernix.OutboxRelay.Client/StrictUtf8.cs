using System.Text;

namespace Pernix.OutboxRelay.Client;

// UTF-8 encoding that rejects lone surrogates instead of writing U+FFFD, so the client writes exactly what it is given.
internal static class StrictUtf8
{
    private static readonly UTF8Encoding Encoding = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    // Exception text names the parameter only, never the value. No inner exception: the encoder's carries the code unit.
    public static byte[] Encode(string value, string paramName)
    {
        if (value.Length == 0)
        {
            return [];
        }

        try
        {
            return Encoding.GetBytes(value);
        }
        catch (EncoderFallbackException)
        {
            throw new ArgumentException("Value contains a lone surrogate and has no UTF-8 form.", paramName);
        }
    }

    public static bool HasLoneSurrogate(ReadOnlySpan<char> text)
    {
        var i = text.IndexOfAnyInRange('\uD800', '\uDFFF');
        if (i < 0)
        {
            return false;
        }

        for (; i < text.Length; i++)
        {
            var c = text[i];
            if (char.IsHighSurrogate(c))
            {
                if (i + 1 >= text.Length || !char.IsLowSurrogate(text[i + 1]))
                {
                    return true;
                }

                i++;
            }
            else if (char.IsLowSurrogate(c))
            {
                return true;
            }
        }

        return false;
    }
}
