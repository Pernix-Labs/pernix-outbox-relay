namespace Pernix.OutboxRelay.Integration.Tests.Kafka;

// Port of Kafka org.apache.kafka.common.utils.Utils.murmur2 and the Java default partitioner for keyed records.
internal static class JavaMurmur2
{
    public static int Partition(byte[] key, int partitions) => ToPositive(Hash(key)) % partitions;

    public static int Hash(byte[] data)
    {
        unchecked
        {
            var length = data.Length;
            const uint seed = 0x9747b28c;
            const uint m = 0x5bd1e995;
            const int r = 24;

            var h = seed ^ (uint)length;
            var length4 = length / 4;
            for (var i = 0; i < length4; i++)
            {
                var i4 = i * 4;
                var k = data[i4] + ((uint)data[i4 + 1] << 8) + ((uint)data[i4 + 2] << 16) + ((uint)data[i4 + 3] << 24);
                k *= m;
                k ^= k >> r;
                k *= m;
                h *= m;
                h ^= k;
            }

            var tail = length & ~3;
            switch (length % 4)
            {
                case 3:
                    h ^= (uint)data[tail + 2] << 16;
                    goto case 2;
                case 2:
                    h ^= (uint)data[tail + 1] << 8;
                    goto case 1;
                case 1:
                    h ^= data[tail];
                    h *= m;
                    break;
            }

            h ^= h >> 13;
            h *= m;
            h ^= h >> 15;
            return (int)h;
        }
    }

    private static int ToPositive(int number) => number & 0x7fffffff;
}
