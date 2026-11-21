using Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;
using Pernix.OutboxRelay.Client.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests;

public sealed class FlywaySnippetTests : IDisposable
{
    private readonly DirectoryInfo migrationsDir = Directory.CreateTempSubdirectory("outbox-flyway-");

    public void Dispose() => migrationsDir.Delete(recursive: true);

    [Fact]
    public void Rendered_files_pass_the_check()
    {
        SchemaSnippets.RenderFlyway(migrationsDir.FullName);

        var expected = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("sales.outbox"))
            .Select(script => SchemaSnippets.FlywayFiles[script.Version])
            .Order(StringComparer.Ordinal);
        Assert.Equal(expected, migrationsDir.GetFiles().Select(f => f.Name).Order(StringComparer.Ordinal));
        SchemaSnippets.CheckFlyway(migrationsDir.FullName);
    }

    [Fact]
    public void One_changed_byte_fails_the_check()
    {
        SchemaSnippets.RenderFlyway(migrationsDir.FullName);
        var file = Path.Combine(migrationsDir.FullName, SchemaSnippets.FlywayFiles[PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("sales.outbox"))[0].Version]);
        var bytes = File.ReadAllBytes(file);
        bytes[bytes.Length / 2] ^= 0x01;
        File.WriteAllBytes(file, bytes);

        var exception = Assert.Throws<InvalidOperationException>(() => SchemaSnippets.CheckFlyway(migrationsDir.FullName));

        Assert.StartsWith(Path.GetFileName(file), exception.Message, StringComparison.Ordinal);
    }
}
