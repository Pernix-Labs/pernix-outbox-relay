namespace Pernix.OutboxRelay.Client.PostgreSql;

/// <summary>
/// One outbox DDL script, rendered for a table and a name prefix by <see cref="PostgreSqlOutboxSchema.Scripts"/>.
/// Apply the scripts in <see cref="Version"/> order with the producer's own migration tool.
/// </summary>
/// <param name="Version">The script number, <c>1</c> for <c>V001</c>.</param>
/// <param name="Name">The script name without extension, for example <c>V001__create_outbox</c>.</param>
/// <param name="Sql">The rendered SQL text, comments stripped.</param>
public sealed record OutboxSchemaScript(int Version, string Name, string Sql);
