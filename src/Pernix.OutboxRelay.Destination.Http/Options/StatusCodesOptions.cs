namespace Pernix.OutboxRelay.Destination.Http.Options;

internal sealed class StatusCodesOptions
{
    public string[]? Delivered { get; set; }
    public string[]? Rejected { get; set; }
    public string[]? Retried { get; set; }
    public string[]? Paused { get; set; }
}
