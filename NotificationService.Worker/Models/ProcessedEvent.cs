namespace NotificationService.Worker.Models;

public class ProcessedEvent
{
    public required Guid EventId { get; set; }
    public required string Name { get; set; }
    public required DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}