namespace Shared.Contracts;

public record OrderCreatedEvent(Guid OrderId, decimal TotalAmount) : Event;

public record PaymentApprovedEvent(Guid OrderId) : Event;
public record PaymentRejectedEvent(Guid OrderId, string Reason) : Event;

public record Event
{
    public Guid EventId { get; init; } = Guid.NewGuid();
}