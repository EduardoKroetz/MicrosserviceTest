using Shared.Contracts;

namespace BuildingBlocks;

public interface IMessageBusConnection : IAsyncDisposable
{
    Task PublishAsync(Event ev, string routingKey, string exchange, CancellationToken ct);
}
