using RabbitMQ.Client.Events;
using Shared.Contracts;

namespace BuildingBlocks;

public interface IMessageBusConnection : IAsyncDisposable
{
    Task PublishAsync(Event ev, string routingKey, string exchange, CancellationToken ct);
    Task ConsumeAsync(AsyncEventHandler<BasicDeliverEventArgs> eventHandler, string exchange, string queue, string routingKey, CancellationToken ct);
}
