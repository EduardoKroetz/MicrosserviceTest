using BuildingBlocks;
using Shared.Contracts;

namespace OrderService.Api;

public class OrderPublisher
{
    private readonly IMessageBusConnection _messageBusConnection;

    public OrderPublisher(IMessageBusConnection connection)
    {
        _messageBusConnection = connection;
    }

    private const string Exchange = "order.exchange";
    private const string RoutingKey = "order.created";

    public async Task PublishOrderCreatedEventAsync(OrderCreatedEvent orderCreatedEvent, CancellationToken ct)
    {
        await _messageBusConnection.PublishAsync(orderCreatedEvent, RoutingKey, Exchange, ct);
    }
}