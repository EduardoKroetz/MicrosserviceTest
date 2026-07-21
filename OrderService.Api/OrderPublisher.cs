using RabbitMQ.Client;
using Shared.Contracts;
using System.Text.Json;

namespace OrderService.Api;

public class OrderPublisher
{
    private readonly IConnection _connection;
    private IChannel? _channel;

    private const string Exchange = "order.exchange";
    private const string RoutingKey = "order.created";

    public OrderPublisher(IConnection connection)
    {
        _connection = connection;
    }

    public async Task PublishOrderCreatedEventAsync(OrderCreatedEvent orderCreatedEvent, CancellationToken ct)
    {
        await PublishAsync(orderCreatedEvent, RoutingKey, ct);
    }

    private async Task PublishAsync(Event ev, string routingKey, CancellationToken ct)
    {
        _channel = await _connection.CreateChannelAsync(options: null, ct);

        await _channel.ExchangeDeclareAsync(
            exchange: Exchange,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            cancellationToken: ct);

        var body = JsonSerializer.SerializeToUtf8Bytes(ev, ev.GetType());

        var props = new BasicProperties
        {
            MessageId = ev.EventId.ToString(),
            ContentType = "application/json",
            DeliveryMode = DeliveryModes.Persistent,
            Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            Type = ev.GetType().Name
        };

        await _channel.BasicPublishAsync(
            exchange: Exchange,
            routingKey: routingKey,
            mandatory: true,
            basicProperties: props,
            body: body,
            cancellationToken: ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
            await _channel.DisposeAsync();
    }
}