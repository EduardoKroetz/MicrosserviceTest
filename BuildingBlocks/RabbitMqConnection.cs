using RabbitMQ.Client;
using Shared.Contracts;
using System.Text.Json;

namespace BuildingBlocks;

public class RabbitMqConnection : IMessageBusConnection
{
    private readonly IConnection _connection;
    private IChannel? _channel;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public RabbitMqConnection(IConnection connection)
    {
        _connection = connection;
    }

    private async Task<IChannel> GetChannelAsync(string exchange, CancellationToken ct)
    {
        // 1º check: barato, sem trava — o caminho comum não paga nada
        if (_channel is { IsOpen: true })
            return _channel;

        await _gate.WaitAsync(ct);
        try
        {
            // 2º check: dentro da trava. Se A criou enquanto B esperava, B reusa.
            if (_channel is { IsOpen: true })
                return _channel;

            var channel = await _connection.CreateChannelAsync(
                new CreateChannelOptions(
                    publisherConfirmationsEnabled: true,
                    publisherConfirmationTrackingEnabled: true),
                ct);

            _channel = channel;
            return _channel;
        }
        finally
        {
            _gate.Release();
        }
    }

    public async Task PublishAsync(Event ev, string routingKey, string exchange, CancellationToken ct)
    {
        var channel = await GetChannelAsync(exchange, ct);

        await channel.ExchangeDeclareAsync(
            exchange: exchange,
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

        await channel.BasicPublishAsync(
            exchange: exchange,
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
        _gate.Dispose();
    }
}
