using BuildingBlocks;
using PaymentService.Worker.Handlers;
using RabbitMQ.Client.Events;
using Shared.Contracts;
using System.Text.Json;

namespace PaymentService.Worker;

public class OrderCreatedConsumer : BackgroundService
{
    private readonly ILogger<OrderCreatedConsumer> _logger;
    private readonly IMessageBusConnection _messageBusConnection;
    private readonly IServiceScopeFactory _scopeFactory;

    private const string Exchange = "order.exchange";
    private const string Queue = "order.created.payments";
    private const string Event = "order.created";

    public OrderCreatedConsumer(ILogger<OrderCreatedConsumer> logger, IMessageBusConnection messageBusConnection, IServiceScopeFactory scopeFactory)
    {
        _logger = logger;
        _messageBusConnection = messageBusConnection;
        _scopeFactory = scopeFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await _messageBusConnection.ConsumeAsync(
            eventHandler: OnMessageAsync,
            exchange: Exchange,
            queue: Queue,
            routingKey: Event,
            ct: ct
        );
    }

    private async Task OnMessageAsync(object sender, BasicDeliverEventArgs ea)
    {
        var channel = ((AsyncEventingBasicConsumer)sender).Channel;
        var messageId = ea.BasicProperties.MessageId;

        try
        {
            var ev = JsonSerializer.Deserialize<OrderCreatedEvent>(ea.Body.ToArray());
            if (ev is null || string.IsNullOrEmpty(messageId))
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                return;
            }

            using var scope = _scopeFactory.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService<OrderCreatedHandler>();
            await handler.HandleAsync(ev);

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        }
        catch (JsonException)
        {
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha ao processar {MessageId}", messageId);
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: true);
        }
    }
}
