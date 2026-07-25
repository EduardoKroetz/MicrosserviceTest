using BuildingBlocks;
using NotificationService.Worker.Handlers;
using RabbitMQ.Client.Events;
using Shared.Contracts;
using System.Text.Json;

namespace NotificationService.Worker;

public class PaymentApprovedConsumer : BackgroundService
{
    private readonly ILogger<PaymentApprovedConsumer> _logger;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly IMessageBusConnection _messageBusConnection;

    private const string Exchange = "payment.exchange";
    private const string Queue = "payment.approved.orders";
    private const string Event = "payment.approved";

    public PaymentApprovedConsumer(ILogger<PaymentApprovedConsumer> logger, IServiceScopeFactory serviceScopeFactory, IMessageBusConnection messageBusConnection)
    {
        _logger = logger;
        _serviceScopeFactory = serviceScopeFactory;
        _messageBusConnection = messageBusConnection;
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
            var ev = JsonSerializer.Deserialize<PaymentApprovedEvent>(ea.Body.ToArray());
            if (ev is null || string.IsNullOrEmpty(messageId))
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                return;
            }

            using var scope = _serviceScopeFactory.CreateScope();
            var paymentApprovedHandler = scope.ServiceProvider.GetRequiredService<PaymentApprovedHandler>();
            await paymentApprovedHandler.HandleAsync(ev);

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        }
        catch (JsonException)
        {
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha ao processar {MessageId}", messageId);
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
        }
    }
}