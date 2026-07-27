using BuildingBlocks;
using NotificationService.Worker.Handlers;
using RabbitMQ.Client.Events;
using Shared.Contracts;
using System.Text.Json;

namespace NotificationService.Worker;

public class PaymentRejectedConsumer : BackgroundService
{
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly ILogger<PaymentRejectedConsumer> _logger;
    private readonly IMessageBusConnection _messageBusConnection;

    private const string Exchange = "payment.exchange";
    private const string Queue = "payment.rejected.notifications";
    private const string Event = "payment.rejected";

    public PaymentRejectedConsumer(ILogger<PaymentRejectedConsumer> logger, IServiceScopeFactory serviceScopeFactory, IMessageBusConnection messageBusConnection)
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
            var ev = JsonSerializer.Deserialize<PaymentRejectedEvent>(ea.Body.ToArray());
            if (ev is null || string.IsNullOrEmpty(messageId))
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                return;
            }

            using var scope = _serviceScopeFactory.CreateScope();
            var paymentRejectedHandler = scope.ServiceProvider.GetRequiredService<PaymentRejectedHandler>();
            await paymentRejectedHandler.HandleAsync(ev, ea.CancellationToken);

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