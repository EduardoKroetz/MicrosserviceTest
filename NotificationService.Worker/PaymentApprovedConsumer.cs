using BuildingBlocks;
using NotificationService.Worker.Handlers;
using RabbitMQ.Client.Events;
using Shared.Contracts;
using System.Diagnostics;
using System.Text.Json;

namespace NotificationService.Worker;

public class PaymentApprovedConsumer : BackgroundService
{
    private readonly ILogger<PaymentApprovedConsumer> _logger;
    private readonly IServiceScopeFactory _serviceScopeFactory;
    private readonly IMessageBusConnection _messageBusConnection;

    private const string Exchange = "payment.exchange";
    private const string Queue = "payment.approved.notifications";
    private const string Event = "payment.approved";

    private static readonly ActivitySource ActivitySource = new("NotificationService.PaymentApprovedConsumer");

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

        using var activity = ea.CreateActivityFromEventArgs("Consume PaymentApprovedEvent", ActivitySource);

        try
        {
            var ev = ea.DeserializeEvent<PaymentApprovedEvent>();
            if (ev is null || string.IsNullOrEmpty(messageId))
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                return;
            }

            using var scope = _serviceScopeFactory.CreateScope();
            var paymentApprovedHandler = scope.ServiceProvider.GetRequiredService<PaymentApprovedHandler>();
            await paymentApprovedHandler.HandleAsync(ev, ea.CancellationToken);

            await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
        }
        catch (JsonException ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha ao processar {MessageId}", messageId);
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            await channel.BasicNackAsync(ea.DeliveryTag, false, requeue: true);
        }
    }
}