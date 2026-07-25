using BuildingBlocks;
using RabbitMQ.Client.Events;
using Shared.Contracts;
using System.Text.Json;

namespace PaymentService.Worker;

public class OrderCreatedConsumer : BackgroundService
{
    private readonly ILogger<OrderCreatedConsumer> _logger;
    private readonly PaymentPublisher _paymentPublisher;
    private readonly IMessageBusConnection _messageBusConnection;

    private const string Exchange = "order.exchange";
    private const string Queue = "order.created.payments";
    private const string Event = "order.created";

    public OrderCreatedConsumer(ILogger<OrderCreatedConsumer> logger, PaymentPublisher paymentPublisher, IMessageBusConnection messageBusConnection)
    {
        _logger = logger;
        _paymentPublisher = paymentPublisher;
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
            var ev = JsonSerializer.Deserialize<OrderCreatedEvent>(ea.Body.ToArray());
            if (ev is null || string.IsNullOrEmpty(messageId))
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                return;
            }

            const int minTotalAmount = 100;

            await Task.Delay(3000);

            if (ev.TotalAmount < minTotalAmount) // Exemplo de validação fictícia: rejeitar pagamentos com valor total menor que 100
            {
                var paymentRejectedEvent = new PaymentRejectedEvent(ev.OrderId, $"The total value must be greater than or equal to {minTotalAmount}.");
                await _paymentPublisher.PublishPaymentRejectedEventAsync(paymentRejectedEvent, CancellationToken.None);
            }
            else
            {
                var paymentApprovedEvent = new PaymentApprovedEvent(ev.OrderId);
                await _paymentPublisher.PublishPaymentApprovedEventAsync(paymentApprovedEvent, CancellationToken.None);
            }

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
