using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Contracts;
using System.Text.Json;

namespace PaymentService.Worker;

public class OrderCreatedConsumer : BackgroundService
{
    private readonly IConnection _connection;
    private readonly ILogger<OrderCreatedConsumer> _logger;
    private readonly PaymentPublisher _paymentPublisher;
    private IChannel? _channel;

    private const string Exchange = "order.exchange";
    private const string Queue = "order.created.payments";

    public OrderCreatedConsumer(IConnection connection, ILogger<OrderCreatedConsumer> logger, PaymentPublisher paymentPublisher)
    {
        _connection = connection;
        _logger = logger;
        _paymentPublisher = paymentPublisher;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        _channel = await _connection.CreateChannelAsync(cancellationToken: ct);

        await _channel.ExchangeDeclareAsync(Exchange, ExchangeType.Topic, durable: true, autoDelete: false, cancellationToken: ct);

        await _channel.QueueDeclareAsync(Queue, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);

        await _channel.QueueBindAsync(Queue, Exchange, "order.created", cancellationToken: ct);

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.ReceivedAsync += OnMessageAsync;

        await _channel.BasicConsumeAsync(Queue, autoAck: false, consumer: consumer, cancellationToken: ct);
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
