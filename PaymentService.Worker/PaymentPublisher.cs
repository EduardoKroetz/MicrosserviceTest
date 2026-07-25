using BuildingBlocks;
using Shared.Contracts;

namespace PaymentService.Worker;

public class PaymentPublisher
{
    private readonly IMessageBusConnection _messageBusConnection;

    private const string Exchange = "payment.exchange";

    public PaymentPublisher(IMessageBusConnection messageBusConnection)
    {
        _messageBusConnection = messageBusConnection;
    }

    public async Task PublishPaymentApprovedEventAsync(PaymentApprovedEvent ev, CancellationToken ct)
    {
        await _messageBusConnection.PublishAsync(ev, "payment.approved", Exchange, ct);
    }

    public async Task PublishPaymentRejectedEventAsync(PaymentRejectedEvent ev, CancellationToken ct)
    {
        await _messageBusConnection.PublishAsync(ev, "payment.rejected", Exchange, ct);
    }
}