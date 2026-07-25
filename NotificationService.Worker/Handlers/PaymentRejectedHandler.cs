using Shared.Contracts;

namespace NotificationService.Worker.Handlers;

public class PaymentRejectedHandler
{
    public async Task HandleAsync(PaymentRejectedEvent ev)
    {
        var message = $"O pagamento da ordem {ev.OrderId} foi rejeitado. Motivo: {ev.Reason}.";

        Console.WriteLine(message);
    }
}
