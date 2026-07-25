using Shared.Contracts;

namespace NotificationService.Worker.Handlers;

public class PaymentApprovedHandler
{
    public async Task HandleAsync(PaymentApprovedEvent ev)
    {
        var message = $"Seu pagamento foi aprovado e sua ordem {ev.OrderId} foi concluída.";

        Console.WriteLine(message);
    }
}
