using OrderService.Api.Data;
using OrderService.Api.Models;
using Shared.Contracts;

namespace OrderService.Api.Handlers;

public class PaymentRejectedHandler(OrderDbContext dbContext)
{
    public async Task HandleAsync(PaymentRejectedEvent ev, CancellationToken ct)
    {
        // TODO: Implementar idempotencia

        var order = await dbContext.Orders.FindAsync(ev.OrderId)
            ?? throw new InvalidOperationException("Order not found");

        order.Status = OrderStatus.Cancelled;

        await dbContext.SaveChangesAsync(ct); // E se der erro no save changes? Oq acontece?
    }
}
