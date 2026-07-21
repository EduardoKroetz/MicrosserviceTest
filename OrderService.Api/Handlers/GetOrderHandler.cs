using OrderService.Api.Data;
using OrderService.Api.Models;

namespace OrderService.Api.Handlers;

public class GetOrderHandler(OrderDbContext dbContext)
{
    public async Task<Order> HandleAsync(Guid id)
    {
        var order = await dbContext.Orders.FindAsync(id)
          ?? throw new InvalidOperationException("Order not found");

        return order;
    }
}
