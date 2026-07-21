using OrderService.Api.Data;
using OrderService.Api.DTOs;
using OrderService.Api.Models;
using Shared.Contracts;

namespace OrderService.Api.Handlers;

public class CreateOrderHandler(OrderDbContext dbContext, OrderPublisher orderPublisher)
{
    public async Task<Order> HandleAsync(CreateOrderRequest request)
    {
        var order = new Order
        {
            Id = Guid.NewGuid(),
            Status = OrderStatus.Pending,
            TotalAmount = request.TotalAmount,
            OrderDate = DateTime.UtcNow
        };

        await dbContext.Orders.AddAsync(order);
        await dbContext.SaveChangesAsync();

        var orderCreatedEvent = new OrderCreatedEvent(order.Id, order.TotalAmount);
        await orderPublisher.PublishOrderCreatedEventAsync(orderCreatedEvent, CancellationToken.None);

        return order;
    }
}
