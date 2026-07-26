using OrderService.Api.Data;
using OrderService.Api.DTOs;
using OrderService.Api.Models;
using Shared.Contracts;
using System.Text.Json;

namespace OrderService.Api.Handlers;

public class CreateOrderHandler(OrderDbContext dbContext)
{
    public async Task<Order> HandleAsync(CreateOrderRequest request)
    {
        using var transaction = await dbContext.Database.BeginTransactionAsync();

        try
        {
            var order = new Order
            {
                Id = Guid.NewGuid(),
                Status = OrderStatus.Pending,
                TotalAmount = request.TotalAmount,
                OrderDate = DateTime.UtcNow
            };

            await dbContext.Orders.AddAsync(order);

            var orderCreatedEvent = new OrderCreatedEvent(order.Id, order.TotalAmount);

            // Adiciona evento ao Outbox para ser processado posteriormente
            var outboxMessage = new OutboxMessage
            {
                EventId = orderCreatedEvent.EventId,
                Type = orderCreatedEvent.GetType().Name,
                Content = JsonSerializer.Serialize(orderCreatedEvent),
                OccurredOnUtc = DateTime.UtcNow
            };

            dbContext.OutboxMessages.Add(outboxMessage);

            await dbContext.SaveChangesAsync();

            await transaction.CommitAsync();

            return order;
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }
}
