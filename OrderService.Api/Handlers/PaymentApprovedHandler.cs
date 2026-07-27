using Microsoft.EntityFrameworkCore;
using Npgsql;
using OrderService.Api.Data;
using OrderService.Api.Models;
using Shared.Contracts;

namespace OrderService.Api.Handlers;

public class PaymentApprovedHandler(OrderDbContext dbContext, ILogger<PaymentApprovedHandler> logger)
{
    public async Task HandleAsync(PaymentApprovedEvent ev, CancellationToken ct)
    {
        // Validação de idempotência: verifica se o evento já foi processado
        if (await dbContext.ProcessedEvents.AnyAsync(e => e.EventId == ev.EventId, ct))
        {
            logger.LogInformation("Event {EventId} already processed. Skipping.", ev.EventId);
            return;
        }

        var transaction = await dbContext.Database.BeginTransactionAsync(ct);

        try
        {
            var order = await dbContext.Orders.FindAsync(ev.OrderId)
                ?? throw new InvalidOperationException("Order not found");

            order.Status = OrderStatus.Confirmed;

            // Adiciona o registro do evento processado para garantir idempotência
            var processedEvent = new ProcessedEvent
            {
                EventId = ev.EventId,
                Name = nameof(OrderCreatedEvent),
                CreatedAt = DateTime.UtcNow
            };

            dbContext.ProcessedEvents.Add(processedEvent);
            await dbContext.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);

            logger.LogInformation("Event {EventId} processed successfully.", ev.EventId);
        }
        catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: "23505" }) // 23505 é o código de erro do PostgreSQL para violação de chave primária
        {
            logger.LogError("Event with ID {EventId} already processed.", ev.EventId);
            await transaction.RollbackAsync(ct);
            return;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing event with ID {EventId}.", ev.EventId);
            await transaction.RollbackAsync(ct);
            throw;
        }

    }
}
