using Microsoft.EntityFrameworkCore;
using Npgsql;
using PaymentService.Worker.Data;
using PaymentService.Worker.Models;
using Shared.Contracts;
using System.Diagnostics;
using System.Text.Json;

namespace PaymentService.Worker.Handlers;

public class OrderCreatedHandler
{
    private readonly PaymentDbContext _dbContext;
    private readonly ILogger<OrderCreatedHandler> _logger;

    public OrderCreatedHandler(PaymentDbContext dbContext, ILogger<OrderCreatedHandler> logger)
    {
        _dbContext = dbContext;
        _logger = logger;
    }

    public async Task HandleAsync(OrderCreatedEvent ev, DateTime? journeyStartedAtUtc = null)
    {
        using var activity = Telemetry.ActivitySource.StartActivity("Handle OrderCreatedEvent", ActivityKind.Internal);
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Validação de idempotência: verifica se o evento já foi processado
            var eventAlreadyProcessed = await _dbContext.ProcessedEvents.AnyAsync(e => e.EventId == ev.EventId);
            if (eventAlreadyProcessed)
            {
                _logger.LogInformation("Event with ID {EventId} has already been processed. Skipping.", ev.EventId);
                return;
            }

            using var transaction = await _dbContext.Database.BeginTransactionAsync();

            try
            {
                // Simula um atraso no processamento do pagamento (por exemplo, para representar a comunicação com um gateway de pagamento)
                var delay = new Random().Next(500, 3000);
                await Task.Delay(delay);

                Event? outboxEvent = null;

                // Simula aprovação ou rejeição aleatória do pagamento
                var isApproved = new Random().Next(0, 2) == 0;
                if (!isApproved)
                {
                    var paymentRejectedEvent = new PaymentRejectedEvent(ev.OrderId, $"Payment rejected.");
                    outboxEvent = paymentRejectedEvent;
                }
                else
                {
                    var paymentApprovedEvent = new PaymentApprovedEvent(ev.OrderId);
                    outboxEvent = paymentApprovedEvent;
                }

                // Adiciona evento ao Outbox para ser processado posteriormente
                var outboxMessage = new OutboxMessage
                {
                    EventId = outboxEvent.EventId,
                    Type = outboxEvent.GetType().Name,
                    Content = JsonSerializer.Serialize(outboxEvent, outboxEvent.GetType()),
                    OccurredOnUtc = DateTime.UtcNow,
                    TraceParent = activity?.Id,
                    JourneyStartedAtUtc = journeyStartedAtUtc
                };

                _dbContext.OutboxMessages.Add(outboxMessage);

                // Adiciona o registro do evento processado para garantir idempotência
                var eventRecord = new ProcessedEvent
                {
                    EventId = ev.EventId,
                    Name = nameof(OrderCreatedEvent),
                    CreatedAt = DateTime.UtcNow
                };

                _dbContext.ProcessedEvents.Add(eventRecord);
                await _dbContext.SaveChangesAsync();

                await transaction.CommitAsync();

                _logger.LogInformation("Event with ID {EventId} processed successfully.", ev.EventId);

                Telemetry.PaymentsProcessed.Add(1, new KeyValuePair<string, object?>("result", isApproved ? "approved" : "rejected"));
            }
            catch (DbUpdateException ex) when (ex.InnerException is PostgresException { SqlState: "23505" }) // 23505 é o código de erro do PostgreSQL para violação de chave primária
            {
                _logger.LogError("Event with ID {EventId} already processed.", ev.EventId);
                activity?.SetStatus(ActivityStatusCode.Unset);
                await transaction.RollbackAsync();
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing event with ID {EventId}.", ev.EventId);
                activity?.SetStatus(ActivityStatusCode.Error);
                await transaction.RollbackAsync();

                throw;
            }
        }
        finally
        {
            stopwatch.Stop();
            Telemetry.OrderCreatedHandlerDuration.Record(stopwatch.Elapsed.TotalSeconds);
        }
    }
}