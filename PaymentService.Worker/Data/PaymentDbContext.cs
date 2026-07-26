using Microsoft.EntityFrameworkCore;
using PaymentService.Worker.Models;

namespace PaymentService.Worker.Data;

public class PaymentDbContext : DbContext
{
    public PaymentDbContext(DbContextOptions<PaymentDbContext> options)
        : base(options)
    {
    }

    public DbSet<ProcessedEvent> ProcessedEvents { get; set; }
    public DbSet<OutboxMessage> OutboxMessages { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<ProcessedEvent>(builder =>
        {
            builder.HasKey(e => e.EventId);
        });

        modelBuilder.Entity<OutboxMessage>(builder =>
        {
            builder.HasKey(e => e.EventId);
            builder.Property(e => e.Type).IsRequired();
            builder.Property(e => e.Content).IsRequired();
        });
    }
}
