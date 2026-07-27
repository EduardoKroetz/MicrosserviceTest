using Microsoft.EntityFrameworkCore;
using NotificationService.Worker.Models;

namespace NotificationService.Worker.Data;

public class NotificationDbContext : DbContext
{
    public NotificationDbContext(DbContextOptions<NotificationDbContext> options)
        : base(options)
    {
    }

    public DbSet<ProcessedEvent> ProcessedEvents { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<ProcessedEvent>(entity =>
        {
            entity.HasKey(e => e.EventId);
            entity.Property(e => e.Name).IsRequired();
            entity.Property(e => e.CreatedAt).IsRequired();
        });
    }
}
