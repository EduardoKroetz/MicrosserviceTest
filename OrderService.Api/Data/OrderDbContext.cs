using Microsoft.EntityFrameworkCore;
using OrderService.Api.Models;

namespace OrderService.Api.Data;

public class OrderDbContext : DbContext
{
    public OrderDbContext(DbContextOptions<OrderDbContext> options)
        : base(options)
    {
    }

    public DbSet<Order> Orders { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.OrderDate).IsRequired();
            entity.Property(e => e.TotalAmount).IsRequired().HasColumnType("decimal(18,2)");
            entity.Property(e => e.Status).IsRequired().HasConversion<string>();
        });
    }
}
