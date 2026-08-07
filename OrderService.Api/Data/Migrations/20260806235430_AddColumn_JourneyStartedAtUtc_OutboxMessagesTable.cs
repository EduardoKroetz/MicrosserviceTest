using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace OrderService.Api.Data.Migrations
{
    /// <inheritdoc />
    public partial class AddColumn_JourneyStartedAtUtc_OutboxMessagesTable : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<DateTime>(
                name: "JourneyStartedAtUtc",
                table: "OutboxMessages",
                type: "timestamp with time zone",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "JourneyStartedAtUtc",
                table: "OutboxMessages");
        }
    }
}
