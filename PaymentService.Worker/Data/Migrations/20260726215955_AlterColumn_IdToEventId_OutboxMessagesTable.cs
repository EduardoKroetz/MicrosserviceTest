using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace PaymentService.Worker.Migrations
{
    /// <inheritdoc />
    public partial class AlterColumn_IdToEventId_OutboxMessagesTable : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameColumn(
                name: "Id",
                table: "ProcessedEvents",
                newName: "EventId");

            migrationBuilder.RenameColumn(
                name: "Id",
                table: "OutboxMessages",
                newName: "EventId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameColumn(
                name: "EventId",
                table: "ProcessedEvents",
                newName: "Id");

            migrationBuilder.RenameColumn(
                name: "EventId",
                table: "OutboxMessages",
                newName: "Id");
        }
    }
}
