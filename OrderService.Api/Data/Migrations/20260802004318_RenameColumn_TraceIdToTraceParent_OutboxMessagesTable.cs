using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace OrderService.Api.Data.Migrations
{
    /// <inheritdoc />
    public partial class RenameColumn_TraceIdToTraceParent_OutboxMessagesTable : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameColumn(
                name: "TraceId",
                table: "OutboxMessages",
                newName: "TraceParent");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.RenameColumn(
                name: "TraceParent",
                table: "OutboxMessages",
                newName: "TraceId");
        }
    }
}
