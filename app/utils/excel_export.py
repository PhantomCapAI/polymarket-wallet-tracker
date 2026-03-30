import io
import logging
from typing import List, Any

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.worksheet.worksheet import Worksheet

from app.models.database import db

logger = logging.getLogger(__name__)

# Table name -> ordered list of display headers
# (header text, SQL column name)
TABLE_SCHEMAS = {
    "wallets_master": [
        ("Wallet", "wallet"),
        ("Signal Score", "signal_score"),
        ("Realized PnL", "realized_pnl"),
        ("Win Rate", "win_rate"),
        ("Avg Position Size", "avg_position_size"),
        ("Market Diversity", "market_diversity"),
        ("Timing Edge", "timing_edge"),
        ("Closing Efficiency", "closing_efficiency"),
        ("Consistency Score", "consistency_score"),
        ("Total Trades", "total_trades"),
        ("Active Days", "active_days"),
        ("Last Trade At", "last_trade_at"),
        ("Last Updated", "last_updated"),
    ],
    "trades_log": [
        ("ID", "id"),
        ("Wallet", "wallet"),
        ("Market", "market"),
        ("Direction", "direction"),
        ("Entry Price", "entry_price"),
        ("Position Size", "position_size"),
        ("Exit Price", "exit_price"),
        ("Peak Price", "peak_price"),
        ("PnL", "pnl"),
        ("Outcome", "outcome"),
        ("Entry Time", "entry_time"),
        ("Exit Time", "exit_time"),
        ("Created At", "created_at"),
    ],
    "market_summary": [
        ("Market", "market"),
        ("Total Volume", "total_volume"),
        ("Avg Win Rate", "avg_win_rate"),
        ("Top Wallet", "top_wallet"),
        ("Volatility", "volatility"),
        ("Trend Bias", "trend_bias"),
        ("Smart Money Count", "smart_money_count"),
        ("Last Updated", "last_updated"),
    ],
    "alerts_log": [
        ("ID", "id"),
        ("Wallet", "wallet"),
        ("Event Type", "event_type"),
        ("Confidence", "confidence"),
        ("Signal Reason", "signal_reason"),
        ("Market", "market"),
        ("Timestamp", "timestamp"),
        ("Processed", "processed"),
    ],
    "copy_trades": [
        ("ID", "id"),
        ("Source Wallet", "source_wallet"),
        ("Market", "market"),
        ("Direction", "direction"),
        ("Entry Price", "entry_price"),
        ("Exit Price", "exit_price"),
        ("Position Size", "position_size"),
        ("Signal Score", "signal_score"),
        ("Status", "status"),
        ("PnL", "pnl"),
        ("Stop Loss Price", "stop_loss_price"),
        ("Created At", "created_at"),
        ("Closed At", "closed_at"),
    ],
    "fees_collected": [
        ("ID", "id"),
        ("Trade ID", "trade_id"),
        ("Gross PnL", "gross_pnl"),
        ("Fee %", "fee_pct"),
        ("Fee Amount", "fee_amount"),
        ("Net PnL", "net_pnl"),
        ("Treasury Wallet", "treasury_wallet"),
        ("Collected At", "collected_at"),
    ],
}

HEADER_FONT = Font(bold=True, color="FFFFFF")
HEADER_FILL = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
HEADER_ALIGN = Alignment(horizontal="center")


class ExcelExportService:
    async def export_all_tables(self) -> bytes:
        """
        Export all 5 database tables to a single .xlsx file.
        Returns the workbook as raw bytes suitable for an HTTP response.
        """
        wb = Workbook()
        # Remove the default blank sheet created by openpyxl
        wb.remove(wb.active)

        for table_name, columns in TABLE_SCHEMAS.items():
            sheet_title = table_name.replace("_", " ").title()
            # Sheet names are limited to 31 chars
            ws = wb.create_sheet(title=sheet_title[:31])

            col_headers = [c[0] for c in columns]
            col_names = [c[1] for c in columns]

            # Fetch all rows
            sql = f"SELECT {', '.join(col_names)} FROM {table_name}"
            try:
                rows = await db.fetch(sql)
            except Exception as e:
                logger.error(f"Error querying {table_name}: {e}")
                rows = []

            # Write header row
            for col_idx, header in enumerate(col_headers, start=1):
                cell = ws.cell(row=1, column=col_idx, value=header)
                cell.font = HEADER_FONT
                cell.fill = HEADER_FILL
                cell.alignment = HEADER_ALIGN

            # Write data rows
            for row_idx, record in enumerate(rows, start=2):
                for col_idx, col_name in enumerate(col_names, start=1):
                    value = record[col_name]
                    # Convert Decimal / other non-serialisable types
                    if value is not None and hasattr(value, '__float__'):
                        try:
                            value = float(value)
                        except (TypeError, ValueError):
                            value = str(value)
                    ws.cell(row=row_idx, column=col_idx, value=value)

            # Auto-adjust column widths
            self._auto_adjust_columns(ws)

            logger.info(f"Exported {len(rows)} rows from {table_name}")

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        return buf.getvalue()

    @staticmethod
    def _auto_adjust_columns(ws: Worksheet):
        """Set each column width to fit the longest cell value (capped at 50)."""
        for column_cells in ws.columns:
            max_len = 0
            col_letter = column_cells[0].column_letter
            for cell in column_cells:
                try:
                    cell_len = len(str(cell.value))
                    if cell_len > max_len:
                        max_len = cell_len
                except (TypeError, AttributeError):
                    pass
            ws.column_dimensions[col_letter].width = min(max_len + 2, 50)


# Global instance
excel_export_service = ExcelExportService()
