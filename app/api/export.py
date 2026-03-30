from fastapi import APIRouter, HTTPException, Query, Response
from typing import Optional
from datetime import datetime, timedelta
import csv
import json
import io
import logging

from app.models.database import db

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/trades/csv")
async def export_trades_csv(
    days: int = Query(30, ge=1, le=365),
    wallet: Optional[str] = None,
    market: Optional[str] = None,
    min_signal_score: float = Query(0.0, ge=0, le=1)
):
    """Export trades to CSV format"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Build query based on filters
        filters = ["t.entry_time >= $1", "(w.signal_score IS NULL OR w.signal_score >= $2)"]
        params = [cutoff_date, min_signal_score]
        param_count = 2
        
        if wallet:
            param_count += 1
            filters.append(f"t.wallet = ${param_count}")
            params.append(wallet)
        
        if market:
            param_count += 1
            filters.append(f"t.market = ${param_count}")
            params.append(market)
        
        where_clause = " AND ".join(filters)
        
        query = f"""
            SELECT 
                t.wallet,
                t.market,
                t.direction,
                t.entry_price,
                t.position_size,
                t.exit_price,
                t.pnl,
                t.entry_time,
                t.exit_time,
                w.signal_score,
                w.win_rate
            FROM trades_log t
            LEFT JOIN wallets_master w ON t.wallet = w.wallet
            WHERE {where_clause}
            ORDER BY t.entry_time DESC
        """
        
        trades = await db.fetch(query, *params)
        
        # Create CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow([
            'Wallet', 'Market', 'Direction', 'Entry Price', 'Position Size',
            'Exit Price', 'PnL', 'Entry Time', 'Exit Time', 'Signal Score', 'Win Rate'
        ])
        
        # Write data
        for trade in trades:
            writer.writerow([
                trade['wallet'],
                trade['market'],
                trade['direction'],
                trade['entry_price'],
                trade['position_size'],
                trade['exit_price'],
                trade['pnl'],
                trade['entry_time'],
                trade['exit_time'],
                trade['signal_score'],
                trade['win_rate']
            ])
        
        # Prepare response
        csv_content = output.getvalue()
        output.close()
        
        filename = f"trades_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        logger.error(f"Error exporting trades to CSV: {e}")
        raise HTTPException(status_code=500, detail="Failed to export trades")

@router.get("/wallets/csv")
async def export_wallets_csv(
    min_score: float = Query(0.5, ge=0, le=1),
    min_trades: int = Query(10, ge=1)
):
    """Export wallet scores to CSV format"""
    try:
        wallets = await db.fetch("""
            SELECT * FROM wallets_master 
            WHERE signal_score >= $1 AND total_trades >= $2
            ORDER BY signal_score DESC
        """, min_score, min_trades)
        
        # Create CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow([
            'Wallet', 'Signal Score', 'Realized PnL', 'Win Rate', 'Avg Position Size',
            'Market Diversity', 'Timing Edge', 'Closing Efficiency', 'Consistency Score',
            'Total Trades', 'Active Days', 'Last Trade At', 'Last Updated'
        ])
        
        # Write data
        for wallet in wallets:
            writer.writerow([
                wallet['wallet'],
                wallet['signal_score'],
                wallet['realized_pnl'],
                wallet['win_rate'],
                wallet['avg_position_size'],
                wallet['market_diversity'],
                wallet['timing_edge'],
                wallet['closing_efficiency'],
                wallet['consistency_score'],
                wallet['total_trades'],
                wallet['active_days'],
                wallet['last_trade_at'],
                wallet['last_updated']
            ])
        
        # Prepare response
        csv_content = output.getvalue()
        output.close()
        
        filename = f"wallets_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        logger.error(f"Error exporting wallets to CSV: {e}")
        raise HTTPException(status_code=500, detail="Failed to export wallets")

@router.get("/pnl/json")
async def export_pnl_json(
    days: int = Query(30, ge=1, le=365),
    group_by: str = Query("wallet", regex="^(wallet|market|daily)$")
):
    """Export PnL data to JSON format"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        if group_by == "wallet":
            data = await db.fetch("""
                SELECT 
                    t.wallet,
                    w.signal_score,
                    COUNT(*) as trade_count,
                    SUM(t.pnl) as total_pnl,
                    AVG(t.pnl) as avg_pnl,
                    SUM(CASE WHEN t.pnl > 0 THEN 1 ELSE 0 END) as wins
                FROM trades_log t
                LEFT JOIN wallets_master w ON t.wallet = w.wallet
                WHERE t.entry_time >= $1 AND t.pnl IS NOT NULL
                GROUP BY t.wallet, w.signal_score
                ORDER BY total_pnl DESC
            """, cutoff_date)
        elif group_by == "market":
            data = await db.fetch("""
                SELECT 
                    market,
                    COUNT(*) as trade_count,
                    SUM(pnl) as total_pnl,
                    AVG(pnl) as avg_pnl,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                    COUNT(DISTINCT wallet) as unique_traders
                FROM trades_log
                WHERE entry_time >= $1 AND pnl IS NOT NULL
                GROUP BY market
                ORDER BY total_pnl DESC
            """, cutoff_date)
        else:  # daily
            data = await db.fetch("""
                SELECT 
                    DATE(entry_time) as date,
                    COUNT(*) as trade_count,
                    SUM(pnl) as total_pnl,
                    AVG(pnl) as avg_pnl,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins
                FROM trades_log
                WHERE entry_time >= $1 AND pnl IS NOT NULL
                GROUP BY DATE(entry_time)
                ORDER BY date DESC
            """, cutoff_date)
        
        # Convert to JSON-serializable format
        result = []
        for row in data:
            row_dict = dict(row)
            # Convert Decimal and other types to float/string
            for key, value in row_dict.items():
                if value is not None:
                    if hasattr(value, '__float__'):
                        row_dict[key] = float(value)
                    elif hasattr(value, 'isoformat'):
                        row_dict[key] = value.isoformat()
            result.append(row_dict)
        
        export_data = {
            "export_type": f"pnl_by_{group_by}",
            "period_days": days,
            "exported_at": datetime.now().isoformat(),
            "count": len(result),
            "data": result
        }
        
        filename = f"pnl_{group_by}_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        return Response(
            content=json.dumps(export_data, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        logger.error(f"Error exporting PnL to JSON: {e}")
        raise HTTPException(status_code=500, detail="Failed to export PnL data")

@router.get("/alerts/csv")
async def export_alerts_csv(
    days: int = Query(7, ge=1, le=30),
    confidence: Optional[str] = None
):
    """Export alerts to CSV format"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        if confidence:
            alerts = await db.fetch("""
                SELECT a.*, w.signal_score, w.win_rate
                FROM alerts_log a
                LEFT JOIN wallets_master w ON a.wallet = w.wallet
                WHERE a.timestamp >= $1 AND a.confidence = $2
                ORDER BY a.timestamp DESC
            """, cutoff_date, confidence)
        else:
            alerts = await db.fetch("""
                SELECT a.*, w.signal_score, w.win_rate
                FROM alerts_log a
                LEFT JOIN wallets_master w ON a.wallet = w.wallet
                WHERE a.timestamp >= $1
                ORDER BY a.timestamp DESC
            """, cutoff_date)
        
        # Create CSV
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow([
            'ID', 'Wallet', 'Event Type', 'Confidence', 'Signal Reason',
            'Market', 'Timestamp', 'Processed', 'Signal Score', 'Win Rate'
        ])
        
        # Write data
        for alert in alerts:
            writer.writerow([
                alert['id'],
                alert['wallet'],
                alert['event_type'],
                alert['confidence'],
                alert['signal_reason'],
                alert['market'],
                alert['timestamp'],
                alert['processed'],
                alert['signal_score'],
                alert['win_rate']
            ])
        
        # Prepare response
        csv_content = output.getvalue()
        output.close()
        
        filename = f"alerts_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return Response(
            content=csv_content,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        logger.error(f"Error exporting alerts to CSV: {e}")
        raise HTTPException(status_code=500, detail="Failed to export alerts")
