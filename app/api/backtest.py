from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime, timedelta
import asyncio
import logging

from app.models.database import db
from app.services.wallet_scoring import WalletScoringService

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post("/run")
async def run_backtest(
    days_back: int = Query(30, ge=7, le=365),
    min_signal_score: float = Query(0.75, ge=0, le=1),
    max_positions: int = Query(5, ge=1, le=20),
    position_size: float = Query(100.0, ge=10.0, le=10000.0)
):
    """Run a backtest simulation"""
    try:
        # Create backtest record
        backtest_id = await db.fetchval("""
            INSERT INTO backtest_results (days_back, min_signal_score, status)
            VALUES ($1, $2, 'running')
            RETURNING id
        """, days_back, min_signal_score)
        
        # Run backtest in background
        asyncio.create_task(_execute_backtest(
            str(backtest_id), days_back, min_signal_score, max_positions, position_size
        ))
        
        return {
            "backtest_id": str(backtest_id),
            "status": "started",
            "parameters": {
                "days_back": days_back,
                "min_signal_score": min_signal_score,
                "max_positions": max_positions,
                "position_size": position_size
            }
        }
        
    except Exception as e:
        logger.error(f"Error starting backtest: {e}")
        raise HTTPException(status_code=500, detail="Failed to start backtest")

@router.get("/{backtest_id}")
async def get_backtest_result(backtest_id: str):
    """Get backtest results by ID"""
    try:
        result = await db.fetchrow("""
            SELECT * FROM backtest_results WHERE id = $1
        """, backtest_id)
        
        if not result:
            raise HTTPException(status_code=404, detail="Backtest not found")
        
        return {
            "backtest_id": backtest_id,
            "status": result['status'],
            "parameters": {
                "days_back": result['days_back'],
                "min_signal_score": float(result['min_signal_score'])
            },
            "results": {
                "total_pnl": float(result['total_pnl'] or 0),
                "win_rate": float(result['win_rate'] or 0),
                "total_trades": result['total_trades']
            } if result['status'] == 'completed' else None,
            "error": result['error'] if result['status'] == 'failed' else None,
            "created_at": result['created_at'],
            "completed_at": result['completed_at']
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching backtest result: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch backtest result")

@router.get("/")
async def list_backtests(limit: int = Query(20, ge=1, le=100)):
    """List recent backtests"""
    try:
        backtests = await db.fetch("""
            SELECT * FROM backtest_results 
            ORDER BY created_at DESC 
            LIMIT $1
        """, limit)
        
        return {
            "backtests": [
                {
                    "backtest_id": str(b['id']),
                    "status": b['status'],
                    "days_back": b['days_back'],
                    "min_signal_score": float(b['min_signal_score']),
                    "total_pnl": float(b['total_pnl'] or 0) if b['total_pnl'] else None,
                    "win_rate": float(b['win_rate'] or 0) if b['win_rate'] else None,
                    "total_trades": b['total_trades'],
                    "created_at": b['created_at'],
                    "completed_at": b['completed_at']
                }
                for b in backtests
            ],
            "count": len(backtests)
        }
        
    except Exception as e:
        logger.error(f"Error listing backtests: {e}")
        raise HTTPException(status_code=500, detail="Failed to list backtests")

async def _execute_backtest(
    backtest_id: str, 
    days_back: int, 
    min_signal_score: float,
    max_positions: int,
    position_size: float
):
    """Execute backtest simulation (internal function)"""
    try:
        logger.info(f"Starting backtest {backtest_id}")
        
        # Define simulation period
        end_date = datetime.now() - timedelta(days=1)  # End yesterday
        start_date = end_date - timedelta(days=days_back)
        
        # Get all trades in the period with sufficient wallet scores
        trades = await db.fetch("""
            SELECT t.*, w.signal_score
            FROM trades_log t
            JOIN wallets_master w ON t.wallet = w.wallet
            WHERE t.entry_time BETWEEN $1 AND $2
            AND w.signal_score >= $3
            AND t.pnl IS NOT NULL
            ORDER BY t.entry_time
        """, start_date, end_date, min_signal_score)
        
        if not trades:
            await db.execute("""
                UPDATE backtest_results 
                SET status = 'failed', error = 'No trades found for simulation'
                WHERE id = $1
            """, backtest_id)
            return
        
        # Simulate trading strategy
        portfolio = {
            'cash': 10000.0,  # Starting capital
            'positions': {},  # {trade_id: position_info}
            'total_trades': 0,
            'winning_trades': 0,
            'total_pnl': 0.0
        }
        
        for trade in trades:
            wallet = trade['wallet']
            signal_score = float(trade['signal_score'])
            
            # Skip if we already have max positions
            if len(portfolio['positions']) >= max_positions:
                continue
            
            # Entry logic: only enter if we have enough cash and high signal score
            entry_cost = position_size
            if portfolio['cash'] >= entry_cost and signal_score >= min_signal_score:
                # Enter position
                position_id = f"{trade['id']}"
                portfolio['positions'][position_id] = {
                    'entry_price': float(trade['entry_price']),
                    'position_size': position_size,
                    'direction': trade['direction'],
                    'entry_time': trade['entry_time'],
                    'wallet': wallet,
                    'signal_score': signal_score
                }
                portfolio['cash'] -= entry_cost
                portfolio['total_trades'] += 1
                
                # Simulate exit based on actual trade result
                if trade['exit_price'] and trade['pnl']:
                    exit_price = float(trade['exit_price'])
                    actual_pnl = float(trade['pnl'])
                    
                    # Scale PnL to our position size
                    original_size = float(trade['position_size'])
                    scale_factor = position_size / (original_size * float(trade['entry_price']))
                    simulated_pnl = actual_pnl * scale_factor
                    
                    # Close position
                    portfolio['cash'] += position_size + simulated_pnl
                    portfolio['total_pnl'] += simulated_pnl
                    
                    if simulated_pnl > 0:
                        portfolio['winning_trades'] += 1
                    
                    # Remove from active positions
                    del portfolio['positions'][position_id]
        
        # Calculate final metrics
        win_rate = portfolio['winning_trades'] / portfolio['total_trades'] if portfolio['total_trades'] > 0 else 0
        
        # Update backtest results
        await db.execute("""
            UPDATE backtest_results 
            SET status = 'completed',
                total_pnl = $1,
                win_rate = $2,
                total_trades = $3,
                completed_at = NOW()
            WHERE id = $4
        """, portfolio['total_pnl'], win_rate, portfolio['total_trades'], backtest_id)
        
        logger.info(f"Backtest {backtest_id} completed: PnL={portfolio['total_pnl']:.2f}, Win Rate={win_rate:.3f}, Trades={portfolio['total_trades']}")
        
    except Exception as e:
        logger.error(f"Error in backtest {backtest_id}: {e}")
        await db.execute("""
            UPDATE backtest_results 
            SET status = 'failed', error = $1
            WHERE id = $2
        """, str(e), backtest_id)

@router.delete("/{backtest_id}")
async def delete_backtest(backtest_id: str):
    """Delete a backtest result"""
    try:
        result = await db.execute("""
            DELETE FROM backtest_results WHERE id = $1
        """, backtest_id)
        
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Backtest not found")
        
        return {"status": "deleted", "backtest_id": backtest_id}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting backtest: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete backtest")
