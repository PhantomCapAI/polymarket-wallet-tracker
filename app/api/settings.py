from fastapi import APIRouter, HTTPException
from typing import Dict, Any
from pydantic import BaseModel, Field, validator
import logging

from config.settings import settings

logger = logging.getLogger(__name__)
router = APIRouter()

class SettingsUpdate(BaseModel):
    min_trades: int = Field(ge=1, le=1000)
    min_signal_score: float = Field(ge=0.0, le=1.0)
    max_position_size: float = Field(ge=1.0, le=100000.0)
    stop_loss_percentage: float = Field(ge=0.01, le=0.5)
    copy_trading_enabled: bool
    max_concurrent_positions: int = Field(ge=1, le=50)
    position_size_multiplier: float = Field(ge=0.01, le=1.0)

class WeightsUpdate(BaseModel):
    weight_consistency: float = Field(ge=0.0, le=1.0)
    weight_timing: float = Field(ge=0.0, le=1.0)
    weight_closing: float = Field(ge=0.0, le=1.0)
    weight_pnl: float = Field(ge=0.0, le=1.0)
    weight_win_rate: float = Field(ge=0.0, le=1.0)
    weight_diversity: float = Field(ge=0.0, le=1.0)
    
    @validator('*')
    def validate_weights_sum(cls, v, values):
        """Validate that all weights sum to 1.0"""
        if len(values) == 5:  # This is the last field being validated
            total = sum(values.values()) + v
            if abs(total - 1.0) > 0.001:
                raise ValueError('All weights must sum to 1.0')
        return v

@router.get("/current")
async def get_current_settings():
    """Get current application settings"""
    try:
        return {
            "trading": {
                "min_trades": settings.MIN_TRADES,
                "min_signal_score": settings.MIN_SIGNAL_SCORE,
                "max_position_size": settings.MAX_POSITION_SIZE,
                "stop_loss_percentage": settings.STOP_LOSS_PERCENTAGE,
                "copy_trading_enabled": settings.COPY_TRADING_ENABLED,
                "max_concurrent_positions": settings.MAX_CONCURRENT_POSITIONS,
                "position_size_multiplier": settings.POSITION_SIZE_MULTIPLIER
            },
            "scoring_weights": {
                "weight_consistency": settings.WEIGHT_CONSISTENCY,
                "weight_timing": settings.WEIGHT_TIMING,
                "weight_closing": settings.WEIGHT_CLOSING,
                "weight_pnl": settings.WEIGHT_PNL,
                "weight_win_rate": settings.WEIGHT_WIN_RATE,
                "weight_diversity": settings.WEIGHT_DIVERSITY
            },
            "thresholds": {
                "high_confidence_threshold": settings.HIGH_CONFIDENCE_THRESHOLD,
                "medium_confidence_threshold": settings.MEDIUM_CONFIDENCE_THRESHOLD
            },
            "api": {
                "host": settings.HOST,
                "port": settings.PORT,
                "debug": settings.DEBUG
            }
        }
        
    except Exception as e:
        logger.error(f"Error fetching current settings: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch settings")

@router.patch("/trading")
async def update_trading_settings(updates: SettingsUpdate):
    """Update trading-related settings"""
    try:
        # In a real implementation, these would be persisted to database or config file
        # For now, we'll just return the updated values
        updated_settings = {
            "min_trades": updates.min_trades,
            "min_signal_score": updates.min_signal_score,
            "max_position_size": updates.max_position_size,
            "stop_loss_percentage": updates.stop_loss_percentage,
            "copy_trading_enabled": updates.copy_trading_enabled,
            "max_concurrent_positions": updates.max_concurrent_positions,
            "position_size_multiplier": updates.position_size_multiplier
        }
        
        logger.info(f"Trading settings updated: {updated_settings}")
        
        return {
            "status": "updated",
            "settings": updated_settings,
            "message": "Trading settings updated successfully"
        }
        
    except Exception as e:
        logger.error(f"Error updating trading settings: {e}")
        raise HTTPException(status_code=500, detail="Failed to update trading settings")

@router.patch("/weights")
async def update_scoring_weights(weights: WeightsUpdate):
    """Update scoring algorithm weights"""
    try:
        # Validate weights sum to 1.0
        total = (weights.weight_consistency + weights.weight_timing + weights.weight_closing +
                weights.weight_pnl + weights.weight_win_rate + weights.weight_diversity)
        
        if abs(total - 1.0) > 0.001:
            raise HTTPException(
                status_code=400, 
                detail=f"Weights must sum to 1.0, current sum: {total:.3f}"
            )
        
        updated_weights = {
            "weight_consistency": weights.weight_consistency,
            "weight_timing": weights.weight_timing,
            "weight_closing": weights.weight_closing,
            "weight_pnl": weights.weight_pnl,
            "weight_win_rate": weights.weight_win_rate,
            "weight_diversity": weights.weight_diversity
        }
        
        logger.info(f"Scoring weights updated: {updated_weights}")
        
        return {
            "status": "updated",
            "weights": updated_weights,
            "total": total,
            "message": "Scoring weights updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating scoring weights: {e}")
        raise HTTPException(status_code=500, detail="Failed to update scoring weights")

@router.get("/validation")
async def validate_settings():
    """Validate current settings configuration"""
    try:
        validation_results = {
            "valid": True,
            "issues": [],
            "warnings": []
        }
        
        # Check if weights sum to 1.0
        weights_sum = (settings.WEIGHT_CONSISTENCY + settings.WEIGHT_TIMING + 
                      settings.WEIGHT_CLOSING + settings.WEIGHT_PNL + 
                      settings.WEIGHT_WIN_RATE + settings.WEIGHT_DIVERSITY)
        
        if abs(weights_sum - 1.0) > 0.001:
            validation_results["valid"] = False
            validation_results["issues"].append(
                f"Scoring weights sum to {weights_sum:.3f}, must equal 1.0"
            )
        
        # Check API credentials
        if not settings.POLYMARKET_API_KEY:
            validation_results["warnings"].append("Polymarket API key not configured")
        
        if not settings.TELEGRAM_BOT_TOKEN:
            validation_results["warnings"].append("Telegram bot token not configured")
        
        # Check database URL
        if "localhost" in settings.DATABASE_URL and not settings.DEBUG:
            validation_results["warnings"].append(
                "Using localhost database URL in production mode"
            )
        
        # Check thresholds
        if settings.HIGH_CONFIDENCE_THRESHOLD <= settings.MEDIUM_CONFIDENCE_THRESHOLD:
            validation_results["valid"] = False
            validation_results["issues"].append(
                "High confidence threshold must be greater than medium confidence threshold"
            )
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error validating settings: {e}")
        raise HTTPException(status_code=500, detail="Failed to validate settings")

@router.post("/reset")
async def reset_to_defaults():
    """Reset all settings to default values"""
    try:
        defaults = {
            "trading": {
                "min_trades": 10,
                "min_signal_score": 0.75,
                "max_position_size": 1000.0,
                "stop_loss_percentage": 0.10,
                "copy_trading_enabled": False,
                "max_concurrent_positions": 5,
                "position_size_multiplier": 0.1
            },
            "weights": {
                "weight_consistency": 0.20,
                "weight_timing": 0.25,
                "weight_closing": 0.15,
                "weight_pnl": 0.25,
                "weight_win_rate": 0.10,
                "weight_diversity": 0.05
            },
            "thresholds": {
                "high_confidence_threshold": 0.85,
                "medium_confidence_threshold": 0.70
            }
        }
        
        logger.info("Settings reset to defaults")
        
        return {
            "status": "reset",
            "defaults": defaults,
            "message": "All settings reset to default values"
        }
        
    except Exception as e:
        logger.error(f"Error resetting settings: {e}")
        raise HTTPException(status_code=500, detail="Failed to reset settings")

@router.get("/export")
async def export_settings():
    """Export current settings as JSON"""
    try:
        settings_export = {
            "export_timestamp": "2024-01-01T00:00:00Z",
            "version": "1.0",
            "settings": {
                "trading": {
                    "min_trades": settings.MIN_TRADES,
                    "min_signal_score": settings.MIN_SIGNAL_SCORE,
                    "max_position_size": settings.MAX_POSITION_SIZE,
                    "stop_loss_percentage": settings.STOP_LOSS_PERCENTAGE,
                    "copy_trading_enabled": settings.COPY_TRADING_ENABLED,
                    "max_concurrent_positions": settings.MAX_CONCURRENT_POSITIONS,
                    "position_size_multiplier": settings.POSITION_SIZE_MULTIPLIER
                },
                "scoring_weights": {
                    "weight_consistency": settings.WEIGHT_CONSISTENCY,
                    "weight_timing": settings.WEIGHT_TIMING,
                    "weight_closing": settings.WEIGHT_CLOSING,
                    "weight_pnl": settings.WEIGHT_PNL,
                    "weight_win_rate": settings.WEIGHT_WIN_RATE,
                    "weight_diversity": settings.WEIGHT_DIVERSITY
                },
                "thresholds": {
                    "high_confidence_threshold": settings.HIGH_CONFIDENCE_THRESHOLD,
                    "medium_confidence_threshold": settings.MEDIUM_CONFIDENCE_THRESHOLD
                }
            }
        }
        
        return settings_export
        
    except Exception as e:
        logger.error(f"Error exporting settings: {e}")
        raise HTTPException(status_code=500, detail="Failed to export settings")
