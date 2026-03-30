import asyncio
import aiohttp
from typing import Optional, Dict, Any
from config.settings import settings

class TelegramBot:
    def __init__(self):
        self.token = settings.TELEGRAM_BOT_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.token}" if self.token else None
        
    async def send_message(self, message: str, parse_mode: str = "Markdown") -> Dict[str, Any]:
        """Send a message via Telegram Bot API"""
        if not self.token or not self.chat_id:
            print(f"Telegram not configured. Message would be: {message}")
            return {"success": False, "error": "Telegram not configured"}
        
        try:
            async with aiohttp.ClientSession() as session:
                payload = {
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": True
                }
                
                async with session.post(
                    f"{self.base_url}/sendMessage",
                    json=payload
                ) as response:
                    result = await response.json()
                    
                    if response.status == 200 and result.get("ok"):
                        return {"success": True, "message_id": result["result"]["message_id"]}
                    else:
                        print(f"Telegram API error: {result}")
                        return {"success": False, "error": result.get("description", "Unknown error")}
                        
        except Exception as e:
            print(f"Error sending Telegram message: {e}")
            return {"success": False, "error": str(e)}
    
    async def send_alert(self, title: str, details: str, emoji: str = "🔔") -> Dict[str, Any]:
        """Send a formatted alert message"""
        message = f"{emoji} **{title}**\n\n{details}"
        return await self.send_message(message)
    
    async def send_trade_alert(self, trade_data: Dict[str, Any]) -> Dict[str, Any]:
        """Send a trade-specific alert"""
        direction_emoji = "📈" if trade_data.get("direction") == "buy" else "📉"
        
        message = f"{direction_emoji} **TRADE ALERT**\n\n"
        message += f"**Market:** {trade_data.get('market', 'Unknown')}\n"
        message += f"**Direction:** {trade_data.get('direction', 'Unknown').upper()}\n"
        message += f"**Price:** ${trade_data.get('price', 0):.3f}\n"
        message += f"**Size:** ${trade_data.get('size', 0):,.2f}\n"
        
        if 'confidence' in trade_data:
            message += f"**Confidence:** {trade_data['confidence'].upper()}\n"
        
        if 'signal_score' in trade_data:
            message += f"**Signal Score:** {trade_data['signal_score']:.3f}\n"
        
        return await self.send_message(message)
    
    async def send_pnl_update(self, pnl: float, additional_info: Optional[str] = None) -> Dict[str, Any]:
        """Send P&L update message"""
        emoji = "✅" if pnl > 0 else "❌" if pnl < 0 else "➡️"
        
        message = f"{emoji} **P&L UPDATE**\n\n"
        message += f"**Amount:** ${pnl:,.2f}\n"
        
        if additional_info:
            message += f"\n{additional_info}"
        
        return await self.send_message(message)
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test Telegram bot connection"""
        if not self.token:
            return {"success": False, "error": "No bot token configured"}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_url}/getMe") as response:
                    result = await response.json()
                    
                    if response.status == 200 and result.get("ok"):
                        bot_info = result["result"]
                        return {
                            "success": True,
                            "bot_username": bot_info.get("username"),
                            "bot_name": bot_info.get("first_name")
                        }
                    else:
                        return {"success": False, "error": result.get("description", "Unknown error")}
                        
        except Exception as e:
            return {"success": False, "error": str(e)}
