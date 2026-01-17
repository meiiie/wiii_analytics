"""
Binance Data Collector for TaHo Analytics
SOTA Pattern: Fetch income, trades, funding from Binance Futures API

Endpoints used:
- /fapi/v1/income (REALIZED_PNL, FUNDING_FEE, COMMISSION)
- /fapi/v1/userTrades
- /fapi/v1/fundingRate (public)
"""

import os
import time
import hmac
import hashlib
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone, timedelta
import httpx
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://fapi.binance.com"


class BinanceCollector:
    """Collect trading analytics data from Binance Futures API."""
    
    def __init__(self):
        self.api_key = os.getenv("BINANCE_API_KEY")
        self.api_secret = os.getenv("BINANCE_API_SECRET")
        
        if not self.api_key or not self.api_secret:
            raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET required")
    
    def _sign(self, params: Dict) -> str:
        """Generate HMAC SHA256 signature."""
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    def _request(self, endpoint: str, params: Dict = None, signed: bool = False) -> Any:
        """Make API request."""
        params = params or {}
        headers = {"X-MBX-APIKEY": self.api_key}
        
        if signed:
            params["timestamp"] = int(time.time() * 1000)
            params["signature"] = self._sign(params)
        
        url = f"{BASE_URL}{endpoint}"
        
        with httpx.Client() as client:
            response = client.get(url, params=params, headers=headers, timeout=30.0)
            
            if response.status_code != 200:
                raise Exception(f"API Error: {response.status_code} - {response.text}")
            
            return response.json()
    
    # =========================================================================
    # INCOME HISTORY
    # =========================================================================
    
    def fetch_income(self, income_type: str, days: int = 30) -> List[Dict]:
        """
        Fetch income history by type.
        
        Args:
            income_type: REALIZED_PNL, FUNDING_FEE, COMMISSION
            days: Number of days to fetch
            
        Returns:
            List of income records
        """
        start_time = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
        
        params = {
            "incomeType": income_type,
            "startTime": start_time,
            "limit": 1000
        }
        
        data = self._request("/fapi/v1/income", params, signed=True)
        
        # Transform for database
        records = []
        for item in data:
            records.append({
                "symbol": item.get("symbol", ""),
                "income_type": item.get("incomeType"),
                "income": float(item.get("income", 0)),
                "timestamp": item.get("time"),
                "trade_id": item.get("tradeId", "")
            })
        
        return records
    
    def fetch_all_income(self, days: int = 30) -> Dict[str, List[Dict]]:
        """Fetch all income types."""
        return {
            "realized_pnl": self.fetch_income("REALIZED_PNL", days),
            "funding_fee": self.fetch_income("FUNDING_FEE", days),
            "commission": self.fetch_income("COMMISSION", days)
        }
    
    # =========================================================================
    # TRADE HISTORY
    # =========================================================================
    
    def fetch_trades(self, symbol: str, days: int = 7) -> List[Dict]:
        """Fetch user trade history for a symbol."""
        start_time = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
        
        params = {
            "symbol": symbol,
            "startTime": start_time,
            "limit": 500
        }
        
        data = self._request("/fapi/v1/userTrades", params, signed=True)
        
        # Transform for database
        records = []
        for item in data:
            records.append({
                "symbol": item.get("symbol"),
                "side": item.get("side"),
                "quantity": float(item.get("qty", 0)),
                "price": float(item.get("price", 0)),
                "realized_pnl": float(item.get("realizedPnl", 0)),
                "commission": float(item.get("commission", 0)),
                "timestamp": item.get("time"),
                "trade_id": str(item.get("id"))
            })
        
        return records
    
    def fetch_trades_multi(self, symbols: List[str], days: int = 7) -> List[Dict]:
        """Fetch trades for multiple symbols."""
        all_trades = []
        for symbol in symbols[:10]:  # Limit to avoid rate limits
            try:
                trades = self.fetch_trades(symbol, days)
                all_trades.extend(trades)
            except Exception as e:
                print(f"Error fetching trades for {symbol}: {e}")
        
        return all_trades
    
    # =========================================================================
    # ACCOUNT INFO
    # =========================================================================
    
    def fetch_account(self) -> Dict:
        """Fetch current account info."""
        data = self._request("/fapi/v2/account", signed=True)
        
        return {
            "wallet_balance": float(data.get("totalWalletBalance", 0)),
            "unrealized_pnl": float(data.get("totalUnrealizedProfit", 0)),
            "available_balance": float(data.get("availableBalance", 0)),
            "margin_balance": float(data.get("totalMarginBalance", 0))
        }
    
    # =========================================================================
    # FUNDING RATES (Public)
    # =========================================================================
    
    def fetch_funding_rate(self, symbol: str = "BTCUSDT", limit: int = 50) -> List[Dict]:
        """Fetch funding rate history (public endpoint)."""
        params = {"symbol": symbol, "limit": limit}
        data = self._request("/fapi/v1/fundingRate", params, signed=False)
        
        return [
            {
                "symbol": item.get("symbol"),
                "funding_rate": float(item.get("fundingRate", 0)),
                "funding_time": item.get("fundingTime")
            }
            for item in data
        ]
