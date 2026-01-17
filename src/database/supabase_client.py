"""
Supabase Client for TaHo Analytics
SOTA Pattern: Connection pooling with singleton

Database: Supabase PostgreSQL (Free Tier)
"""

import os
from typing import Optional, List, Dict, Any
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# Singleton instance
_client: Optional[Client] = None


def get_supabase() -> Client:
    """Get Supabase client (singleton pattern)."""
    global _client
    if _client is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY required in environment")
        _client = create_client(url, key)
    return _client


class AnalyticsRepository:
    """Repository for analytics data operations."""
    
    def __init__(self):
        self.client = get_supabase()
    
    # =========================================================================
    # TRADES
    # =========================================================================
    
    def upsert_trades(self, trades: List[Dict[str, Any]]) -> int:
        """Insert or update trade records."""
        if not trades:
            return 0
        
        result = self.client.table("trades").upsert(
            trades,
            on_conflict="trade_id"
        ).execute()
        
        return len(result.data) if result.data else 0
    
    def get_trades(self, symbol: str = None, days: int = 30) -> List[Dict]:
        """Get trades for analysis."""
        import time
        start_time = int((time.time() - days * 86400) * 1000)
        
        query = self.client.table("trades").select("*").gte("timestamp", start_time)
        if symbol:
            query = query.eq("symbol", symbol)
        
        result = query.order("timestamp", desc=True).execute()
        return result.data or []
    
    # =========================================================================
    # INCOME
    # =========================================================================
    
    def upsert_income(self, income_records: List[Dict[str, Any]]) -> int:
        """Insert income records (PnL, Funding, Commission)."""
        if not income_records:
            return 0
        
        # Generate unique ID for deduplication
        for record in income_records:
            if "id" not in record:
                record["id"] = f"{record.get('timestamp')}_{record.get('income_type')}_{record.get('trade_id', '')}"
        
        result = self.client.table("income").upsert(
            income_records,
            on_conflict="id"
        ).execute()
        
        return len(result.data) if result.data else 0
    
    def get_income_by_type(self, income_type: str, days: int = 30) -> List[Dict]:
        """Get income records by type."""
        import time
        start_time = int((time.time() - days * 86400) * 1000)
        
        result = self.client.table("income").select("*") \
            .eq("income_type", income_type) \
            .gte("timestamp", start_time) \
            .order("timestamp", desc=True) \
            .execute()
        
        return result.data or []
    
    # =========================================================================
    # DAILY SUMMARY
    # =========================================================================
    
    def upsert_daily_summary(self, summary: Dict[str, Any]) -> bool:
        """Upsert daily summary."""
        result = self.client.table("daily_summary").upsert(
            summary,
            on_conflict="date"
        ).execute()
        return bool(result.data)
    
    def get_daily_summaries(self, days: int = 30) -> List[Dict]:
        """Get daily summaries."""
        from datetime import datetime, timedelta
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        result = self.client.table("daily_summary").select("*") \
            .gte("date", start_date) \
            .order("date", desc=True) \
            .execute()
        
        return result.data or []
    
    # =========================================================================
    # HOURLY STATS
    # =========================================================================
    
    def upsert_hourly_stats(self, stats: List[Dict[str, Any]]) -> int:
        """Upsert hourly performance stats."""
        if not stats:
            return 0
        
        result = self.client.table("hourly_stats").upsert(
            stats,
            on_conflict="hour"
        ).execute()
        
        return len(result.data) if result.data else 0
    
    def get_hourly_stats(self) -> List[Dict]:
        """Get all hourly stats (0-23)."""
        result = self.client.table("hourly_stats").select("*") \
            .order("hour") \
            .execute()
        
        return result.data or []
