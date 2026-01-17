"""
Time-Based Analyzer for TaHo Analytics
SOTA: Analyze trading performance by hour and day

Key Insights:
- Which hours are most profitable (Vietnam time UTC+7)
- Daily P&L trends
- Trading session analysis (Asia/EU/US)
"""

from typing import Dict, List, Any
from datetime import datetime, timezone, timedelta
from collections import defaultdict


class TimeAnalyzer:
    """Analyze trading performance by time dimensions."""
    
    # Vietnam timezone offset
    VN_OFFSET = 7  # UTC+7
    
    @staticmethod
    def analyze_by_hour(income_records: List[Dict]) -> List[Dict]:
        """
        Analyze PnL by hour of day (Vietnam time).
        
        Returns:
            List of 24 hourly stats [{hour, pnl, trades, win_rate}, ...]
        """
        hourly_stats = defaultdict(lambda: {"pnl": 0.0, "trades": 0, "wins": 0})
        
        for record in income_records:
            if record.get("income_type") != "REALIZED_PNL":
                continue
            
            timestamp_ms = record.get("timestamp", 0)
            utc_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            vn_hour = (utc_time.hour + TimeAnalyzer.VN_OFFSET) % 24
            
            pnl = record.get("income", 0)
            hourly_stats[vn_hour]["pnl"] += pnl
            hourly_stats[vn_hour]["trades"] += 1
            if pnl > 0:
                hourly_stats[vn_hour]["wins"] += 1
        
        # Build result for all 24 hours
        result = []
        for hour in range(24):
            stats = hourly_stats[hour]
            trades = stats["trades"]
            result.append({
                "hour": hour,
                "total_pnl": round(stats["pnl"], 2),
                "trade_count": trades,
                "win_count": stats["wins"],
                "win_rate": round(stats["wins"] / trades * 100, 1) if trades > 0 else 0
            })
        
        return result
    
    @staticmethod
    def analyze_by_day(income_records: List[Dict]) -> List[Dict]:
        """
        Analyze PnL by date.
        
        Returns:
            List of daily stats [{date, gross_pnl, commission, net_pnl}, ...]
        """
        daily_stats = defaultdict(lambda: {
            "gross_pnl": 0.0,
            "commission": 0.0,
            "funding": 0.0,
            "trades": 0,
            "wins": 0
        })
        
        for record in income_records:
            timestamp_ms = record.get("timestamp", 0)
            utc_time = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            date_str = utc_time.strftime("%Y-%m-%d")
            income_type = record.get("income_type")
            income = record.get("income", 0)
            
            if income_type == "REALIZED_PNL":
                daily_stats[date_str]["gross_pnl"] += income
                daily_stats[date_str]["trades"] += 1
                if income > 0:
                    daily_stats[date_str]["wins"] += 1
            elif income_type == "COMMISSION":
                daily_stats[date_str]["commission"] += abs(income)
            elif income_type == "FUNDING_FEE":
                daily_stats[date_str]["funding"] += income
        
        # Build result
        result = []
        for date_str in sorted(daily_stats.keys(), reverse=True):
            stats = daily_stats[date_str]
            result.append({
                "date": date_str,
                "gross_pnl": round(stats["gross_pnl"], 2),
                "commission": round(stats["commission"], 2),
                "funding": round(stats["funding"], 2),
                "net_pnl": round(stats["gross_pnl"] - stats["commission"] + stats["funding"], 2),
                "trade_count": stats["trades"],
                "win_count": stats["wins"],
                "win_rate": round(stats["wins"] / stats["trades"] * 100, 1) if stats["trades"] > 0 else 0
            })
        
        return result
    
    @staticmethod
    def identify_best_worst_hours(hourly_stats: List[Dict], top_n: int = 3) -> Dict:
        """Identify best and worst trading hours."""
        sorted_by_pnl = sorted(hourly_stats, key=lambda x: x["total_pnl"], reverse=True)
        
        return {
            "best_hours": sorted_by_pnl[:top_n],
            "worst_hours": sorted_by_pnl[-top_n:]
        }
