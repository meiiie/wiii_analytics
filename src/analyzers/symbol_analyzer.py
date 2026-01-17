"""
Symbol-Based Analyzer for TaHo Analytics
SOTA: Analyze trading performance by symbol

Key Insights:
- Which tokens are most profitable
- Win rate per symbol
- Average trade size and duration
"""

from typing import Dict, List, Any
from collections import defaultdict


class SymbolAnalyzer:
    """Analyze trading performance by symbol."""
    
    @staticmethod
    def analyze_by_symbol(income_records: List[Dict]) -> List[Dict]:
        """
        Analyze PnL by symbol.
        
        Returns:
            List of symbol stats sorted by PnL descending
        """
        symbol_stats = defaultdict(lambda: {
            "pnl": 0.0,
            "commission": 0.0,
            "trades": 0,
            "wins": 0,
            "losses": 0
        })
        
        for record in income_records:
            symbol = record.get("symbol", "UNKNOWN")
            if not symbol:
                continue
            
            income_type = record.get("income_type")
            income = record.get("income", 0)
            
            if income_type == "REALIZED_PNL":
                symbol_stats[symbol]["pnl"] += income
                symbol_stats[symbol]["trades"] += 1
                if income > 0:
                    symbol_stats[symbol]["wins"] += 1
                else:
                    symbol_stats[symbol]["losses"] += 1
            elif income_type == "COMMISSION":
                symbol_stats[symbol]["commission"] += abs(income)
        
        # Build result
        result = []
        for symbol, stats in symbol_stats.items():
            trades = stats["trades"]
            result.append({
                "symbol": symbol,
                "gross_pnl": round(stats["pnl"], 2),
                "commission": round(stats["commission"], 2),
                "net_pnl": round(stats["pnl"] - stats["commission"], 2),
                "trade_count": trades,
                "win_count": stats["wins"],
                "loss_count": stats["losses"],
                "win_rate": round(stats["wins"] / trades * 100, 1) if trades > 0 else 0,
                "avg_pnl_per_trade": round(stats["pnl"] / trades, 4) if trades > 0 else 0
            })
        
        # Sort by net PnL descending
        result.sort(key=lambda x: x["net_pnl"], reverse=True)
        
        return result
    
    @staticmethod
    def get_top_performers(symbol_stats: List[Dict], n: int = 10) -> Dict:
        """Get top and bottom performing symbols."""
        return {
            "top_profitable": symbol_stats[:n],
            "top_losing": symbol_stats[-n:][::-1] if len(symbol_stats) >= n else symbol_stats[::-1]
        }
    
    @staticmethod
    def get_most_traded(symbol_stats: List[Dict], n: int = 10) -> List[Dict]:
        """Get most frequently traded symbols."""
        sorted_by_trades = sorted(symbol_stats, key=lambda x: x["trade_count"], reverse=True)
        return sorted_by_trades[:n]
