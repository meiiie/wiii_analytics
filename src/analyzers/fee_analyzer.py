"""
Fee Analyzer for TaHo Analytics
SOTA: Analyze trading fees and their impact on performance

Key Insights:
- Commission as % of gross PnL
- Maker vs Taker breakdown
- Funding fee impact
"""

from typing import Dict, List, Any
from collections import defaultdict


class FeeAnalyzer:
    """Analyze trading fees and their impact."""
    
    @staticmethod
    def analyze_fees(income_records: List[Dict]) -> Dict:
        """
        Analyze all fees (commission, funding).
        
        Returns:
            Fee breakdown and impact metrics
        """
        totals = {
            "gross_pnl": 0.0,
            "commission": 0.0,
            "funding_received": 0.0,
            "funding_paid": 0.0,
            "trade_count": 0
        }
        
        for record in income_records:
            income_type = record.get("income_type")
            income = float(record.get("income", 0))
            
            if income_type == "REALIZED_PNL":
                totals["gross_pnl"] += income
                totals["trade_count"] += 1
            elif income_type == "COMMISSION":
                totals["commission"] += abs(income)
            elif income_type == "FUNDING_FEE":
                if income > 0:
                    totals["funding_received"] += income
                else:
                    totals["funding_paid"] += abs(income)
        
        # Calculate metrics
        gross = totals["gross_pnl"]
        commission = totals["commission"]
        net_funding = totals["funding_received"] - totals["funding_paid"]
        net_pnl = gross - commission + net_funding
        
        return {
            "gross_pnl": round(gross, 2),
            "total_commission": round(commission, 2),
            "funding_received": round(totals["funding_received"], 2),
            "funding_paid": round(totals["funding_paid"], 2),
            "net_funding": round(net_funding, 2),
            "net_pnl": round(net_pnl, 2),
            "trade_count": totals["trade_count"],
            "commission_pct_of_gross": round(commission / gross * 100, 1) if gross > 0 else 0,
            "avg_commission_per_trade": round(commission / totals["trade_count"], 4) if totals["trade_count"] > 0 else 0
        }
    
    @staticmethod
    def analyze_fees_by_symbol(income_records: List[Dict]) -> List[Dict]:
        """
        Analyze fees by symbol.
        
        Returns:
            List of symbol fee stats sorted by commission descending
        """
        symbol_fees = defaultdict(lambda: {
            "commission": 0.0,
            "funding": 0.0,
            "trades": 0
        })
        
        for record in income_records:
            symbol = record.get("symbol", "UNKNOWN")
            if not symbol:
                continue
            
            income_type = record.get("income_type")
            income = float(record.get("income", 0))
            
            if income_type == "COMMISSION":
                symbol_fees[symbol]["commission"] += abs(income)
                symbol_fees[symbol]["trades"] += 1
            elif income_type == "FUNDING_FEE":
                symbol_fees[symbol]["funding"] += income
        
        # Build result
        result = []
        for symbol, fees in symbol_fees.items():
            result.append({
                "symbol": symbol,
                "total_commission": round(fees["commission"], 2),
                "net_funding": round(fees["funding"], 2),
                "trade_count": fees["trades"],
                "avg_commission": round(fees["commission"] / fees["trades"], 4) if fees["trades"] > 0 else 0
            })
        
        # Sort by commission descending
        result.sort(key=lambda x: x["total_commission"], reverse=True)
        
        return result
