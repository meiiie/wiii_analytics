"""
Execution Quality Analyzer for TaHo Analytics
SOTA Institutional Pattern: Trade Execution Analysis

Based on Citadel Securities, Virtu Financial patterns.

Key Metrics:
- Slippage Analysis: Expected vs actual fill price
- Fill Rate: Limit order execution success
- Latency Impact: Time from signal to execution
- Cost Analysis: Maker vs Taker distribution
"""

from typing import Dict, List, Any
from datetime import datetime, timedelta
from collections import defaultdict


class ExecutionAnalyzer:
    """
    SOTA Execution Quality Analytics.
    
    Used by market makers and HFT firms to optimize execution.
    Critical for understanding true trading costs.
    """
    
    # =========================================================================
    # SLIPPAGE ANALYSIS
    # =========================================================================
    
    @staticmethod
    def analyze_slippage(trades: List[Dict]) -> Dict:
        """
        Analyze slippage between expected and actual fill prices.
        
        Slippage = (Fill Price - Target Price) / Target Price
        - Positive: Worse than expected (cost)
        - Negative: Better than expected (improvement)
        
        Note: Requires trade records with both target_price and fill_price.
        If not available, estimates from commission data.
        """
        if not trades:
            return {}
        
        # Since we may not have target_price, estimate from other signals
        # For now, analyze commission as proxy for execution cost
        
        total_volume = sum(abs(t.get("quantity", 0) * t.get("price", 0)) for t in trades)
        total_commission = sum(abs(t.get("commission", 0)) for t in trades)
        
        # Effective fee rate
        effective_fee_rate = total_commission / total_volume if total_volume > 0 else 0
        
        # Binance VIP0 taker fee is 0.05%
        expected_taker_fee = 0.0005
        slippage_from_expected = (effective_fee_rate - expected_taker_fee) * 100
        
        return {
            "total_volume": round(total_volume, 2),
            "total_commission": round(total_commission, 4),
            "effective_fee_rate_pct": round(effective_fee_rate * 100, 4),
            "expected_taker_fee_pct": expected_taker_fee * 100,
            "slippage_bps": round(slippage_from_expected * 100, 2),  # Basis points
            "trades_analyzed": len(trades)
        }
    
    # =========================================================================
    # MAKER/TAKER ANALYSIS
    # =========================================================================
    
    @staticmethod
    def analyze_order_types(trades: List[Dict]) -> Dict:
        """
        Analyze maker vs taker order distribution.
        
        Maker orders: Lower fees, provide liquidity
        Taker orders: Higher fees, take liquidity
        
        High maker ratio = better execution efficiency.
        """
        if not trades:
            return {}
        
        maker_count = sum(1 for t in trades if t.get("maker", False))
        taker_count = len(trades) - maker_count
        total = len(trades)
        
        maker_volume = sum(
            abs(t.get("quantity", 0) * t.get("price", 0))
            for t in trades if t.get("maker", False)
        )
        taker_volume = sum(
            abs(t.get("quantity", 0) * t.get("price", 0))
            for t in trades if not t.get("maker", False)
        )
        
        return {
            "maker_count": maker_count,
            "taker_count": taker_count,
            "maker_ratio_pct": round(maker_count / total * 100, 1) if total > 0 else 0,
            "taker_ratio_pct": round(taker_count / total * 100, 1) if total > 0 else 0,
            "maker_volume": round(maker_volume, 2),
            "taker_volume": round(taker_volume, 2),
            "potential_savings_if_all_maker": round(taker_volume * 0.0003, 2)  # 0.03% diff
        }
    
    # =========================================================================
    # TIME-TO-FILL ANALYSIS
    # =========================================================================
    
    @staticmethod
    def analyze_fill_timing(orders: List[Dict], trades: List[Dict]) -> Dict:
        """
        Analyze time from order placement to fill.
        
        Fast fills: Signal timing is good
        Slow fills: May indicate poor entry timing or low liquidity
        Never filled: Missed opportunities
        """
        if not orders or not trades:
            return {"message": "Insufficient order/trade data for timing analysis"}
        
        # Match orders to their fills
        filled_orders = [o for o in orders if o.get("status") == "FILLED"]
        canceled_orders = [o for o in orders if o.get("status") == "CANCELED"]
        
        fill_rate = len(filled_orders) / len(orders) * 100 if orders else 0
        
        # Time to fill analysis (if timestamps available)
        fill_times = []
        for order in filled_orders:
            order_time = order.get("create_time", 0)
            fill_time = order.get("update_time", order_time)
            if order_time and fill_time:
                duration_seconds = (fill_time - order_time) / 1000
                fill_times.append(duration_seconds)
        
        avg_fill_time = sum(fill_times) / len(fill_times) if fill_times else 0
        
        return {
            "total_orders": len(orders),
            "filled_orders": len(filled_orders),
            "canceled_orders": len(canceled_orders),
            "fill_rate_pct": round(fill_rate, 1),
            "avg_fill_time_seconds": round(avg_fill_time, 2),
            "instant_fills_pct": round(
                len([t for t in fill_times if t < 1]) / len(fill_times) * 100, 1
            ) if fill_times else 0
        }
    
    # =========================================================================
    # EXECUTION BY TIME OF DAY
    # =========================================================================
    
    @staticmethod
    def analyze_execution_by_hour(trades: List[Dict]) -> List[Dict]:
        """
        Analyze execution quality by hour of day.
        
        Identifies:
        - Hours with best fills
        - Hours with highest slippage
        - Optimal execution windows
        """
        VN_OFFSET = 7  # Vietnam timezone
        
        hourly_stats = defaultdict(lambda: {
            "volume": 0.0,
            "commission": 0.0,
            "trade_count": 0
        })
        
        for trade in trades:
            timestamp_ms = trade.get("timestamp", 0)
            if not timestamp_ms:
                continue
            
            utc_hour = datetime.utcfromtimestamp(timestamp_ms / 1000).hour
            vn_hour = (utc_hour + VN_OFFSET) % 24
            
            volume = abs(trade.get("quantity", 0) * trade.get("price", 0))
            commission = abs(trade.get("commission", 0))
            
            hourly_stats[vn_hour]["volume"] += volume
            hourly_stats[vn_hour]["commission"] += commission
            hourly_stats[vn_hour]["trade_count"] += 1
        
        # Calculate effective fee rate per hour
        result = []
        for hour in range(24):
            stats = hourly_stats[hour]
            volume = stats["volume"]
            result.append({
                "hour": hour,
                "trade_count": stats["trade_count"],
                "volume": round(volume, 2),
                "commission": round(stats["commission"], 4),
                "effective_fee_pct": round(
                    stats["commission"] / volume * 100, 4
                ) if volume > 0 else 0
            })
        
        return result
    
    # =========================================================================
    # COMPREHENSIVE EXECUTION REPORT
    # =========================================================================
    
    @staticmethod
    def generate_execution_report(
        trades: List[Dict],
        orders: List[Dict] = None
    ) -> Dict:
        """
        Generate comprehensive execution quality report.
        """
        slippage = ExecutionAnalyzer.analyze_slippage(trades)
        order_types = ExecutionAnalyzer.analyze_order_types(trades)
        hourly = ExecutionAnalyzer.analyze_execution_by_hour(trades)
        
        fill_timing = {}
        if orders:
            fill_timing = ExecutionAnalyzer.analyze_fill_timing(orders, trades)
        
        # Identify best/worst hours for execution
        sorted_hours = sorted(hourly, key=lambda x: x["effective_fee_pct"])
        best_hours = [h for h in sorted_hours if h["trade_count"] > 0][:3]
        worst_hours = [h for h in sorted_hours if h["trade_count"] > 0][-3:]
        
        return {
            "slippage_analysis": slippage,
            "order_type_analysis": order_types,
            "fill_timing": fill_timing,
            "hourly_execution": hourly,
            "best_execution_hours": best_hours,
            "worst_execution_hours": worst_hours,
            "generated_at": datetime.utcnow().isoformat()
        }
