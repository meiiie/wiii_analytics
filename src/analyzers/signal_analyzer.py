"""
Signal Attribution Analyzer for TaHo Analytics
SOTA Institutional Pattern: Performance Attribution

Based on Two Sigma, AQR Performance Attribution methodology.

Key Analysis:
- Which signal types perform best
- Confidence level vs actual outcomes
- Entry timing effectiveness
- Exit reason breakdown
"""

from typing import Dict, List, Any
from datetime import datetime
from collections import defaultdict


class SignalAnalyzer:
    """
    SOTA Signal Attribution Analytics.
    
    Answers critical questions:
    - Are high confidence signals actually better?
    - Which entry conditions are most profitable?
    - Which exit reasons are most common?
    """
    
    # =========================================================================
    # CONFIDENCE ATTRIBUTION
    # =========================================================================
    
    @staticmethod
    def analyze_by_confidence(trades: List[Dict]) -> Dict:
        """
        Analyze trade performance by confidence level.
        
        Groups trades into confidence buckets and measures:
        - Win rate per bucket
        - Average PnL per bucket
        - Expected relationship: Higher confidence = Better results
        """
        # Confidence buckets
        buckets = {
            "low": {"min": 0, "max": 0.4, "trades": [], "pnl": 0},
            "medium": {"min": 0.4, "max": 0.7, "trades": [], "pnl": 0},
            "high": {"min": 0.7, "max": 1.0, "trades": [], "pnl": 0}
        }
        
        for trade in trades:
            confidence = trade.get("confidence", 0.5)
            pnl = trade.get("realized_pnl", 0)
            
            for name, bucket in buckets.items():
                if bucket["min"] <= confidence < bucket["max"]:
                    bucket["trades"].append(trade)
                    bucket["pnl"] += pnl
                    break
        
        result = {}
        for name, bucket in buckets.items():
            trades_in_bucket = bucket["trades"]
            count = len(trades_in_bucket)
            wins = len([t for t in trades_in_bucket if t.get("realized_pnl", 0) > 0])
            
            result[name] = {
                "trade_count": count,
                "win_count": wins,
                "win_rate": round(wins / count * 100, 1) if count > 0 else 0,
                "total_pnl": round(bucket["pnl"], 2),
                "avg_pnl": round(bucket["pnl"] / count, 4) if count > 0 else 0
            }
        
        # Check if confidence correlation is positive (as expected)
        is_confidence_valid = (
            result["high"]["win_rate"] >= result["medium"]["win_rate"] >= result["low"]["win_rate"]
        )
        
        return {
            "by_confidence": result,
            "confidence_correlation_valid": is_confidence_valid,
            "recommendation": (
                "Confidence scoring is effective" if is_confidence_valid
                else "Consider re-calibrating confidence model"
            )
        }
    
    # =========================================================================
    # DIRECTION ATTRIBUTION
    # =========================================================================
    
    @staticmethod
    def analyze_by_direction(trades: List[Dict]) -> Dict:
        """
        Analyze performance by trade direction (LONG vs SHORT).
        
        Identifies market regime bias:
        - Bull market: LONGs should outperform
        - Bear market: SHORTs should outperform
        - Ranging: Should be roughly equal
        """
        long_trades = [t for t in trades if t.get("side", "").upper() in ["BUY", "LONG"]]
        short_trades = [t for t in trades if t.get("side", "").upper() in ["SELL", "SHORT"]]
        
        def calc_stats(trade_list):
            if not trade_list:
                return {"count": 0, "pnl": 0, "win_rate": 0}
            
            count = len(trade_list)
            pnl = sum(t.get("realized_pnl", 0) for t in trade_list)
            wins = len([t for t in trade_list if t.get("realized_pnl", 0) > 0])
            
            return {
                "count": count,
                "total_pnl": round(pnl, 2),
                "win_rate": round(wins / count * 100, 1) if count > 0 else 0,
                "avg_pnl": round(pnl / count, 4) if count > 0 else 0
            }
        
        long_stats = calc_stats(long_trades)
        short_stats = calc_stats(short_trades)
        
        # Determine market bias
        if long_stats["total_pnl"] > short_stats["total_pnl"] * 1.5:
            market_bias = "BULLISH"
        elif short_stats["total_pnl"] > long_stats["total_pnl"] * 1.5:
            market_bias = "BEARISH"
        else:
            market_bias = "NEUTRAL"
        
        return {
            "long": long_stats,
            "short": short_stats,
            "market_bias": market_bias
        }
    
    # =========================================================================
    # EXIT REASON ATTRIBUTION
    # =========================================================================
    
    @staticmethod
    def analyze_by_exit_reason(trades: List[Dict]) -> Dict:
        """
        Analyze trade outcomes by exit reason.
        
        Exit reasons:
        - TAKE_PROFIT_1: First TP hit (partial)
        - TAKE_PROFIT_2: Second TP hit (full TP)
        - STOP_LOSS: Hit stop loss
        - TRAILING_STOP: Trailing stop triggered
        - MANUAL: Manually closed
        - LIQUIDATION: Position liquidated
        """
        exit_stats = defaultdict(lambda: {"count": 0, "pnl": 0, "wins": 0})
        
        for trade in trades:
            reason = trade.get("exit_reason", "UNKNOWN")
            pnl = trade.get("realized_pnl", 0)
            
            exit_stats[reason]["count"] += 1
            exit_stats[reason]["pnl"] += pnl
            if pnl > 0:
                exit_stats[reason]["wins"] += 1
        
        result = {}
        for reason, stats in exit_stats.items():
            count = stats["count"]
            result[reason] = {
                "count": count,
                "total_pnl": round(stats["pnl"], 2),
                "avg_pnl": round(stats["pnl"] / count, 4) if count > 0 else 0,
                "win_rate": round(stats["wins"] / count * 100, 1) if count > 0 else 0,
                "pct_of_total": round(count / len(trades) * 100, 1) if trades else 0
            }
        
        return dict(sorted(result.items(), key=lambda x: x[1]["count"], reverse=True))
    
    # =========================================================================
    # SIGNAL SOURCE ATTRIBUTION
    # =========================================================================
    
    @staticmethod
    def analyze_by_signal_source(trades: List[Dict]) -> Dict:
        """
        Analyze performance by signal source/strategy.
        
        Identifies which components of the strategy are working.
        """
        source_stats = defaultdict(lambda: {"count": 0, "pnl": 0, "wins": 0})
        
        for trade in trades:
            # Extract strategy/signal info if available
            source = trade.get("strategy", trade.get("signal_source", "LIMIT_SNIPER"))
            pnl = trade.get("realized_pnl", 0)
            
            source_stats[source]["count"] += 1
            source_stats[source]["pnl"] += pnl
            if pnl > 0:
                source_stats[source]["wins"] += 1
        
        result = {}
        for source, stats in source_stats.items():
            count = stats["count"]
            result[source] = {
                "count": count,
                "total_pnl": round(stats["pnl"], 2),
                "avg_pnl": round(stats["pnl"] / count, 4) if count > 0 else 0,
                "win_rate": round(stats["wins"] / count * 100, 1) if count > 0 else 0
            }
        
        return result
    
    # =========================================================================
    # COMPREHENSIVE SIGNAL REPORT
    # =========================================================================
    
    @staticmethod
    def generate_signal_report(trades: List[Dict]) -> Dict:
        """
        Generate comprehensive signal attribution report.
        """
        confidence = SignalAnalyzer.analyze_by_confidence(trades)
        direction = SignalAnalyzer.analyze_by_direction(trades)
        exit_reasons = SignalAnalyzer.analyze_by_exit_reason(trades)
        sources = SignalAnalyzer.analyze_by_signal_source(trades)
        
        return {
            "confidence_attribution": confidence,
            "direction_attribution": direction,
            "exit_reason_attribution": exit_reasons,
            "signal_source_attribution": sources,
            "total_trades_analyzed": len(trades),
            "generated_at": datetime.utcnow().isoformat()
        }
