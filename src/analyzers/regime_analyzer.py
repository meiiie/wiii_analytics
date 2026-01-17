"""
Market Regime Detector for TaHo Analytics
SOTA Institutional Pattern: Regime Detection

Based on Renaissance Technologies, AQR regime analysis.

Key Concepts:
- Volatility Regime: High/Normal/Low volatility periods
- Trend Regime: Trending/Ranging market states
- Correlation Regime: Risk-on/Risk-off conditions
"""

from typing import Dict, List, Any
from datetime import datetime
from collections import defaultdict
import math


class RegimeAnalyzer:
    """
    SOTA Market Regime Detection.
    
    Renaissance Technologies uses Hidden Markov Models (HMM).
    This is a simplified version using statistical thresholds.
    
    Purpose:
    - Identify current market conditions
    - Adjust strategy parameters dynamically
    - Explain performance variations
    """
    
    # =========================================================================
    # VOLATILITY REGIME
    # =========================================================================
    
    @staticmethod
    def detect_volatility_regime(daily_pnl: List[Dict]) -> Dict:
        """
        Detect volatility regime from daily P&L variance.
        
        Regimes:
        - LOW_VOL: σ < 1% daily
        - NORMAL_VOL: 1% <= σ < 3%
        - HIGH_VOL: σ >= 3%
        """
        if len(daily_pnl) < 5:
            return {"regime": "INSUFFICIENT_DATA", "volatility": 0}
        
        # Calculate daily returns variance
        pnl_values = [d.get("net_pnl", 0) for d in daily_pnl]
        
        mean_pnl = sum(pnl_values) / len(pnl_values)
        variance = sum((p - mean_pnl) ** 2 for p in pnl_values) / (len(pnl_values) - 1)
        std_dev = math.sqrt(variance)
        
        # Normalization: assume ~$100 capital for percentage
        base_capital = 100
        volatility_pct = (std_dev / base_capital) * 100 if base_capital > 0 else 0
        
        # Classify regime
        if volatility_pct < 1:
            regime = "LOW_VOLATILITY"
            description = "Calm market, smaller moves"
        elif volatility_pct < 3:
            regime = "NORMAL_VOLATILITY"
            description = "Standard market conditions"
        else:
            regime = "HIGH_VOLATILITY"
            description = "Elevated volatility, larger moves"
        
        return {
            "regime": regime,
            "volatility_pct": round(volatility_pct, 2),
            "std_dev_usd": round(std_dev, 2),
            "description": description,
            "days_analyzed": len(pnl_values)
        }
    
    # =========================================================================
    # TREND REGIME
    # =========================================================================
    
    @staticmethod
    def detect_trend_regime(daily_pnl: List[Dict]) -> Dict:
        """
        Detect if strategy is in trending or ranging mode.
        
        Based on consistency of P&L direction.
        
        Regimes:
        - STRONG_TREND: >70% days same direction
        - WEAK_TREND: 55-70% days same direction
        - RANGING: <55% days same direction
        """
        if len(daily_pnl) < 5:
            return {"regime": "INSUFFICIENT_DATA"}
        
        pnl_values = [d.get("net_pnl", 0) for d in daily_pnl]
        
        positive_days = sum(1 for p in pnl_values if p > 0)
        negative_days = sum(1 for p in pnl_values if p < 0)
        total_directional = positive_days + negative_days
        
        if total_directional == 0:
            return {"regime": "FLAT", "direction": "NEUTRAL"}
        
        # Calculate direction bias
        dominant_direction = "BULLISH" if positive_days > negative_days else "BEARISH"
        consistency = max(positive_days, negative_days) / total_directional * 100
        
        # Classify trend strength
        if consistency >= 70:
            regime = "STRONG_TREND"
        elif consistency >= 55:
            regime = "WEAK_TREND"
        else:
            regime = "RANGING"
        
        # Calculate cumulative trend
        cumulative_pnl = sum(pnl_values)
        trend_direction = "UP" if cumulative_pnl > 0 else "DOWN" if cumulative_pnl < 0 else "FLAT"
        
        return {
            "regime": regime,
            "direction": dominant_direction,
            "trend_direction": trend_direction,
            "consistency_pct": round(consistency, 1),
            "positive_days": positive_days,
            "negative_days": negative_days,
            "cumulative_pnl": round(cumulative_pnl, 2)
        }
    
    # =========================================================================
    # WIN RATE REGIME
    # =========================================================================
    
    @staticmethod
    def detect_performance_regime(trades: List[Dict], window: int = 20) -> Dict:
        """
        Detect if strategy is in hot/cold streak.
        
        Rolling win rate analysis:
        - HOT_STREAK: Recent win rate > historical by 15%+
        - NORMAL: Within 15% of historical
        - COLD_STREAK: Recent win rate < historical by 15%+
        """
        if len(trades) < window:
            return {"regime": "INSUFFICIENT_DATA"}
        
        # Historical win rate (all trades)
        all_wins = sum(1 for t in trades if t.get("realized_pnl", 0) > 0)
        historical_wr = all_wins / len(trades) * 100
        
        # Recent win rate (last N trades)
        recent_trades = trades[-window:]
        recent_wins = sum(1 for t in recent_trades if t.get("realized_pnl", 0) > 0)
        recent_wr = recent_wins / len(recent_trades) * 100
        
        # Calculate deviation
        deviation = recent_wr - historical_wr
        
        if deviation > 15:
            regime = "HOT_STREAK"
            action = "Consider reducing position size (reversion expected)"
        elif deviation < -15:
            regime = "COLD_STREAK"
            action = "Review strategy or wait for normalization"
        else:
            regime = "NORMAL"
            action = "Continue standard operation"
        
        return {
            "regime": regime,
            "historical_win_rate": round(historical_wr, 1),
            "recent_win_rate": round(recent_wr, 1),
            "deviation_pct": round(deviation, 1),
            "window_size": window,
            "recommended_action": action
        }
    
    # =========================================================================
    # COMPREHENSIVE REGIME REPORT
    # =========================================================================
    
    @staticmethod
    def generate_regime_report(
        daily_pnl: List[Dict],
        trades: List[Dict]
    ) -> Dict:
        """
        Generate comprehensive market regime report.
        """
        volatility = RegimeAnalyzer.detect_volatility_regime(daily_pnl)
        trend = RegimeAnalyzer.detect_trend_regime(daily_pnl)
        performance = RegimeAnalyzer.detect_performance_regime(trades)
        
        # Overall market assessment
        regimes = [volatility.get("regime"), trend.get("regime"), performance.get("regime")]
        
        if "HIGH_VOLATILITY" in regimes or "COLD_STREAK" in regimes:
            overall_risk = "ELEVATED"
            overall_action = "Consider reducing exposure"
        elif "LOW_VOLATILITY" in regimes and "HOT_STREAK" in regimes:
            overall_risk = "LOW"
            overall_action = "Favorable conditions"
        else:
            overall_risk = "NORMAL"
            overall_action = "Standard operation"
        
        return {
            "volatility_regime": volatility,
            "trend_regime": trend,
            "performance_regime": performance,
            "overall_assessment": {
                "risk_level": overall_risk,
                "recommended_action": overall_action
            },
            "generated_at": datetime.utcnow().isoformat()
        }
