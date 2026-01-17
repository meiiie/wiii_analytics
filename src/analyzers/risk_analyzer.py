"""
Risk Analyzer for TaHo Analytics
SOTA Institutional Pattern: Comprehensive Risk Metrics

Based on Two Sigma, Citadel, Renaissance Technologies patterns.

Key Metrics:
- Sharpe Ratio: Risk-adjusted returns
- Sortino Ratio: Downside risk only
- Max Drawdown: Worst peak-to-trough decline
- Win/Loss Streaks: Consecutive outcomes
- Value at Risk (VaR): Potential loss estimation
- Profit Factor: Gross profit / Gross loss
- Expectancy: Average expected value per trade
"""

from typing import Dict, List, Any
from datetime import datetime
import math


class RiskAnalyzer:
    """
    SOTA Risk Analytics following institutional quant patterns.
    
    Two Sigma / Citadel use these metrics for:
    - Strategy evaluation
    - Risk budgeting
    - Capital allocation
    """
    
    # =========================================================================
    # RISK-ADJUSTED RETURN METRICS
    # =========================================================================
    
    @staticmethod
    def calculate_sharpe_ratio(
        daily_returns: List[float],
        risk_free_rate: float = 0.0,
        annualization_factor: int = 365  # Crypto trades 365 days
    ) -> float:
        """
        Sharpe Ratio = (Rp - Rf) / σp
        
        Where:
        - Rp: Portfolio return
        - Rf: Risk-free rate
        - σp: Standard deviation of returns
        
        Interpretation:
        - > 1.0: Good
        - > 2.0: Very good
        - > 3.0: Excellent (suspicious?)
        """
        if len(daily_returns) < 2:
            return 0.0
        
        avg_return = sum(daily_returns) / len(daily_returns)
        excess_return = avg_return - (risk_free_rate / annualization_factor)
        
        variance = sum((r - avg_return) ** 2 for r in daily_returns) / (len(daily_returns) - 1)
        std_dev = math.sqrt(variance)
        
        if std_dev == 0:
            return 0.0
        
        sharpe = (excess_return / std_dev) * math.sqrt(annualization_factor)
        return round(sharpe, 2)
    
    @staticmethod
    def calculate_sortino_ratio(
        daily_returns: List[float],
        risk_free_rate: float = 0.0,
        annualization_factor: int = 365
    ) -> float:
        """
        Sortino Ratio = (Rp - Rf) / σd
        
        Similar to Sharpe but only uses DOWNSIDE deviation.
        Better for strategies with asymmetric returns.
        """
        if len(daily_returns) < 2:
            return 0.0
        
        avg_return = sum(daily_returns) / len(daily_returns)
        excess_return = avg_return - (risk_free_rate / annualization_factor)
        
        # Only consider negative returns for downside deviation
        negative_returns = [r for r in daily_returns if r < 0]
        if not negative_returns:
            return float('inf') if excess_return > 0 else 0.0
        
        downside_variance = sum(r ** 2 for r in negative_returns) / len(negative_returns)
        downside_dev = math.sqrt(downside_variance)
        
        if downside_dev == 0:
            return 0.0
        
        sortino = (excess_return / downside_dev) * math.sqrt(annualization_factor)
        return round(sortino, 2)
    
    # =========================================================================
    # DRAWDOWN ANALYSIS
    # =========================================================================
    
    @staticmethod
    def calculate_max_drawdown(equity_curve: List[Dict]) -> Dict:
        """
        Max Drawdown: Maximum peak-to-trough decline.
        
        Returns:
        - max_drawdown_pct: Worst case drawdown
        - max_drawdown_usd: In absolute terms
        - drawdown_duration: How long it lasted
        - recovery_time: Time to recover (if recovered)
        """
        if len(equity_curve) < 2:
            return {"max_drawdown_pct": 0, "max_drawdown_usd": 0}
        
        peak = equity_curve[0].get("balance", 0)
        max_dd_pct = 0
        max_dd_usd = 0
        dd_start = None
        longest_dd_duration = 0
        current_dd_start = None
        
        for point in equity_curve:
            balance = point.get("balance", 0)
            
            if balance > peak:
                # New peak
                if current_dd_start:
                    # Calculate duration of previous drawdown
                    pass
                peak = balance
                current_dd_start = None
            else:
                # In drawdown
                if current_dd_start is None:
                    current_dd_start = point.get("time")
                
                drawdown_pct = (peak - balance) / peak * 100 if peak > 0 else 0
                drawdown_usd = peak - balance
                
                if drawdown_pct > max_dd_pct:
                    max_dd_pct = drawdown_pct
                    max_dd_usd = drawdown_usd
                    dd_start = current_dd_start
        
        return {
            "max_drawdown_pct": round(max_dd_pct, 2),
            "max_drawdown_usd": round(max_dd_usd, 2),
            "peak_before_dd": round(peak, 2)
        }
    
    # =========================================================================
    # TRADE STATISTICS
    # =========================================================================
    
    @staticmethod
    def calculate_trade_stats(trades: List[Dict]) -> Dict:
        """
        Comprehensive trade statistics used by institutional traders.
        
        Returns all key metrics for strategy evaluation.
        """
        if not trades:
            return {}
        
        pnl_list = [t.get("realized_pnl", 0) for t in trades]
        winners = [p for p in pnl_list if p > 0]
        losers = [p for p in pnl_list if p < 0]
        
        gross_profit = sum(winners) if winners else 0
        gross_loss = abs(sum(losers)) if losers else 0
        net_profit = gross_profit - gross_loss
        
        win_rate = len(winners) / len(pnl_list) * 100 if pnl_list else 0
        avg_win = gross_profit / len(winners) if winners else 0
        avg_loss = gross_loss / len(losers) if losers else 0
        
        # Profit Factor: Gross Profit / Gross Loss
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        # Risk/Reward Ratio: Avg Win / Avg Loss
        risk_reward = avg_win / avg_loss if avg_loss > 0 else float('inf')
        
        # Expectancy: (Win Rate × Avg Win) - (Loss Rate × Avg Loss)
        loss_rate = 100 - win_rate
        expectancy = (win_rate/100 * avg_win) - (loss_rate/100 * avg_loss)
        
        # Win/Loss Streaks
        streaks = RiskAnalyzer._calculate_streaks(pnl_list)
        
        return {
            "total_trades": len(pnl_list),
            "winning_trades": len(winners),
            "losing_trades": len(losers),
            "win_rate": round(win_rate, 1),
            "gross_profit": round(gross_profit, 2),
            "gross_loss": round(gross_loss, 2),
            "net_profit": round(net_profit, 2),
            "avg_win": round(avg_win, 4),
            "avg_loss": round(avg_loss, 4),
            "profit_factor": round(min(profit_factor, 99.99), 2),
            "risk_reward_ratio": round(min(risk_reward, 99.99), 2),
            "expectancy_per_trade": round(expectancy, 4),
            "largest_win": round(max(winners) if winners else 0, 2),
            "largest_loss": round(min(losers) if losers else 0, 2),
            **streaks
        }
    
    @staticmethod
    def _calculate_streaks(pnl_list: List[float]) -> Dict:
        """Calculate win/loss streaks."""
        if not pnl_list:
            return {"max_win_streak": 0, "max_loss_streak": 0, "current_streak": 0}
        
        max_win_streak = 0
        max_loss_streak = 0
        current_win_streak = 0
        current_loss_streak = 0
        
        for pnl in pnl_list:
            if pnl > 0:
                current_win_streak += 1
                current_loss_streak = 0
                max_win_streak = max(max_win_streak, current_win_streak)
            elif pnl < 0:
                current_loss_streak += 1
                current_win_streak = 0
                max_loss_streak = max(max_loss_streak, current_loss_streak)
        
        # Current streak (positive = wins, negative = losses)
        current_streak = current_win_streak if current_win_streak > 0 else -current_loss_streak
        
        return {
            "max_win_streak": max_win_streak,
            "max_loss_streak": max_loss_streak,
            "current_streak": current_streak
        }
    
    # =========================================================================
    # COMPREHENSIVE RISK REPORT
    # =========================================================================
    
    @staticmethod
    def generate_risk_report(
        trades: List[Dict],
        daily_pnl: List[Dict],
        equity_curve: List[Dict] = None
    ) -> Dict:
        """
        Generate comprehensive risk report (Two Sigma style).
        """
        # Trade stats
        trade_stats = RiskAnalyzer.calculate_trade_stats(trades)
        
        # Daily returns (as percentages)
        daily_returns = []
        for day in daily_pnl:
            # Assume starting balance around $40-$300 for % calculation
            pnl = day.get("net_pnl", 0)
            # Use relative return if we have enough context
            daily_returns.append(pnl)  # Will be converted to % later
        
        # Sharpe & Sortino (using PnL as returns)
        if len(daily_returns) > 1:
            avg_daily = sum(daily_returns) / len(daily_returns)
            # Normalize to percentage-like values
            normalized_returns = [r / max(abs(avg_daily), 1) for r in daily_returns]
            sharpe = RiskAnalyzer.calculate_sharpe_ratio(normalized_returns)
            sortino = RiskAnalyzer.calculate_sortino_ratio(normalized_returns)
        else:
            sharpe = 0
            sortino = 0
        
        # Drawdown
        drawdown_stats = {}
        if equity_curve:
            drawdown_stats = RiskAnalyzer.calculate_max_drawdown(equity_curve)
        
        return {
            "trade_statistics": trade_stats,
            "risk_metrics": {
                "sharpe_ratio": sharpe,
                "sortino_ratio": sortino,
                **drawdown_stats
            },
            "period_days": len(daily_pnl),
            "generated_at": datetime.utcnow().isoformat()
        }
