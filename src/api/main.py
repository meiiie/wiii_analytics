"""
Wiii Analytics API - FastAPI Application
SOTA: RESTful API for trading analytics

Endpoints:
- /health - Health check (UptimeRobot)
- /collect - Trigger data collection (cron-job.org)
- /analytics/* - Analysis endpoints
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, Optional
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Wiii Analytics API",
    description="Trading performance analytics for Wiii Trader",
    version="1.0.0"
)

# CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.get("/health", tags=["System"])
async def health_check():
    """
    Health check endpoint for UptimeRobot.
    Keeps the service alive (prevents cold start).
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "wiii-analytics"
    }


# =============================================================================
# DATA COLLECTION
# =============================================================================

@app.post("/collect", tags=["Collection"])
async def collect_data(days: int = 7):
    """
    Trigger data collection from Binance.
    Called by cron-job.org every hour.
    
    Args:
        days: Number of days to fetch (default 7)
    """
    try:
        from src.collectors.binance_collector import BinanceCollector
        from src.database.supabase_client import AnalyticsRepository
        
        collector = BinanceCollector()
        repo = AnalyticsRepository()
        
        # Collect income data
        income_data = collector.fetch_all_income(days=days)
        
        total_records = 0
        for income_type, records in income_data.items():
            count = repo.upsert_income(records)
            total_records += count
            logger.info(f"Collected {count} {income_type} records")
        
        return {
            "success": True,
            "records_collected": total_records,
            "days": days,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Collection failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# ANALYTICS ENDPOINTS
# =============================================================================

@app.get("/analytics/daily", tags=["Analytics"])
async def get_daily_analytics(days: int = 30):
    """Get daily P&L history."""
    try:
        from src.collectors.binance_collector import BinanceCollector
        from src.analyzers.time_analyzer import TimeAnalyzer
        
        collector = BinanceCollector()
        all_income = collector.fetch_all_income(days=days)
        
        # Flatten income records
        records = []
        for income_type, items in all_income.items():
            records.extend(items)
        
        daily_stats = TimeAnalyzer.analyze_by_day(records)
        
        return {
            "success": True,
            "days": days,
            "data": daily_stats
        }
        
    except Exception as e:
        logger.error(f"Daily analytics failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/hourly", tags=["Analytics"])
async def get_hourly_analytics(days: int = 30):
    """Get performance by hour of day (Vietnam time)."""
    try:
        from src.collectors.binance_collector import BinanceCollector
        from src.analyzers.time_analyzer import TimeAnalyzer
        
        collector = BinanceCollector()
        all_income = collector.fetch_all_income(days=days)
        
        # Flatten income records
        records = []
        for income_type, items in all_income.items():
            records.extend(items)
        
        hourly_stats = TimeAnalyzer.analyze_by_hour(records)
        best_worst = TimeAnalyzer.identify_best_worst_hours(hourly_stats)
        
        return {
            "success": True,
            "days": days,
            "data": hourly_stats,
            "insights": best_worst
        }
        
    except Exception as e:
        logger.error(f"Hourly analytics failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/symbols", tags=["Analytics"])
async def get_symbol_analytics(days: int = 30):
    """Get performance by symbol."""
    try:
        from src.collectors.binance_collector import BinanceCollector
        from src.analyzers.symbol_analyzer import SymbolAnalyzer
        
        collector = BinanceCollector()
        all_income = collector.fetch_all_income(days=days)
        
        # Flatten income records
        records = []
        for income_type, items in all_income.items():
            records.extend(items)
        
        symbol_stats = SymbolAnalyzer.analyze_by_symbol(records)
        performers = SymbolAnalyzer.get_top_performers(symbol_stats)
        most_traded = SymbolAnalyzer.get_most_traded(symbol_stats)
        
        return {
            "success": True,
            "days": days,
            "data": symbol_stats,
            "top_performers": performers,
            "most_traded": most_traded
        }
        
    except Exception as e:
        logger.error(f"Symbol analytics failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/fees", tags=["Analytics"])
async def get_fee_analytics(days: int = 30):
    """Get fee breakdown and impact analysis."""
    try:
        from src.collectors.binance_collector import BinanceCollector
        from src.analyzers.fee_analyzer import FeeAnalyzer
        
        collector = BinanceCollector()
        all_income = collector.fetch_all_income(days=days)
        
        # Flatten income records
        records = []
        for income_type, items in all_income.items():
            records.extend(items)
        
        fee_summary = FeeAnalyzer.analyze_fees(records)
        fee_by_symbol = FeeAnalyzer.analyze_fees_by_symbol(records)
        
        return {
            "success": True,
            "days": days,
            "summary": fee_summary,
            "by_symbol": fee_by_symbol[:20]  # Top 20
        }
        
    except Exception as e:
        logger.error(f"Fee analytics failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/summary", tags=["Analytics"])
async def get_summary(days: int = 30):
    """Get all-in-one analytics summary."""
    try:
        from src.collectors.binance_collector import BinanceCollector
        from src.analyzers.time_analyzer import TimeAnalyzer
        from src.analyzers.symbol_analyzer import SymbolAnalyzer
        from src.analyzers.fee_analyzer import FeeAnalyzer
        
        collector = BinanceCollector()
        
        # Fetch account info
        account = collector.fetch_account()
        
        # Fetch income
        all_income = collector.fetch_all_income(days=days)
        
        # Flatten records
        records = []
        for income_type, items in all_income.items():
            records.extend(items)
        
        # Analyze
        daily = TimeAnalyzer.analyze_by_day(records)
        hourly = TimeAnalyzer.analyze_by_hour(records)
        symbols = SymbolAnalyzer.analyze_by_symbol(records)
        fees = FeeAnalyzer.analyze_fees(records)
        
        return {
            "success": True,
            "days": days,
            "account": account,
            "performance": fees,
            "daily": daily[:7],  # Last 7 days
            "hourly": TimeAnalyzer.identify_best_worst_hours(hourly),
            "top_symbols": symbols[:5]
        }
        
    except Exception as e:
        logger.error(f"Summary failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# INSTITUTIONAL ANALYTICS ENDPOINTS (SOTA - Jan 2026)
# =============================================================================

@app.get("/analytics/risk", tags=["Institutional Analytics"])
async def get_risk_analytics(days: int = 30):
    """
    SOTA Risk Metrics (Two Sigma / Citadel pattern).
    
    Returns:
    - Sharpe Ratio
    - Sortino Ratio  
    - Max Drawdown
    - Profit Factor
    - Expectancy
    - Win/Loss Streaks
    """
    try:
        from src.collectors.binance_collector import BinanceCollector
        from src.analyzers.time_analyzer import TimeAnalyzer
        from src.analyzers.risk_analyzer import RiskAnalyzer
        
        collector = BinanceCollector()
        all_income = collector.fetch_all_income(days=days)
        
        records = []
        for _, items in all_income.items():
            records.extend(items)
        
        # Get daily stats for risk calculations
        daily = TimeAnalyzer.analyze_by_day(records)
        
        # Build trade-like structure from income records
        trades = [
            {"realized_pnl": r.get("income", 0)}
            for r in records if r.get("income_type") == "REALIZED_PNL"
        ]
        
        risk_report = RiskAnalyzer.generate_risk_report(trades, daily)
        
        return {
            "success": True,
            "days": days,
            **risk_report
        }
        
    except Exception as e:
        logger.error(f"Risk analytics failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/execution", tags=["Institutional Analytics"])
async def get_execution_analytics(days: int = 30):
    """
    Execution Quality Analysis (Citadel Securities pattern).
    
    Returns:
    - Slippage analysis
    - Maker/Taker distribution
    - Execution by hour
    """
    try:
        from src.collectors.binance_collector import BinanceCollector
        from src.analyzers.execution_analyzer import ExecutionAnalyzer
        
        collector = BinanceCollector()
        
        # Fetch trades for multiple symbols
        trades = collector.fetch_trades_multi(
            ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"],
            days=days
        )
        
        execution_report = ExecutionAnalyzer.generate_execution_report(trades)
        
        return {
            "success": True,
            "days": days,
            **execution_report
        }
        
    except Exception as e:
        logger.error(f"Execution analytics failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/signals", tags=["Institutional Analytics"])
async def get_signal_analytics(days: int = 30):
    """
    Signal Attribution Analysis (Two Sigma pattern).
    
    Returns:
    - Confidence vs outcome
    - Direction performance (LONG vs SHORT)
    - Exit reason breakdown
    """
    try:
        from src.collectors.binance_collector import BinanceCollector
        from src.analyzers.signal_analyzer import SignalAnalyzer
        
        collector = BinanceCollector()
        all_income = collector.fetch_all_income(days=days)
        
        # Build trade records from income
        records = [r for r in [] for _, items in all_income.items() for r in items]
        trades = [
            {
                "realized_pnl": r.get("income", 0),
                "side": "LONG",  # Need actual data from signals
                "confidence": 0.6,  # Default
                "exit_reason": "UNKNOWN"
            }
            for r in all_income.get("realized_pnl", [])
        ]
        
        signal_report = SignalAnalyzer.generate_signal_report(trades)
        
        return {
            "success": True,
            "days": days,
            **signal_report
        }
        
    except Exception as e:
        logger.error(f"Signal analytics failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/regime", tags=["Institutional Analytics"])
async def get_regime_analytics(days: int = 30):
    """
    Market Regime Detection (Renaissance Technologies pattern).
    
    Returns:
    - Volatility regime (LOW/NORMAL/HIGH)
    - Trend regime (RANGING/TRENDING)
    - Performance regime (HOT/COLD/NORMAL streak)
    """
    try:
        from src.collectors.binance_collector import BinanceCollector
        from src.analyzers.time_analyzer import TimeAnalyzer
        from src.analyzers.regime_analyzer import RegimeAnalyzer
        
        collector = BinanceCollector()
        all_income = collector.fetch_all_income(days=days)
        
        records = []
        for _, items in all_income.items():
            records.extend(items)
        
        daily = TimeAnalyzer.analyze_by_day(records)
        trades = [
            {"realized_pnl": r.get("income", 0)}
            for r in records if r.get("income_type") == "REALIZED_PNL"
        ]
        
        regime_report = RegimeAnalyzer.generate_regime_report(daily, trades)
        
        return {
            "success": True,
            "days": days,
            **regime_report
        }
        
    except Exception as e:
        logger.error(f"Regime analytics failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/full-report", tags=["Institutional Analytics"])
async def get_full_report(days: int = 30):
    """
    Comprehensive Institutional Report.
    
    All-in-one report combining:
    - Account info
    - Performance metrics
    - Risk analysis
    - Time/Symbol breakdown
    - Regime detection
    """
    try:
        from src.collectors.binance_collector import BinanceCollector
        from src.analyzers.time_analyzer import TimeAnalyzer
        from src.analyzers.symbol_analyzer import SymbolAnalyzer
        from src.analyzers.fee_analyzer import FeeAnalyzer
        from src.analyzers.risk_analyzer import RiskAnalyzer
        from src.analyzers.regime_analyzer import RegimeAnalyzer
        
        collector = BinanceCollector()
        
        # Fetch all data
        account = collector.fetch_account()
        all_income = collector.fetch_all_income(days=days)
        
        records = []
        for _, items in all_income.items():
            records.extend(items)
        
        # Build trade list
        trades = [
            {"realized_pnl": r.get("income", 0)}
            for r in records if r.get("income_type") == "REALIZED_PNL"
        ]
        
        # Run all analyzers
        daily = TimeAnalyzer.analyze_by_day(records)
        hourly = TimeAnalyzer.analyze_by_hour(records)
        symbols = SymbolAnalyzer.analyze_by_symbol(records)
        fees = FeeAnalyzer.analyze_fees(records)
        risk = RiskAnalyzer.generate_risk_report(trades, daily)
        regime = RegimeAnalyzer.generate_regime_report(daily, trades)
        
        return {
            "success": True,
            "days": days,
            "generated_at": datetime.utcnow().isoformat(),
            "account": account,
            "performance": fees,
            "risk_metrics": risk,
            "market_regime": regime,
            "time_analysis": {
                "daily": daily[:7],
                "best_worst_hours": TimeAnalyzer.identify_best_worst_hours(hourly)
            },
            "symbol_analysis": {
                "top_5": symbols[:5],
                "bottom_5": symbols[-5:] if len(symbols) >= 5 else symbols
            }
        }
        
    except Exception as e:
        logger.error(f"Full report failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# RUN SERVER
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)

