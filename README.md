# Wiii Analytics System

Trading performance analytics for Wiii Trader.

## Features

- **Time Analysis**: Hourly and daily P&L breakdown
- **Symbol Analysis**: Per-token performance metrics
- **Fee Analysis**: Commission and funding impact
- **Risk Metrics**: Sharpe, Sortino, Drawdown, Profit Factor
- **Market Regime**: Volatility, Trend, Performance detection

## Deployment

This service is designed to run on Render.com Free Tier.

### External Services

1. **Supabase** (PostgreSQL) - Free 500MB database
2. **UptimeRobot** - Ping every 10 min to prevent cold start
3. **cron-job.org** - Trigger /collect every hour

### Environment Variables

```
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_KEY=your-anon-key
BINANCE_API_KEY=your-api-key
BINANCE_API_SECRET=your-api-secret
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/collect` | POST | Trigger data collection |
| `/analytics/daily` | GET | Daily P&L |
| `/analytics/hourly` | GET | Hourly performance |
| `/analytics/symbols` | GET | Per-symbol stats |
| `/analytics/fees` | GET | Fee breakdown |
| `/analytics/risk` | GET | Risk metrics |
| `/analytics/regime` | GET | Market regime |
| `/analytics/full-report` | GET | Complete report |

## Local Development

```bash
pip install -r requirements.txt
python -m src.api.main
```

## Project Structure

```
wiii-analytics/
├── src/
│   ├── api/main.py         # FastAPI app
│   ├── collectors/         # Binance data fetching
│   ├── analyzers/          # Time/Symbol/Risk analysis
│   └── database/           # Supabase client
├── requirements.txt
├── render.yaml
└── README.md
```
