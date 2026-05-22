# NTC WatchDog

Health, monitoring, and remediation worker for NTC Newark services.

This project checks WebCall health, source-agent freshness, listener/HLS reachability, and level-monitor warnings. It can record incidents and restart configured Docker targets when remediation is enabled.

## Runtime

- Entry point: `ntc_watchdog.py`
- Runtime state lives under `data/` and is not committed
- Environment variables use the `NTC_WATCHDOG_*`, `NTC_ALERT_*`, and related `NTC_*` prefixes

## Local Validation

```bash
python3 -m py_compile ntc_watchdog.py ntc_watchdog_app.py
python3 -m pytest test_ntc_watchdog.py
```
