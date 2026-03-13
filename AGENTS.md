# AGENTS.md

## Cursor Cloud specific instructions

### Project overview

VIP客服日报系统 — 从网易七鱼 (Qiyu) OpenAPI 拉取 VIP 工单数据，自动生成标准化日报（TXT/Excel/PDF），提供 Flask Web 仪表盘。

### Running services

| Service | Command | Default Port |
|---------|---------|-------------|
| Web dashboard | `python app.py --port 5001` | 5001 |
| CLI report | `python main.py --date YYYY-MM-DD` | N/A |
| Scheduler | `python scheduler.py` | N/A |

### Tests

```bash
python3 -m pytest tests/ -v
```

There are 2 known pre-existing test failures in `tests/test_report_builder.py` (`test_suggestion` and `test_gameplay`) where test expectations don't match the combined category name `玩法咨询/游戏建议` returned by `classify_ticket`. These are not environment issues.

### Lint

No dedicated linter is configured in this project. Code is pure Python 3.12+.

### Gotchas

- The Flask app uses `sys.stdout.reconfigure(encoding='utf-8')` at the top of `app.py`; this can fail if stdout is not a text stream (e.g. piped to a non-UTF-8 sink).
- Qiyu API credentials are hardcoded as defaults in `config.py`. Override via `QIYU_APP_KEY` / `QIYU_APP_SECRET` env vars for different environments.
- Optional features (LLM classification, alerts, scheduling, web auth) are all disabled by default and controlled via environment variables. See `config.py` for the full list.
- SQLite cache (`cache/tickets.db`) and report output (`output/`) directories are auto-created on import of `config.py`.
- PDF generation requires CJK system fonts (e.g. `wqy-microhei`) for Chinese character rendering. Without them, PDF output will lack Chinese text.
