# Logging

The rules every module follows are in [`AGENTS.md`](../AGENTS.md#logging). The JSON-lines shape under systemd and the `journalctl` commands are in [`DEPLOYMENT.md`](DEPLOYMENT.md#logging). This doc holds the uvicorn detail behind the `--log-config` rule.

## uvicorn log routing

- **uvicorn:** every uvicorn invocation (systemd `ExecStart` + all dev-server commands) must pass `--log-config src/wslcb_licensing_tracker/log_config.json`, or uvicorn's own `uvicorn`/`uvicorn.access`/`uvicorn.error` lines bypass the JSON formatter and journald gets mixed plain-text/JSON logs (#162). That file and `setup_logging()` share one formatter via `build_json_formatter()` — change the JSON schema in that factory only. `log_config.json` also attaches `ColorMessageFilter` to every uvicorn logger, stripping uvicorn's ANSI-coloured `color_message` extra at the record (#163) — keep it on the *loggers*, not the handler or the formatter's `reserved_attrs`: a filter placed on one sink is resurrected by the next one, and a logger never inherits an ancestor's filters.
