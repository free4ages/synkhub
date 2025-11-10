import logging
from typing import Optional
from datetime import datetime

from .logs_storage import LogsStorage


class RunLogsHandler(logging.Handler):
    """Logging handler that forwards records to LogsStorage for a specific job run."""

    def __init__(self, logs_storage: LogsStorage, job_name: str, strategy_name: str, run_id: str):
        super().__init__()
        self.logs_storage = logs_storage
        self.job_name = job_name
        self.strategy_name = strategy_name
        self.run_id = run_id

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record) if self.formatter else record.getMessage()
            timestamp = datetime.fromtimestamp(record.created)
            level = record.levelname
            logger_name = record.name
            self.logs_storage.append_log(
                job_name=self.job_name,
                strategy_name=self.strategy_name,
                run_id=self.run_id,
                timestamp=timestamp,
                level=level,
                message=message,
                logger_name=logger_name,
            )
        except Exception:
            # Do not raise errors from logging handler to avoid breaking the main flow
            pass


class LoggingCollector:
    """Manages lifecycle of run-specific logging handler."""

    def __init__(self, logs_storage: LogsStorage):
        self.logs_storage = logs_storage
        self._handler: Optional[RunLogsHandler] = None

    def start_job_run(self, job_name: str, strategy_name: str, run_id: str, level: int = logging.DEBUG) -> None:
        self.stop_job_run()  # Ensure any existing handler is removed first
        handler = RunLogsHandler(self.logs_storage, job_name, strategy_name, run_id)
        handler.setLevel(level)
        # Simple formatter; message already includes logger/time via storage
        handler.setFormatter(logging.Formatter("%(message)s"))
        logging.getLogger().addHandler(handler)
        self._handler = handler

    def stop_job_run(self) -> None:
        if self._handler is not None:
            try:
                logging.getLogger().removeHandler(self._handler)
            except Exception:
                pass
            self._handler = None


