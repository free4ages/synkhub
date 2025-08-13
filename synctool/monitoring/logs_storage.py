import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import logging


class LogsStorage:
    """File-based storage for job run logs (JSON lines per run)."""

    def __init__(self, logs_dir: str = "./data/logs", max_runs_per_job: int = 50):
        self.logs_dir = Path(logs_dir)
        self.max_runs_per_job = max_runs_per_job
        self.logger = logging.getLogger(__name__)

        # Create logs directory if it doesn't exist
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def _job_dir(self, job_name: str) -> Path:
        job_dir = self.logs_dir / job_name
        job_dir.mkdir(exist_ok=True)
        return job_dir

    def _run_file(self, job_name: str, run_id: str) -> Path:
        return self._job_dir(job_name) / f"{run_id}.log"

    def append_log(self, job_name: str, run_id: str, timestamp: datetime, level: str, message: str, logger_name: Optional[str] = None) -> None:
        """Append a single log entry for a given run as a JSON line."""
        entry: Dict[str, Any] = {
            "timestamp": timestamp.isoformat(),
            "level": level,
            "message": message,
            "run_id": run_id,
        }
        if logger_name:
            entry["logger"] = logger_name

        run_file = self._run_file(job_name, run_id)
        try:
            with open(run_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            self.logger.warning(f"Failed to append log for {job_name}:{run_id}: {e}")

        # Optionally cleanup old runs to cap storage usage
        self._cleanup_old_runs(job_name)

    def get_run_logs(self, job_name: str, run_id: str, level: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Read logs for a specific run.

        Returns list of dicts with keys: timestamp (datetime), level, message, run_id.
        """
        run_file = self._run_file(job_name, run_id)
        if not run_file.exists():
            return []

        logs: List[Dict[str, Any]] = []
        try:
            with open(run_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = json.loads(line)
                        if level and data.get("level") != level.upper():
                            continue
                        # Convert timestamp to datetime object
                        if isinstance(data.get("timestamp"), str):
                            data["timestamp"] = datetime.fromisoformat(data["timestamp"])  # type: ignore
                        logs.append(data)
                    except Exception as parse_err:
                        self.logger.debug(f"Skipping malformed log line in {run_file}: {parse_err}")
        except Exception as e:
            self.logger.error(f"Failed to read logs for {job_name}:{run_id}: {e}")
            return []

        # Apply limit (most recent last; entries are in append order)
        if limit is not None and limit >= 0:
            logs = logs[-limit:]

        return logs

    def _cleanup_old_runs(self, job_name: str) -> None:
        """Keep only the latest N runs by modification time for a job."""
        job_dir = self._job_dir(job_name)

        run_files = sorted(
            job_dir.glob("*.log"),
            key=lambda x: x.stat().st_mtime,
            reverse=True,
        )

        if len(run_files) > self.max_runs_per_job:
            for old_file in run_files[self.max_runs_per_job:]:
                try:
                    old_file.unlink()
                    self.logger.debug(f"Removed old log file: {old_file}")
                except Exception as e:
                    self.logger.warning(f"Failed to remove old log file {old_file}: {e}")


