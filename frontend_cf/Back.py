from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from urllib import error as urlerror
from urllib import request as urlrequest
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field


BASE_DIR = Path(__file__).resolve().parent
OUTREACH_SCRIPT = BASE_DIR / "Outreach(1).py"
LOG_BUFFER_SIZE = 1000

app = FastAPI(
	title="Outreach FastAPI Backend",
	version="1.0.0",
	description="API endpoints to start and monitor Outreach(1).py runs.",
)

_state_lock = threading.Lock()
_process: subprocess.Popen | None = None
_run_id: str | None = None
_started_at: str | None = None
_finished_at: str | None = None
_exit_code: int | None = None
_csv_path: str | None = None
_logs: deque[str] = deque(maxlen=LOG_BUFFER_SIZE)


class OutreachStartRequest(BaseModel):
	csv_path: str | None = Field(
		default=None,
		description="Optional CSV path. Relative paths are resolved from project root.",
	)


def _utc_now_iso() -> str:
	return datetime.now(timezone.utc).isoformat()


def _append_log(line: str) -> None:
	clean = line.rstrip("\r\n")
	if not clean:
		return
	with _state_lock:
		_logs.append(clean)


def _stream_process_output(proc: subprocess.Popen) -> None:
	if proc.stdout is None:
		return
	for line in proc.stdout:
		_append_log(line)
	proc.stdout.close()


def _refresh_process_state() -> None:
	global _exit_code, _finished_at
	with _state_lock:
		if _process is None:
			return
		code = _process.poll()
		if code is None:
			return
		if _exit_code is None:
			_exit_code = int(code)
			_finished_at = _utc_now_iso()


def _resolve_csv_path(csv_path: str | None) -> str | None:
	if not csv_path:
		return None

	candidate = Path(csv_path).expanduser()
	if not candidate.is_absolute():
		candidate = (BASE_DIR / candidate).resolve()

	if not candidate.exists():
		raise HTTPException(status_code=400, detail=f"CSV file not found: {candidate}")
	if candidate.is_dir():
		raise HTTPException(status_code=400, detail=f"CSV path is a directory: {candidate}")

	return str(candidate)


def _validate_ping_url(url: str) -> str:
	parsed = urlparse(url)
	if parsed.scheme not in {"http", "https"} or not parsed.netloc:
		raise HTTPException(status_code=400, detail="Use a valid http/https URL")
	return url


@app.get("/")
def root() -> dict:
	return {
		"service": "Outreach FastAPI Backend",
		"docs": "/docs",
		"start_endpoint": "/outreach/start",
	}


@app.get("/health")
def health() -> dict:
	_refresh_process_state()
	return {"status": "ok"}


@app.get("/ping")
def ping() -> dict:
	_refresh_process_state()
	with _state_lock:
		running = _process is not None and _process.poll() is None
		return {
			"status": "ok",
			"checked_at": _utc_now_iso(),
			"outreach_running": running,
			"run_id": _run_id,
		}


@app.get("/endpoint/ping")
def ping_endpoint(
	url: str = Query(..., description="Full http/https URL to ping"),
	timeout: float = Query(default=8.0, ge=1.0, le=30.0),
) -> dict:
	target = _validate_ping_url(url)
	request = urlrequest.Request(target, method="GET", headers={"User-Agent": "OutreachFastAPI/1.0"})
	start = time.perf_counter()

	try:
		with urlrequest.urlopen(request, timeout=timeout) as response:
			status_code = int(response.status)
			reason = str(getattr(response, "reason", ""))
			ok = 200 <= status_code < 400
	except urlerror.HTTPError as exc:
		status_code = int(exc.code)
		reason = str(exc.reason)
		ok = False
	except Exception as exc:
		elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
		return {
			"target": target,
			"ok": False,
			"status_code": None,
			"reason": str(exc),
			"response_time_ms": elapsed_ms,
			"checked_at": _utc_now_iso(),
		}

	elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
	return {
		"target": target,
		"ok": ok,
		"status_code": status_code,
		"reason": reason,
		"response_time_ms": elapsed_ms,
		"checked_at": _utc_now_iso(),
	}


@app.post("/outreach/start")
def start_outreach(payload: OutreachStartRequest) -> dict:
	global _process, _run_id, _started_at, _finished_at, _exit_code, _csv_path

	if not OUTREACH_SCRIPT.exists():
		raise HTTPException(status_code=500, detail=f"Script not found: {OUTREACH_SCRIPT}")

	csv_arg = _resolve_csv_path(payload.csv_path)
	_refresh_process_state()

	with _state_lock:
		if _process is not None and _process.poll() is None:
			raise HTTPException(status_code=409, detail="Outreach run is already in progress")

		cmd = [sys.executable, str(OUTREACH_SCRIPT)]
		if csv_arg:
			cmd.append(csv_arg)

		try:
			proc = subprocess.Popen(
				cmd,
				cwd=str(BASE_DIR),
				stdout=subprocess.PIPE,
				stderr=subprocess.STDOUT,
				text=True,
				bufsize=1,
			)
		except Exception as exc:
			raise HTTPException(status_code=500, detail=f"Failed to start Outreach script: {exc}") from exc

		_process = proc
		_run_id = uuid.uuid4().hex[:12]
		_started_at = _utc_now_iso()
		_finished_at = None
		_exit_code = None
		_csv_path = csv_arg
		_logs.clear()
		_logs.append(f"[{_started_at}] Started: {' '.join(cmd)}")

		reader = threading.Thread(target=_stream_process_output, args=(proc,), daemon=True)
		reader.start()

		return {
			"status": "started",
			"run_id": _run_id,
			"pid": proc.pid,
			"csv_path": _csv_path,
			"started_at": _started_at,
		}


@app.get("/outreach/status")
def outreach_status() -> dict:
	_refresh_process_state()
	with _state_lock:
		running = _process is not None and _process.poll() is None
		return {
			"running": running,
			"run_id": _run_id,
			"pid": _process.pid if _process else None,
			"csv_path": _csv_path,
			"started_at": _started_at,
			"finished_at": _finished_at,
			"exit_code": _exit_code,
		}


@app.get("/outreach/logs")
def outreach_logs(tail: int = Query(default=200, ge=1, le=1000)) -> dict:
	_refresh_process_state()
	with _state_lock:
		lines = list(_logs)[-tail:]
		return {
			"run_id": _run_id,
			"line_count": len(lines),
			"logs": lines,
		}


@app.post("/outreach/stop")
def stop_outreach() -> dict:
	_refresh_process_state()

	with _state_lock:
		if _process is None or _process.poll() is not None:
			raise HTTPException(status_code=409, detail="No running Outreach process")

		_process.terminate()
		_logs.append(f"[{_utc_now_iso()}] Stop requested")
		return {
			"status": "stopping",
			"run_id": _run_id,
			"pid": _process.pid,
		}


if __name__ == "__main__":
	import uvicorn

	uvicorn.run("Back:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
