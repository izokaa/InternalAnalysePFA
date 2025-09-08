# run_logger.py
import os
import uuid
import socket
import traceback
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict, Any
from datetime import datetime, UTC
from pathlib import Path

from pymongo import MongoClient
from dotenv import load_dotenv

# ---------- ENV ----------
load_dotenv()

# Racine projet (ex: /home/salma/monitor_app). Permet de rendre LOG_FILE absolu
# même quand le job est lancé depuis cron ou un autre cwd.
PROJECT_ROOT = Path(os.getenv("APP_ROOT") or Path(__file__).resolve().parents[1])

# Valeur .env ou défaut "logs/run.log"
_LOG_FILE_ENV = os.getenv("LOG_FILE", "logs/run.log")
LOG_FILE_PATH = Path(_LOG_FILE_ENV)
if not LOG_FILE_PATH.is_absolute():
    LOG_FILE_PATH = (PROJECT_ROOT / LOG_FILE_PATH).resolve()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "monitor_app")
LOG_FILE  = str(LOG_FILE_PATH)  # string pour le handler logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_MAX_BYTES    = int(os.getenv("LOG_MAX_BYTES", "1048576"))   # 1MB
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _incr_nested(d: dict, path: str, delta: int | float = 1):
    parts = path.split(".") if isinstance(path, str) else list(path)
    cur = d
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    last = parts[-1]
    cur[last] = (cur.get(last, 0) or 0) + delta


class RunLogger:
    """
    Version minimale compatible avec tes jobs:
      - rl = RunLogger("scrape_all_minimal")
      - rl.start(tags=[...])
      - rl.event("texte", level="INFO", step="scrape_med")
      - rl.incr("rows_med", len(df))
      - rl.finish_success() / rl.finish_error(e)
      - rl.db  (accès natif à Mongo)
      - rl.run_id (string)
    """
    def __init__(self, job_name: Optional[str] = None):
        self.job_name = job_name or "default_job"
        self.run_id   = f"{_iso_now()}-{uuid.uuid4()}"
        self.host     = socket.gethostname()
        self.pid      = os.getpid()
        self.metrics: Dict[str, Any] = {}
        self.tags: list[str] = []

        # --- Mongo
        self._client = MongoClient(MONGO_URI)
        self.db      = self._client[MONGO_DB]

        # --- Logger fichier (rotation)
        os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
        self.logger = logging.getLogger(f"run.{self.job_name}")
        # éviter les handlers dupliqués si plusieurs RunLogger sont créés
        if not self.logger.handlers:
            self.logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
            fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
            fh  = RotatingFileHandler(
                LOG_FILE,
                maxBytes=LOG_MAX_BYTES,
                backupCount=LOG_BACKUP_COUNT,
                encoding="utf-8"
            )
            fh.setFormatter(fmt)
            self.logger.addHandler(fh)

    # --------- Cycle de vie ---------
    def start(self, tags: Optional[list[str]] = None, extra: Optional[dict] = None):
        self.tags = list(tags or [])
        doc = {
            "run_id": self.run_id,
            "name": self.job_name,
            "host": self.host,
            "pid": self.pid,
            "tags": self.tags,
            "status": "running",
            "state": "running",
            "started_at": _iso_now(),
            "updated_at": _iso_now(),
            "last_event_at": None,
            "metrics": {},
            "events": [],   # événements embed (pratique pour SSE)
        }
        if extra:
            doc.update(extra)
        self.db.runs.insert_one(doc)
        self.logger.info("[start] %s tags=%s", self.job_name, self.tags)

    def event(self, message: str, level: str = "INFO", step: str | None = None):
        """
        Append un événement lisible par l’UI (SSE).
        """
        evt = {
            "ts": _iso_now(),
            "level": level,
            "message": message,
            "step": step,
        }
        self.db.runs.update_one(
            {"run_id": self.run_id},
            {"$push": {"events": evt},
             "$set": {"updated_at": _iso_now(), "last_event_at": _iso_now()}}
        )
        try:
            getattr(self.logger, level.lower(), self.logger.info)(message)
        except Exception:
            self.logger.info(message)

    def incr(self, path: str, delta: int | float = 1):
        """
        Incrémente un compteur en mémoire (persisté à finish_*()).
        Supporte chemin 'a.b.c' pour regrouper proprement.
        """
        _incr_nested(self.metrics, path, delta)

    def finish_success(self):
        self.db.runs.update_one(
            {"run_id": self.run_id},
            {"$set": {
                "status": "success",
                "state": "finished",
                "finished_at": _iso_now(),
                "updated_at": _iso_now(),
                "metrics": self.metrics
            }}
        )
        self.logger.info("[finish_success] %s", self.job_name)

    def finish_error(self, err: Exception):
        err_doc = {
            "type": type(err).__name__,
            "msg": str(err),
            "trace": traceback.format_exc()
        }
        self.db.runs.update_one(
            {"run_id": self.run_id},
            {"$set": {
                "status": "error",
                "state": "finished",
                "finished_at": _iso_now(),
                "updated_at": _iso_now(),
                "metrics": self.metrics,
                "error": err_doc
            }}
        )
        self.logger.error("[finish_error] %s | %s", self.job_name, err_doc["msg"])
