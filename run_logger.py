# run_logger.py
import os
import time
import uuid
import socket
import traceback
import logging
from logging.handlers import RotatingFileHandler
import datetime as dt
from typing import Optional, Dict, Any
import threading  # <-- pour le lock (thread-safe)

from pymongo import MongoClient
from dotenv import load_dotenv

# Charger le fichier .env
load_dotenv()
job_name = "scrape_all_minimal"
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
LOG_FILE = os.getenv("LOG_FILE", "logs/run.log")  # pour les étapes et le suivi


def _incr_nested(metrics: dict, path: list[str], delta: int = 1):
    """
    Incrémente metrics[path[0]]...path[-1] += delta en créant les nœuds si besoin.
    Ex: _incr_nested(m, ["per_platform", "dabadoc", "rows"], 3)
    -> m = {"per_platform": {"dabadoc": {"rows": 3}}}
    """
    cur = metrics
    for k in path[:-1]:
        if k not in cur or not isinstance(cur[k], dict):
            cur[k] = {}
        cur = cur[k]
    last = path[-1]
    cur[last] = (cur.get(last, 0) or 0) + delta


class RunLogger:
    """
    - LOG FICHIER: start/steps/errors/fin (suivi détaillé lisible par cron)
    - MONGODB: résumé seulement (start/end/duration/status/errors/metrics)

    METRICS format (simple et clair, demandé):
    {
      "per_platform": {
        "dabadoc":   {"rows": 120},
        "nabady":    {"rows": 110},
        "med.ma":    {"rows": 130},
        "docdialy":  {"rows": 95}
      },
      "rows_total": 455
    }
    """
    def __init__(self, job_name: str):
        # Mongo (résumé)
        self.client = MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]
        try:
            self.db.runs.create_index([("job_name", 1), ("status", 1)])
        except Exception:
            pass

        # Identité du run
        self.job_name = job_name
        self.run_id = f"{dt.datetime.utcnow().isoformat()}-{uuid.uuid4()}"
        self.t0: Optional[float] = None

        # METRICS: uniquement name + nombre de rows (et total)
        self.metrics: Dict[str, Any] = {}
        self._lock = threading.Lock()  # <-- important pour écriture concurrente

        # Logger fichier (rotation)
        os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
        self.logger = logging.getLogger(f"run.{self.job_name}")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=5)
            handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            self.logger.addHandler(handler)

    # ————— Helpers —————
    def _has_running_instance(self) -> Optional[dict]:
        """
        Retourne le dernier run 'running' pour ce job s'il existe, sinon None.
        """
        return self.db.runs.find_one(
            {"job_name": self.job_name, "status": "running"},
            sort=[("started_at", -1)]
        )

    # ————— API —————
    def start(self, tags=None):
        existing = self._has_running_instance()
        if existing:
            raise RuntimeError(
                f"Un run '{self.job_name}' est déjà en cours "
                f"(run_id={existing.get('run_id')}, démarré le {existing.get('started_at')})."
            )

        self.t0 = time.perf_counter()

        # Log fichier (début)
        self.logger.info(
            f"[{self.job_name}] ▶️ Début extraction | run_id={self.run_id} | tags={tags or []} | host={socket.gethostname()}"
        )

        # Résumé DB (status running)
        self.db.runs.insert_one({
            "run_id": self.run_id,
            "job_name": self.job_name,
            "status": "running",
            "started_at": dt.datetime.utcnow(),
            "finished_at": None,
            "duration_sec": None,
            "env": {"host": socket.gethostname()},
            "metrics": {},
            "error": None,
            "tags": tags or []
        })

    def event(self, msg: str, level: str = "INFO", step: Optional[str] = None, **kv):
        """
        Étapes → FICHIER UNIQUEMENT (pas en base).
        """
        prefix = f"[{self.job_name}]  Étape"
        payload = f"{prefix} | {msg}"
        if step:
            payload += f" | step={step}"
        if kv:
            payload += f" | {kv}"
        lvl = level.upper()
        if lvl == "ERROR":
            self.logger.error(payload)
        elif lvl == "WARNING":
            self.logger.warning(payload)
        else:
            self.logger.info(payload)

    # ----------------- MÉTRIQUES (name + rows) -----------------
    def add_rows(self, platform_name: str, rows_count: int):
        """
        Ajoute 'rows_count' lignes pour une plateforme donnée (thread-safe).
        Exemple d'appel depuis un thread de scraping plateforme:
            rl.add_rows("dabadoc", n_lignes_extraites)
        """
        if not platform_name:
            platform_name = "unknown"

        rows_count = int(rows_count or 0)
        if rows_count <= 0:
            return

        with self._lock:
            # per_platform.<name>.rows += rows_count
            _incr_nested(self.metrics, ["per_platform", platform_name, "rows"], rows_count)
            # total global
            _incr_nested(self.metrics, ["rows_total"], rows_count)

    # (option utilitaire, si tu veux juste poser une valeur)
    def set_rows(self, platform_name: str, rows_count: int):
        """
        Fixe la valeur rows pour une plateforme (réécrit la valeur).
        Moins utilisée en pratique; 'add_rows' suffit généralement.
        """
        if not platform_name:
            platform_name = "unknown"

        rows_count = max(int(rows_count or 0), 0)
        with self._lock:
            # recalculer le total: retirer l'ancienne valeur si elle existe puis ajouter la nouvelle
            old = ((self.metrics.get("per_platform", {}) or {}).get(platform_name, {}) or {}).get("rows", 0)
            # set
            if "per_platform" not in self.metrics or not isinstance(self.metrics["per_platform"], dict):
                self.metrics["per_platform"] = {}
            if platform_name not in self.metrics["per_platform"] or not isinstance(self.metrics["per_platform"][platform_name], dict):
                self.metrics["per_platform"][platform_name] = {}
            self.metrics["per_platform"][platform_name]["rows"] = rows_count

            # total
            current_total = int(self.metrics.get("rows_total", 0) or 0)
            current_total = current_total - int(old or 0) + rows_count
            self.metrics["rows_total"] = max(current_total, 0)

    # -----------------------------------------------------------

    def finish_success(self):
        dur = round(time.perf_counter() - self.t0, 3) if self.t0 else None
        # Log fichier (fin)
        self.logger.info(
            f"[{self.job_name}]  Fin extraction | statut=success | durée_sec={dur} | run_id={self.run_id}"
        )
        # Résumé DB (success)
        self.db.runs.update_one(
            {"run_id": self.run_id},
            {"$set": {
                "status": "success",
                "finished_at": dt.datetime.utcnow(),
                "duration_sec": dur,
                "metrics": self.metrics
            }}
        )

    def finish_error(self, err: Exception):
        dur = round(time.perf_counter() - self.t0, 3) if self.t0 else None
        err_doc = {
            "type": type(err).__name__,
            "message": str(err),
            "traceback": traceback.format_exc()[:8000],  # résumé
        }
        # Log fichier (erreur + fin)
        self.logger.error(
            f"[{self.job_name}]  ERREUR | {err_doc['type']}: {err_doc['message']}\n{err_doc['traceback']}"
        )
        self.logger.info(
            f"[{self.job_name}]  Fin extraction | statut=failed | durée_sec={dur} | run_id={self.run_id}"
        )
        # Résumé DB (failed)
        self.db.runs.update_one(
            {"run_id": self.run_id},
            {"$set": {
                "status": "failed",
                "finished_at": dt.datetime.utcnow(),
                "duration_sec": dur,
                "error": err_doc,
                "metrics": self.metrics
            }}
        )
