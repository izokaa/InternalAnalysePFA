#!/usr/bin/env python3
# job_manual.py
# Lancement MANUEL: mêmes étapes visibles en SSE (RunLogger + LOG_FILE),
# test si l'AUTOMATIQUE (cron) est actif → on saute proprement, sans rien désactiver côté UI.
# Garde les fonctionnalités manuelles: sélection 1..N plateformes, pause 10s sur erreur + bouton « Continuer ».

import os, time, logging, json, signal, threading
from contextlib import contextmanager
from typing import Optional, List
from datetime import datetime, UTC

import pandas as pd
from run_logger import RunLogger

# ====== imports de ton code existant ======
from DefinitionMethod_s import (
    unifier_dataframe, attribuer_ids_df_hash_simple,
    lancer_scrapping_med, lancer_scrapping_nabady, lancer_scrapping_dabadoc,
    nettoyer_dataframe_medecins
)
# Fallback docdialy (Excel) ou fonction si dispo
try:
    from DefinitionMethod_s import lancer_scrapping_docdialy  # type: ignore
    _HAS_DOC_FUN = True
except Exception:
    _HAS_DOC_FUN = False

from insertion import inserer_dataframe
from ConnexionMongo_DB_ import get_db
from CalculerKPi import get_kpi
# ==========================================

# ---------- Configuration log (même fichier que l'admin pour SSE) ----------
LOG_FILE = os.getenv("LOG_FILE", "logs/run.log")
os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    encoding="utf-8"
)
log = logging.getLogger(__name__)

# Import optionnel du dashboard
try:
    from app import create_app
except Exception:
    create_app = None

# Chemin Excel Docdialy si on lit un fichier
DOC_EXCEL_PATH = os.getenv("DOC_EXCEL_PATH", "/home/salma/monitor_app/Fichier_DocDialy_ma_Apres_Nettoyage.xlsx")

# Comportement
FAIL_FAST = (os.getenv("FAIL_FAST", "1") == "1")                 # basculable via clic UI
STEP_LOG_TO_ROOT = (os.getenv("STEP_LOG_TO_ROOT", "0") == "1")   # 0 = pas de doublons root
PAUSE_SECONDS = int(os.getenv("PAUSE_SECONDS", "30"))            # pause sur erreur avec bouton Continuer

# ====== Chemins status/lock lus par l’UI ======
LOCK_PATH = os.getenv("LOCK_PATH", "/tmp/monitor_app.lock")
STATUS_PATH = os.getenv("STATUS_PATH", "/tmp/monitor_app.status.json")

def _write_status_atomic(data: dict):
    """Écrit STATUS_PATH de manière atomique pour que l'UI le lise sans refresh partiel."""
    try:
        os.makedirs(os.path.dirname(STATUS_PATH) or ".", exist_ok=True)
    except Exception:
        pass
    tmp = STATUS_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, STATUS_PATH)

# ====== Heartbeat fichier + DB flags (source="manual") ======
_heartbeat_on = {"on": False}
_heartbeat_state = {"run_id": None, "started_at": None}

def _set_global_running(rl: "RunLogger", running: bool, source: str, run_id: str | None = None):
    """Flag global DB pour l’UI (db.flags._id='run_lock')."""
    try:
        rl.db["flags"].update_one(
            {"_id": "run_lock"},
            {"$set": {
                "running": bool(running),
                "source": source,
                "run_id": run_id,
                "heartbeat": datetime.now(UTC).isoformat(),
                "updated_at": datetime.now(UTC).isoformat(),
            }},
            upsert=True
        )
    except Exception as e:
        log.warning("set_global_running: %s", e)

def _touch_global_heartbeat(rl: "RunLogger", source: str, run_id: str | None = None):
    try:
        rl.db["flags"].update_one(
            {"_id": "run_lock"},
            {"$set": {
                "running": True,
                "source": source,
                "run_id": run_id,
                "heartbeat": datetime.now(UTC).isoformat(),
                "updated_at": datetime.now(UTC).isoformat(),
            }},
            upsert=True
        )
    except Exception:
        pass

def _start_heartbeat_manual(rl: "RunLogger"):
    """Thread léger qui publie un heartbeat toutes les 3s (fichier + DB)."""
    _heartbeat_on["on"] = True
    _heartbeat_state["started_at"] = datetime.now(UTC).isoformat()

    def _beat():
        while _heartbeat_on["on"]:
            _write_status_atomic({
                "running": True,
                "pid": os.getpid(),
                "source": "manual",
                "run_id": _heartbeat_state.get("run_id"),
                "started_at": _heartbeat_state.get("started_at"),
                "heartbeat": datetime.now(UTC).isoformat()
            })
            _touch_global_heartbeat(rl, "manual", _heartbeat_state.get("run_id"))
            time.sleep(3)

    threading.Thread(target=_beat, daemon=True).start()

def _stop_heartbeat_manual(final_ok: bool, rl: "RunLogger"):
    _heartbeat_on["on"] = False
    _write_status_atomic({
        "running": False,
        "pid": os.getpid(),
        "source": "manual",
        "run_id": _heartbeat_state.get("run_id"),
        "ended_at": datetime.now(UTC).isoformat(),
        "status": "success" if final_ok else "error"
    })
    _set_global_running(rl, False, "manual", _heartbeat_state.get("run_id"))
    try:
        if os.path.exists(LOCK_PATH):
            os.remove(LOCK_PATH)
    except Exception:
        pass

# ====== Step logger (SSE via rl.event + option root) ======
STEP_IDX = 0
def _bump_step():
    global STEP_IDX
    STEP_IDX += 1
    return STEP_IDX

def _tee(rl: RunLogger, text: str, level: str = "INFO", step: str | None = None):
    rl.event(text, level=level, step=step)
    if STEP_LOG_TO_ROOT:
        getattr(log, level.lower(), log.info)(text)

@contextmanager
def step(rl: RunLogger, title: str, step_key: str):
    idx = _bump_step()
    _tee(rl, f"🧭 Étape | [{idx}] ▶️ {title}", level="INFO", step=step_key)
    try:
        yield idx
    except Exception as e:
        _tee(rl, f"🧭 Étape | [{idx}] ❌ {title}: {e}", level="ERROR", step=step_key)
        raise
    else:
        _tee(rl, f"🧭 Étape | [{idx}] ✅ {title}", level="INFO", step=step_key)

# ====== Tolérance d’erreurs (bouton Continuer) ======
def _refresh_fail_fast(rl: RunLogger):
    """Passe FAIL_FAST=False si l’admin a cliqué 'Continuer maintenant' dans l'UI."""
    try:
        global FAIL_FAST
        doc = rl.db.runs.find_one({"run_id": rl.run_id}, {"controls": 1}) or {}
        ctrl = (doc.get("controls") or {})
        if ctrl.get("continue_on_error") is True and FAIL_FAST:
            _tee(rl, "⚙️ Mode 'Continuer malgré erreurs' activé (toggle UI)", step="controls")
            FAIL_FAST = False
    except Exception:
        pass

def _await_continue_or_fail(rl: RunLogger, step_key: str, exc_msg: str, wait_seconds: int | None = None) -> bool:
    wait = PAUSE_SECONDS if wait_seconds is None else wait_seconds
    _tee(rl, f"⏸ Pause {wait}s — Erreur {step_key}: {exc_msg}. Ouvrez l’admin et cliquez « Continuer maintenant ».",
         level="WARNING", step=step_key)
    deadline = time.time() + wait
    while time.time() < deadline:
        time.sleep(1)
        _refresh_fail_fast(rl)
        if not FAIL_FAST:
            _tee(rl, "▶ Continuer accepté par l’admin", step=step_key)
            return True
    _tee(rl, "⛔️ Arrêt: aucun clic sur « Continuer » reçu dans le délai.", level="ERROR", step=step_key)
    return False

# ====== Pré-check: si AUTOMATIQUE actif, le MANUEL saute proprement ======
def _auto_run_active(rl: "RunLogger", stale_seconds: int = 900) -> bool:
    now = time.time()

    def _parse_any_ts(s: str | None) -> float | None:
        if not s: return None
        try:
            txt = str(s).replace("T"," ").split(".")[0]
            from time import strptime, mktime
            for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M"):
                try: return mktime(strptime(txt, fmt))
                except Exception: pass
        except Exception:
            return None
        return None

    def _recent(d: dict) -> bool:
        for k in ("heartbeat","updated_at","updatedAt","last_event_at","start_at","started_at"):
            ts = _parse_any_ts(d.get(k))
            if ts is not None and (now - ts) <= stale_seconds:
                return True
        if not d.get("finished_at") and not d.get("end_at") and not d.get("ended_at"):
            return True
        return False

    try:
        # 1) Vérifie flags.run_lock (plus simple/rapide)
        fl = rl.db["flags"].find_one({"_id": "run_lock"}) or {}
        if fl.get("running") and fl.get("source") in ("cron", "auto") and _recent(fl):
            return True

        # 2) Cherche des runs 'cron' récents
        q = {
            "name": "scrape_all_minimal",
            "tags": {"$in": ["cron"]},
            "$or": [
                {"state": {"$in": ["running", "started"]}},
                {"status": {"$in": ["running", "in_progress"]}},
                {"finished_at": {"$exists": False}},
                {"end_at": {"$exists": False}},
            ]
        }
        cur = rl.db.runs.find(q, {
            "heartbeat":1,"updated_at":1,"updatedAt":1,"last_event_at":1,
            "start_at":1,"started_at":1,"finished_at":1,"end_at":1
        }).limit(5)
        for d in (cur or []):
            if _recent(d):
                return True
    except Exception as e:
        log.warning("precheck auto: %s", e)
    return False

# ====== Scrape helpers (sélection plateforme) ======
def _scrape_platform(rl: RunLogger, key: str, date_extraction: str) -> pd.DataFrame | None:
    """Scrape une plateforme; renvoie None si toléré après erreur."""
    if key == "med":
        with step(rl, "Scrape Med", "scrape_med"):
            df = lancer_scrapping_med()
            df["Date Extraction"] = date_extraction
            rl.incr("rows_med", len(df))
            return df

    if key == "nabady":
        with step(rl, "Scrape Nabady", "scrape_nabady"):
            df = lancer_scrapping_nabady()
            df["Date Extraction"] = date_extraction
            rl.incr("rows_nabady", len(df))
            return df

    if key == "dabadoc":
        with step(rl, "Scrape Dabadoc", "scrape_dabadoc"):
            df = lancer_scrapping_dabadoc()
            df["Date Extraction"] = date_extraction
            rl.incr("rows_dabadoc", len(df))
            return df

    if key == "docdialy":
        with step(rl, "Lire Docdialy", "docdialy_read"):
            if _HAS_DOC_FUN:
                df = lancer_scrapping_docdialy()  # si dispo dans DefinitionMethod_s
            else:
                df = pd.read_excel(DOC_EXCEL_PATH)  # fallback Excel
            df["Date Extraction"] = date_extraction
            rl.incr("rows_docdialy", len(df))
            return df

    # clé inconnue → step d'erreur géré par pause
    raise RuntimeError(f"Plateforme inconnue: {key}")

# ====== MAIN ======
def main(selected_platforms: Optional[List[str]] = None):

    _final_ok_flag = {"ok": False}
    rl = RunLogger("scrape_all_minimal")

    # Par défaut: mêmes 4 plateformes
    if not selected_platforms:
        selected_platforms = ["med", "nabady", "docdialy", "dabadoc"]

    # 0) Si automatique actif → on saute le manuel (sans tout désactiver)
    if _auto_run_active(rl, stale_seconds=900):
        rl.start(tags=["manual", "skipped", "auto_running"])
        _tee(rl, "⏭ Manuel sauté : un run automatique (cron) est actif/récent.", step="preflight")
        rl.finish_success()
        _write_status_atomic({
            "running": False,
            "pid": os.getpid(),
            "source": "manual",
            "run_id": rl.run_id,
            "ended_at": datetime.now(UTC).isoformat(),
            "status": "skipped_auto_running"
        })
        _final_ok_flag["ok"] = True
        return

    # 1) Préparer heartbeat/status/flags
    try:
        open(LOCK_PATH, "a").close()
    except Exception:
        pass
    _write_status_atomic({
        "running": True,
        "pid": os.getpid(),
        "source": "manual",
        "run_id": None,  # mis à jour après rl.start()
        "started_at": datetime.now(UTC).isoformat(),
        "heartbeat": datetime.now(UTC).isoformat()
    })
    _set_global_running(rl, True, "manual", None)
    _start_heartbeat_manual(rl)

    try:
        rl.start(tags=["manual"] + selected_platforms)
        # propage le run_id au heartbeat
        _heartbeat_state["run_id"] = rl.run_id
        _write_status_atomic({
            "running": True,
            "pid": os.getpid(),
            "source": "manual",
            "run_id": rl.run_id,
            "started_at": datetime.now(UTC).isoformat(),
            "heartbeat": datetime.now(UTC).isoformat()
        })
        _set_global_running(rl, True, "manual", rl.run_id)

        # Permettre le bouton Continuer dans l'UI
        try:
            rl.db.runs.update_one({"run_id": rl.run_id},
                                  {"$set": {"controls": {"continue_on_error": False},
                                            "selected_platforms": selected_platforms}},
                                  upsert=True)
        except Exception:
            pass

        _tee(rl, "▶️ Début du job (manuel)", step="start")
        date_extraction = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 2) SCRAPE (1..N plateformes)
        dfs = []
        for key in selected_platforms:
            try:
                df = _scrape_platform(rl, key, date_extraction)
                if df is not None and not df.empty:
                    dfs.append((key, df))
                else:
                    _tee(rl, f"——— ⏭ Plateforme '{key}' — aucun résultat", step=f"pf_{key}_skip")
            except Exception as e:
                if not _await_continue_or_fail(rl, f"scrape_{key}", str(e)):
                    raise
                # sinon on tolère et on continue

        # 3) UNIFIER
        try:
            with step(rl, "Unifier schéma", "unify"):
                unified = []
                for key, df in dfs:
                    label = {"med":"Med","nabady":"Nabady","dabadoc":"Dabadoc","docdialy":"Docdialy"}.get(key, key)
                    u = unifier_dataframe(df, label)
                    if u is not None and not u.empty:
                        unified.append(u)
                if not unified:
                    raise RuntimeError("Aucune donnée à unifier (toutes plateformes vides ou en erreur).")
        except Exception as e:
            if not _await_continue_or_fail(rl, "unify", str(e)):
                raise
            unified = []

        # 4) CONCAT
        with step(rl, "Concaténer les dataframes", "concat"):
            df_final = pd.concat(unified, ignore_index=True) if unified else pd.DataFrame()
            rl.incr("rows_total", len(df_final))

        # 5) IDs
        skip_db_ops = False
        try:
            with step(rl, "Attribuer IDs", "ids"):
                db = get_db()
                if db is None:
                    raise RuntimeError("Connexion DB indisponible (get_db() a renvoyé None).")
                df_final, id_maps = attribuer_ids_df_hash_simple(df_final, db, collection_name="collection_globale")
        except Exception as e:
            if not _await_continue_or_fail(rl, "ids", str(e)):
                raise
            skip_db_ops = True

        # 6) INSERT
        inserted = 0
        if not skip_db_ops:
            try:
                with step(rl, "Insérer dans Mongo (collection_globale)", "insert"):
                    inserer_dataframe(df_final, db, "collection_globale")
                    inserted = len(df_final)
            except Exception as e:
                if not _await_continue_or_fail(rl, "insert", str(e)):
                    raise

        # 7) KPI + Dashboard
        try:
            with step(rl, "Calcul KPI + Dashboard", "kpi"):
                if callable(create_app):
                    _ = create_app()
                else:
                    try:
                        _ = get_kpi(db)
                    except Exception:
                        pass
        except Exception as e:
            if not _await_continue_or_fail(rl, "kpi", str(e)):
                raise

        _tee(rl, "Terminé", step="done")
        rl.finish_success()
        _final_ok_flag["ok"] = True
        return {"status": "ok", "selected": selected_platforms, "inserted": inserted}

    except Exception as e:
        _tee(rl, f"Erreur globale: {e}", level="ERROR", step="global")
        rl.finish_error(e)
        _final_ok_flag["ok"] = False
        raise
    finally:
        _stop_heartbeat_manual(final_ok=_final_ok_flag["ok"], rl=rl)

# ====== Entrée CLI ======
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("-p", "--platforms", type=str,
                    help="Liste des plateformes séparées par des virgules (ex: med,nabady,docdialy,dabadoc)")
    ap.add_argument("--continue-on-error", action="store_true",
                    help="Continuer même si une étape échoue (équivaut à FAIL_FAST=0)")
    args = ap.parse_args()

    if args.continue_on_error:
        os.environ["FAIL_FAST"] = "0"
    # Recalcule la globale FAIL_FAST après lecture env
    FAIL_FAST = (os.getenv("FAIL_FAST", "1") == "1")

    # Arrêt propre sur SIGTERM/SIGINT (Ctrl+C)
    def _graceful_exit(signum, frame):
        try:
            # On écrit un statut final 'error' si interrompu avant la fin
            _write_status_atomic({
                "running": False,
                "pid": os.getpid(),
                "source": "manual",
                "run_id": _heartbeat_state.get("run_id"),
                "ended_at": datetime.now(UTC).isoformat(),
                "status": "error"
            })
        except Exception:
            pass
        raise SystemExit(1)

    signal.signal(signal.SIGTERM, _graceful_exit)
    signal.signal(signal.SIGINT, _graceful_exit)

    selected = [s.strip() for s in (args.platforms or "").split(",") if s.strip()] or None
    main(selected_platforms=selected)
