#!/usr/bin/env python3
"""LAN dashboard for the bounded two-directory music pipeline."""
from __future__ import annotations

import html
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

try:
    os.umask(0)
except Exception:
    pass

STATE = Path(os.environ.get("MUSIC_STATE", os.environ.get("MUSIC_DATA", "/state")))
CONFIG, STATUS, LOCK, LOG = STATE / "config.json", STATE / "status.json", STATE / "pipeline.lock", STATE / "last-run.log"
PORT = int(os.environ.get("MUSIC_UI_PORT", "8091"))
FALLBACK_OUTPUT_COND = (
    "(output_path LIKE '%未知艺术家%' OR output_path LIKE '%Unknown Artist%' "
    "OR output_path LIKE '%未知专辑%' OR output_path LIKE '%未分类专辑%' OR output_path LIKE '%Unknown Album%' "
    "OR metadata_state IN ('metadata_not_found', 'fallback_tags'))"
)
if Path("/appdata").is_dir():
    os.environ.setdefault("MUSIC_CACHE_DIR", "/appdata/cache")


def get_local_now() -> datetime:
    tz_env = os.environ.get("TZ", "Asia/Shanghai").strip()
    if ZoneInfo:
        try:
            return datetime.now(ZoneInfo(tz_env)).replace(tzinfo=None)
        except Exception:
            pass
    if any(k in tz_env.lower() for k in ("shanghai", "beijing", "cst", "asia/")):
        try:
            return datetime.now(timezone(timedelta(hours=8))).replace(tzinfo=None)
        except Exception:
            pass
    return datetime.now()


def read_json(path: Path, default: dict) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else default
    except (OSError, json.JSONDecodeError):
        return default


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def get_or_init_config() -> dict:
    cfg = read_json(CONFIG, {})
    acc = accessible_paths()
    changed = False

    src_val = cfg.get("source_dir", "")
    out_val = cfg.get("output_dir", "")

    # Only validate existing user-configured paths against accessible mounts;
    # if a configured path is no longer accessible, reset it to empty.
    if src_val and not any(Path(src_val) == a or a in Path(src_val).parents for a in acc):
        src_val = ""
        cfg["source_dir"] = ""
        changed = True

    if out_val and not any(Path(out_val) == a or a in Path(out_val).parents for a in acc):
        out_val = ""
        cfg["output_dir"] = ""
        changed = True

    # Absolutely NO hardcoded guesses, NO keyword heuristics, NO default directory assignment.
    # If not configured, keep it strictly empty ("") and let the user explicitly select via Web UI.

    if changed:
        try:
            write_json(CONFIG, cfg)
        except Exception:
            pass
    return cfg


def accessible_paths() -> list[Path]:
    roots: list[Path] = []

    # 0. Primary standard: /music volume mount
    music_mount = Path("/music")
    if music_mount.exists() and music_mount.is_dir():
        roots.append(music_mount)

    values: list[str] = []
    # 1. From authorized-paths file written by fnOS callback
    auth_file = Path("/appdata/authorized-paths")
    if auth_file.is_file():
        try:
            for line in auth_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    values.append(line)
        except Exception:
            pass

    # 2. From TRIM_DATA_ACCESSIBLE_PATHS environment variable
    env_paths = os.environ.get("TRIM_DATA_ACCESSIBLE_PATHS", "").split(":")
    values.extend(env_paths)

    # 3. From mounts in /proc/mounts starting with /vol or /music
    try:
        proc_mounts = Path("/proc/mounts")
        if proc_mounts.is_file():
            for line in proc_mounts.read_text(encoding="utf-8", errors="replace").splitlines():
                parts = line.split()
                if len(parts) >= 2:
                    target = parts[1]
                    if (target.startswith("/vol") or target == "/music") and "@" not in target:
                        values.append(target)
    except Exception:
        pass

    # 4. Fallback from environment variables
    values.extend([os.environ.get("MUSIC_PATH", ""), os.environ.get("MUSIC_MOUNT_SOURCE", ""), os.environ.get("MUSIC_MOUNT_OUTPUT", "")])

    for value in values:
        if not value:
            continue
        v_str = value.strip()
        if not (v_str.startswith("/vol") or v_str.startswith("/music")):
            continue
        try:
            path = Path(v_str).resolve(strict=False)
            if path.exists() and path.is_dir() and path not in roots:
                roots.append(path)
        except Exception:
            pass

    result: list[Path] = list(roots)
    # Automatically scan subdirectories (up to 3 levels deep, max 300 items) for each authorized root
    queue: list[tuple[Path, int]] = [(r, 1) for r in roots]
    while queue and len(result) < 300:
        curr, depth = queue.pop(0)
        if depth > 3:
            continue
        try:
            for item in sorted(curr.iterdir(), key=lambda p: p.name.lower()):
                if item.is_dir() and not item.name.startswith((".", "@")):
                    if item not in result:
                        result.append(item)
                    queue.append((item, depth + 1))
        except Exception:
            pass

    return sorted(result, key=lambda p: str(p))


ERRORS = {
    "unsupported_fields": {
        "zh": "配置只能包含受支持的应用设置字段",
        "en": "Configuration can only contain supported application settings fields."
    },
    "dirs_required": {
        "zh": "请在下方选择整理前（源目录）与整理后（曲库目录）",
        "en": "Please select source and output library directories below."
    },
    "vol_path_required": {
        "zh": "只能使用 /music/... 或 /volN/... 存储卷路径",
        "en": "Only valid /music/... or /volN/... storage paths can be used."
    },
    "not_authorized": {
        "zh": "所选目录未包含在已挂载的 /music 存储卷中（请在飞牛安装/设置中确认 MUSIC_PATH 设置）",
        "en": "Selected directory is not within mounted /music storage volume (check MUSIC_PATH in fnOS settings)."
    },
    "source_not_exists": {
        "zh": "原始文件夹必须真实存在于 NAS 存储卷中",
        "en": "Source music folder must physically exist on the NAS storage volume."
    },
    "source_not_readable": {
        "zh": "原始文件夹不可读（请在飞牛「文件管理」中检查该目录读取权限）",
        "en": "Source music folder is not readable (check read permissions in fnOS File Manager)."
    },
    "output_not_writable": {
        "zh": "整理后文件夹不可写入（请在飞牛「文件管理」中检查该目录或上级共享文件夹读写权限）",
        "en": "Output library folder is not writable (check read/write permissions in fnOS File Manager)."
    },
    "nested_dirs": {
        "zh": "两个文件夹不能相同，也不能互相包含",
        "en": "Source and output folders cannot be identical or nested within each other."
    },
    "invalid_stage": {
        "zh": "当前阶段不能执行此操作",
        "en": "This action cannot be performed in the current phase."
    },
    "task_running": {
        "zh": "已有任务在运行",
        "en": "A task is already running."
    },
    "unauthorized_path": {
        "zh": "路径未授权或不存在",
        "en": "Path is not authorized or does not exist."
    },
    "admin_auth_failed": {
        "zh": "控制台访问受限：管理密码不匹配",
        "en": "Console access restricted: Admin password incorrect."
    }
}


def get_error_message(key: str, lang: str = "zh") -> str:
    lang_key = "en" if str(lang).lower().startswith("en") else "zh"
    entry = ERRORS.get(key, {})
    return entry.get(lang_key, entry.get("zh", key))


def authorized(path: Path) -> bool:
    candidate = path.resolve(strict=False)
    acc = accessible_paths()
    return any(candidate == root or root in candidate.parents for root in acc)


def nested(first: Path, second: Path) -> bool:
    first, second = first.resolve(strict=False), second.resolve(strict=False)
    return first == second or first in second.parents or second in first.parents


def check_writable(path: Path) -> bool:
    try:
        cand = path.resolve(strict=False)
        # 1. If folder exists and is a directory, test direct access or probe file write
        if cand.is_dir() or cand.exists():
            if os.access(cand, os.W_OK):
                return True
            probe = cand / f".perm_probe_{uuid.uuid4().hex[:6]}.tmp"
            try:
                probe.write_text("ok", encoding="utf-8")
                probe.unlink(missing_ok=True)
                return True
            except (OSError, PermissionError):
                pass

        # 2. If folder does not exist, probe mkdir creation
        if not (cand.is_dir() or cand.exists()):
            try:
                cand.mkdir(parents=True, exist_ok=True)
                probe = cand / f".perm_probe_{uuid.uuid4().hex[:6]}.tmp"
                try:
                    probe.write_text("ok", encoding="utf-8")
                    probe.unlink(missing_ok=True)
                    return True
                except (OSError, PermissionError):
                    pass
            except (OSError, PermissionError):
                pass

        # 3. Parent inheritance: check existing nearest parent
        curr = cand if not (cand.is_dir() or cand.exists()) else cand.parent
        while str(curr) != "/" and not (curr.is_dir() or curr.exists()):
            curr = curr.parent
        if curr.is_dir() or curr.exists():
            if os.access(curr, os.W_OK):
                return True
            probe = curr / f".perm_probe_{uuid.uuid4().hex[:6]}.tmp"
            try:
                probe.write_text("ok", encoding="utf-8")
                probe.unlink(missing_ok=True)
                return True
            except (OSError, PermissionError):
                pass

        # 4. Authorized root inheritance: if any authorized ancestor is writable, subdirectories inherit
        for root in accessible_paths():
            if cand == root or root in cand.parents:
                if os.access(root, os.W_OK):
                    return True
                probe_root = root / f".perm_probe_{uuid.uuid4().hex[:6]}.tmp"
                try:
                    probe_root.write_text("ok", encoding="utf-8")
                    probe_root.unlink(missing_ok=True)
                    return True
                except (OSError, PermissionError):
                    pass
    except Exception:
        pass
    return False


def check_readable(path: Path) -> bool:
    try:
        cand = path.resolve(strict=False)
        if cand.is_dir() or cand.exists():
            if os.access(cand, os.R_OK):
                return True
            try:
                next(cand.iterdir(), None)
                return True
            except (OSError, PermissionError):
                pass
        curr = cand.parent
        while str(curr) != "/" and not (curr.is_dir() or curr.exists()):
            curr = curr.parent
        if (curr.is_dir() or curr.exists()) and os.access(curr, os.R_OK):
            return True
        for root in accessible_paths():
            if cand == root or root in cand.parents:
                if os.access(root, os.R_OK):
                    return True
    except Exception:
        pass
    return False


def config_valid(config: dict, lang: str = "zh") -> tuple[bool, str]:
    allowed_keys = {"source_dir", "output_dir", "sample_verified", "initialized", "proxy", "offline_mode", "admin_password", "schedule"}
    if set(config) - allowed_keys:
        return False, get_error_message("unsupported_fields", lang)
    source_raw, output_raw = config.get("source_dir"), config.get("output_dir")
    if not source_raw or not output_raw:
        return False, get_error_message("dirs_required", lang)
    if not all(isinstance(value, str) and (value.startswith("/vol") or value.startswith("/music")) for value in (source_raw, output_raw)):
        return False, get_error_message("vol_path_required", lang)
    source, output = Path(source_raw), Path(output_raw)
    if not authorized(source) or not authorized(output):
        return False, get_error_message("not_authorized", lang)
    if not (source.is_dir() or source.exists()):
        return False, get_error_message("source_not_exists", lang)
    if not check_readable(source):
        return False, get_error_message("source_not_readable", lang)
    if not check_writable(output):
        return False, get_error_message("output_not_writable", lang)
    if not (output.is_dir() or output.exists()):
        try:
            output.mkdir(parents=True, exist_ok=True)
        except OSError:
            pass
    if nested(source, output):
        return False, get_error_message("nested_dirs", lang)
    return True, ""


def connection() -> sqlite3.Connection:
    import pipeline
    pipeline.init_db(STATE / "ledger-v6.sqlite")
    database = sqlite3.connect(STATE / "ledger-v6.sqlite", timeout=60)
    database.row_factory = sqlite3.Row
    database.execute("PRAGMA journal_mode=WAL;")
    database.execute("PRAGMA busy_timeout=60000;")
    database.execute("PRAGMA synchronous=NORMAL;")
    return database


def latest_runs(limit: int = 20) -> list[dict]:
    database: sqlite3.Connection | None = None
    try:
        database = connection()
        rows = database.execute("SELECT id,mode,status,phase,phase_done,phase_total,started_at,finished_at,summary_json FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["summary"] = json.loads(item.pop("summary_json") or "{}")
            result.append(item)
        return result
    except (OSError, sqlite3.Error, json.JSONDecodeError):
        return []
    finally:
        if database is not None:
            database.close()


def report(run_id: str | None = None) -> dict:
    db_conn: sqlite3.Connection | None = None
    try:
        db_conn = connection()
        if run_id:
            run = db_conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        else:
            run = db_conn.execute("SELECT * FROM runs WHERE status='running' ORDER BY started_at DESC LIMIT 1").fetchone()
            if not run:
                run = db_conn.execute("SELECT * FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
        if not run:
            return {}

        target_run_id = run["id"]
        if run["status"] == "done" and run["summary_json"] and run["summary_json"] != "{}":
            try:
                cached = json.loads(run["summary_json"])
                if isinstance(cached, dict) and cached.get("metrics"):
                    return cached
            except json.JSONDecodeError:
                pass

        def counts(column: str) -> dict[str, int]:
            return {str(r["key"]): int(r["value"]) for r in db_conn.execute(f"SELECT {column} AS key,count(*) AS value FROM items WHERE run_id=? GROUP BY {column}", (target_run_id,))}

        dispositions = counts("disposition")
        metadata = counts("metadata_state")
        lyrics = counts("lyrics_state")
        covers = counts("cover_state")
        groups = {str(r["key"]): int(r["value"]) for r in db_conn.execute("SELECT decision AS key,count(*) AS value FROM groups WHERE run_id=? GROUP BY decision", (target_run_id,))}

        lyrics_already_present = lyrics.get("lyrics_already_present", 0)
        lyrics_embedded = lyrics.get("lyrics_embedded", 0)
        cover_already_present = covers.get("cover_already_present", 0)
        cover_embedded = covers.get("cover_embedded", 0)

        fb_row = db_conn.execute(
            f"SELECT count(*) FROM items WHERE run_id=? AND disposition IN ('published', 'sample_validated') AND {FALLBACK_OUTPUT_COND}",
            (target_run_id,)
        ).fetchone()
        fallback_published = fb_row[0] if fb_row else 0

        upg_row = db_conn.execute("SELECT count(*) FROM groups WHERE run_id=? AND decision='quality_upgrade'", (target_run_id,)).fetchone()
        quality_upgraded = upg_row[0] if upg_row else groups.get("quality_upgrade", 0)

        cls_info = {}
        if run["summary_json"] and run["summary_json"] != "{}":
            try:
                s_obj = json.loads(run["summary_json"])
                if isinstance(s_obj, dict):
                    cls_info = s_obj.get("classification") or s_obj.get("metrics") or {}
            except Exception:
                pass

        metrics = {
            "scanned_total": sum(dispositions.values()),
            "published": dispositions.get("published", 0),
            "exact_duplicate": dispositions.get("duplicate_exact", 0),
            "same_recording_duplicate": dispositions.get("duplicate_same_recording", 0),
            "possible_version_groups": groups.get("keep_all_ambiguous", 0),
            "quality_upgraded": quality_upgraded,
            "failed": dispositions.get("failed", 0),
            "fallback_published": fallback_published,
            "metadata_matched": metadata.get("metadata_matched", 0),
            "lyrics_embedded": lyrics_embedded,
            "lyrics_already_present": lyrics_already_present,
            "lyrics_total_with": lyrics_already_present + lyrics_embedded,
            "lyrics_not_found": lyrics.get("lyrics_not_found", 0),
            "cover_embedded": cover_embedded,
            "cover_already_present": cover_already_present,
            "cover_total_with": cover_already_present + cover_embedded,
            "cover_not_found": covers.get("cover_not_found", 0),
            "new_files": cls_info.get("new_files", 0),
            "modified_files": cls_info.get("modified_files", 0),
            "stale_decisions": cls_info.get("stale_decisions", 0),
            "retry_files": cls_info.get("retry_files", 0),
            "orphans_archived": cls_info.get("orphans_archived", 0),
            "state_bytes": state_size(),
            "temporary_audio_bytes": 0,
            "acoustic_duplicates": dispositions.get("duplicate_same_recording", 0),
            "exact_duplicates": dispositions.get("duplicate_exact", 0),
            "scanned": sum(dispositions.values()),
            "version_groups": groups.get("keep_all_ambiguous", 0),
            "upgraded": quality_upgraded,
            "cover_art_embedded": cover_embedded,
            "cover_art_already_present": cover_already_present,
        }
        return {
            "run_id": target_run_id,
            "status": run["status"],
            "phase": {"name": run["phase"], "done": run["phase_done"], "total": run["phase_total"]},
            "started_at": run["started_at"],
            "finished_at": run["finished_at"],
            "dispositions": dispositions,
            "metadata": metadata,
            "lyrics": lyrics,
            "covers": covers,
            "groups": groups,
            "metrics": metrics,
        }
    except Exception:
        return {}
    finally:
        if db_conn is not None:
            db_conn.close()


def sample_ready(config: dict) -> bool:
    marker = config.get("sample_verified") or {}
    run_id = marker.get("run_id") if isinstance(marker, dict) else None
    if not run_id:
        return False
    try:
        with connection() as db_conn:
            return bool(db_conn.execute("SELECT 1 FROM runs WHERE id=? AND mode='sample' AND status='done'", (run_id,)).fetchone())
    except Exception:
        return any(run["id"] == run_id and run["status"] == "done" for run in latest_runs(100))


def state_size() -> int:
    total = 0
    if STATE.exists():
        for path in STATE.rglob("*"):
            try:
                if path.is_file():
                    total += path.stat().st_size
            except OSError:
                pass
    return total


def take_lock() -> bool:
    try:
        STATE.mkdir(parents=True, exist_ok=True)
        if LOCK.exists():
            try:
                os.kill(int(LOCK.read_text(encoding="ascii")), 0)
                return False
            except (OSError, ValueError):
                try:
                    LOCK.unlink(missing_ok=True)
                except OSError:
                    pass
        fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o666)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return True
    except (FileExistsError, OSError):
        return False


GLOBAL_PIPELINE_PROC: subprocess.Popen | None = None
STOP_REQUESTED: bool = False


def is_pipeline_running() -> bool:
    global GLOBAL_PIPELINE_PROC
    if GLOBAL_PIPELINE_PROC is not None and GLOBAL_PIPELINE_PROC.poll() is None:
        return True
    return LOCK.exists()


def stop_pipeline() -> None:
    global GLOBAL_PIPELINE_PROC, STOP_REQUESTED
    STOP_REQUESTED = True
    proc = GLOBAL_PIPELINE_PROC
    if proc and proc.poll() is None:
        try:
            if hasattr(os, "killpg") and hasattr(os, "getpgid"):
                try:
                    os.killpg(os.getpgid(proc.pid), 15)
                except OSError:
                    proc.terminate()
            else:
                proc.terminate()
            proc.wait(timeout=3)
        except Exception:
            try:
                if hasattr(os, "killpg") and hasattr(os, "getpgid"):
                    try:
                        os.killpg(os.getpgid(proc.pid), 9)
                    except OSError:
                        proc.kill()
                else:
                    proc.kill()
            except Exception:
                pass
    GLOBAL_PIPELINE_PROC = None
    try:
        LOCK.unlink(missing_ok=True)
    except OSError:
        pass
    write_json(STATUS, {"state": "stopped", "message": "任务已由用户手动停止"})

    # Clean up any stale temporary run roots on output disk
    try:
        cfg_val = read_json(CONFIG, {})
        out_str = cfg_val.get("output_dir")
        if out_str and Path(out_str).is_dir():
            for p in Path(out_str).iterdir():
                if p.is_dir() and p.name.startswith(".music-rebuild-run-"):
                    shutil.rmtree(p, ignore_errors=True)
    except Exception:
        pass

    now_str = get_local_now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with LOG.open("a", encoding="utf-8") as handle:
            handle.write(f"\n[{now_str}] 任务已手动停止。\n")
    except Exception:
        pass
    write_json(STATUS, {"state": "stopped", "message": "任务已由用户手动停止"})
    try:
        with connection() as db_conn:
            db_conn.execute("UPDATE runs SET status='stopped',finished_at=CURRENT_TIMESTAMP WHERE status='running'")
            db_conn.commit()
    except Exception:
        pass


def reset_system(clear_history: bool = True, clear_inventory: bool = True, clear_kb: bool = False, clear_ncm: bool = False) -> dict:
    if read_json(STATUS, {}).get("state") == "running":
        raise RuntimeError("任务运行中，禁止执行重置操作")

    # 1. Clear database tables
    db_path = STATE / "ledger-v6.sqlite"
    if db_path.is_file():
        conn = sqlite3.connect(db_path, timeout=30)
        try:
            if clear_history:
                for tbl in ("runs", "items", "groups", "events"):
                    try:
                        conn.execute(f"DELETE FROM {tbl}")
                    except sqlite3.OperationalError:
                        pass
            if clear_inventory:
                try:
                    conn.execute("DELETE FROM source_inventory")
                except sqlite3.OperationalError:
                    pass
            if clear_kb:
                try:
                    conn.execute("DELETE FROM knowledge_base")
                except sqlite3.OperationalError:
                    pass
            conn.commit()
            conn.execute("VACUUM")
        finally:
            conn.close()

    # 2. Reset Dupsonic if exists
    dup_db = STATE / "dupsonic-v6.sqlite"
    if clear_history and dup_db.is_file():
        try:
            conn = sqlite3.connect(dup_db, timeout=10)
            conn.execute("DELETE FROM files")
            conn.commit()
            conn.execute("VACUUM")
            conn.close()
        except Exception:
            pass

    # 3. Clear NCM cache directory if requested
    # 3. Clear NCM cache directory and legacy work dirs if requested
    ncm_count = 0
    if clear_ncm:
        cache_dir = Path(os.environ.get("MUSIC_CACHE_DIR", "/appdata/cache"))
        if cache_dir.is_dir():
            for p in list(cache_dir.rglob("*")):
                try:
                    if p.is_file():
                        p.unlink()
                        ncm_count += 1
                except OSError:
                    pass
            shutil.rmtree(cache_dir, ignore_errors=True)
        legacy_work = Path("/appdata/work")
        if legacy_work.is_dir():
            shutil.rmtree(legacy_work, ignore_errors=True)

    # 4. Clear Log & Reset Status
    if clear_history:
        try:
            LOG.parent.mkdir(parents=True, exist_ok=True)
            LOG.write_text("", encoding="utf-8")
        except OSError:
            pass
        write_json(STATUS, {
            "state": "idle",
            "message": "系统状态与历史记录已重置，已回到从头开始状态"
        })

    # 5. Reset config flags if inventory cleared
    cfg_val = read_json(CONFIG, {})
    if clear_inventory:
        cfg_val.pop("sample_verified", None)
        cfg_val.pop("initialized", None)
        write_json(CONFIG, cfg_val)

    return {
        "ok": True,
        "success": True,
        "cleared_history": clear_history,
        "cleared_inventory": clear_inventory,
        "cleared_knowledge_base": clear_kb,
        "cleared_ncm_cache_files": ncm_count,
        "message": "已成功重置，可从头开始执行整理！"
    }


def run_pipeline(mode: str) -> None:
    global GLOBAL_PIPELINE_PROC, STOP_REQUESTED
    STOP_REQUESTED = False
    try:
        cfg_val = read_json(CONFIG, {})
        proxy = str(cfg_val.get("proxy", "")).strip()
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        env["PYTHONUNBUFFERED"] = "1"
        if proxy:
            env["HTTP_PROXY"] = proxy
            env["HTTPS_PROXY"] = proxy
            env["ALL_PROXY"] = proxy
            env["http_proxy"] = proxy
            env["https_proxy"] = proxy
            env["all_proxy"] = proxy
        script_path = str(Path(__file__).resolve().with_name("pipeline.py"))
        py_bin = sys.executable or "python3"
        with LOG.open("a", encoding="utf-8") as handle:
            proc = subprocess.Popen(
                [py_bin, "-u", script_path, mode],
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                start_new_session=(os.name != "nt")
            )
            GLOBAL_PIPELINE_PROC = proc
            returncode = proc.wait()
        GLOBAL_PIPELINE_PROC = None
        if STOP_REQUESTED:
            write_json(STATUS, {"state": "stopped", "message": "任务已由用户手动停止", "mode": mode})
        elif returncode:
            current = read_json(STATUS, {})
            if current.get("state") not in ("idle", "stopped"):
                write_json(STATUS, {"state": "failed", "message": "整理失败；查看日志", "mode": mode})
    except Exception as exc:
        err_msg = traceback.format_exc()
        try:
            with LOG.open("a", encoding="utf-8") as handle:
                handle.write(f"\n[启动异常] 流水线启动失败:\n{err_msg}\n")
        except Exception:
            pass
        write_json(STATUS, {"state": "failed", "message": f"启动异常: {exc}", "mode": mode})
    finally:
        GLOBAL_PIPELINE_PROC = None
        try:
            LOCK.unlink(missing_ok=True)
        except OSError:
            pass


def start_mode(mode: str, lang: str = "zh") -> tuple[int, str]:
    config = read_json(CONFIG, {})
    valid, message = config_valid(config, lang)
    if not valid:
        return 400, message
    allowed = (
        mode == "sample"
        or (mode == "full" and (sample_ready(config) or bool(config.get("initialized"))))
        or (mode == "incremental" and bool(config.get("initialized")))
    )
    if not allowed:
        return 409, get_error_message("invalid_stage", lang)
    if not take_lock():
        return 409, get_error_message("task_running", lang)
    now_str = get_local_now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        if LOG.exists() and LOG.stat().st_size > 0:
            rotated = LOG.with_name("last-run.1.log")
            try:
                if rotated.exists():
                    rotated.unlink()
                LOG.rename(rotated)
            except Exception:
                pass
        LOG.write_text(f"[{now_str}] 正在初始化并启动 [{mode}] 整理流水线...\n", encoding="utf-8")
    except Exception:
        pass
    write_json(STATUS, {"state": "running", "message": f"正在启动 {mode} 整理...", "mode": mode, "phase": "starting", "phase_done": 0, "phase_total": 0})
    threading.Thread(target=run_pipeline, args=(mode,), daemon=True).start()
    return 202, json.dumps({"accepted": True, "mode": mode}, ensure_ascii=False)


def parse_time_parts(time_str: str, default_h: int = 3, default_m: int = 0) -> tuple[int, int]:
    try:
        parts = str(time_str).strip().split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return max(0, min(23, h)), max(0, min(59, m))
    except Exception:
        return default_h, default_m


def compute_next_run(
    rule: str = "daily",
    custom_time: str = "03:00",
    from_dt: datetime | None = None,
    anchor_time: str | None = None,
    interval_hours: int = 6,
    days: list[int] | None = None
) -> datetime:
    now = from_dt or get_local_now()
    rule = str(rule or "daily").strip().lower()

    if anchor_time is None:
        if rule in ("hourly", "interval_1h"):
            return (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        elif rule == "interval_6h":
            return now + timedelta(hours=6)
        elif rule == "interval_12h":
            return now + timedelta(hours=12)
        elif rule == "interval_24h":
            return now + timedelta(hours=24)

    if rule.startswith("interval"):
        if "_" in rule and rule.split("_")[-1].endswith("h"):
            try:
                interval_hours = int(rule.split("_")[-1][:-1])
            except Exception:
                pass
        interval_hours = max(1, min(24, int(interval_hours or 6)))
        ah, am = parse_time_parts(anchor_time or "00:00", 0, 0)
        pts = sorted(list(set([(ah + i * interval_hours) % 24 for i in range(24 // interval_hours + 1)])))
        for day_offset in (0, 1):
            base_date = now.date() + timedelta(days=day_offset)
            for h in pts:
                cand = datetime.combine(base_date, datetime.min.time()).replace(hour=h, minute=am, second=0, microsecond=0)
                if cand > now:
                    return cand
        return now + timedelta(hours=interval_hours)

    elif rule == "weekly":
        h, m = parse_time_parts(custom_time, 3, 0)
        valid_days = set(days) if days else {7}
        for d in range(8):
            cand_date = now.date() + timedelta(days=d)
            cand = datetime.combine(cand_date, datetime.min.time()).replace(hour=h, minute=m, second=0, microsecond=0)
            if cand > now and cand.isoweekday() in valid_days:
                return cand
        return now + timedelta(days=1)

    else:  # daily
        h, m = parse_time_parts(custom_time, 3, 0)
        target = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return target


def get_schedule_preview(sched: dict, from_dt: datetime | None = None) -> dict:
    now = from_dt or get_local_now()
    rule = str(sched.get("rule", "daily")).strip().lower()
    custom_time = str(sched.get("custom_time") or sched.get("time") or "03:00").strip()
    anchor_time = sched.get("anchor_time")
    interval_hours = int(sched.get("interval_hours", 6))
    days = sched.get("days") or [1, 2, 3, 4, 5, 6, 7]

    next_dt = compute_next_run(rule, custom_time, now, anchor_time, interval_hours, days)
    
    points = []
    if rule.startswith("interval"):
        if "_" in rule and rule.split("_")[-1].endswith("h"):
            try:
                interval_hours = int(rule.split("_")[-1][:-1])
            except Exception:
                pass
        interval_hours = max(1, min(24, int(interval_hours or 6)))
        if anchor_time:
            ah, am = parse_time_parts(anchor_time, 0, 0)
            pts = sorted(list(set([(ah + i * interval_hours) % 24 for i in range(24 // interval_hours + 1)])))
            points = [f"{h:02d}:{am:02d}" for h in pts]
        else:
            points = [next_dt.strftime("%H:%M")]
    elif rule == "weekly":
        h, m = parse_time_parts(custom_time, 3, 0)
        points = [f"{h:02d}:{m:02d}"]
    else:
        h, m = parse_time_parts(custom_time, 3, 0)
        points = [f"{h:02d}:{m:02d}"]

    return {
        "rule": rule,
        "points": points,
        "next_run": next_dt.strftime("%Y-%m-%d %H:%M:%S")
    }


def check_and_trigger_schedule(now_dt: datetime | None = None) -> bool:
    now = now_dt or get_local_now()
    cfg = read_json(CONFIG, {})
    sched = cfg.get("schedule")
    if not isinstance(sched, dict) or not sched.get("enabled"):
        return False
    if not cfg.get("initialized"):
        return False

    st = read_json(STATUS, {})
    if st.get("state") == "running":
        return False

    next_run_str = sched.get("next_run")
    rule = sched.get("rule", "daily")
    custom_time = sched.get("custom_time") or sched.get("time") or "03:00"
    anchor_time = sched.get("anchor_time")
    interval_hours = int(sched.get("interval_hours", 6))
    days = sched.get("days")

    if not next_run_str:
        preview = get_schedule_preview(sched, now)
        sched["next_run"] = preview["next_run"]
        sched["points"] = preview["points"]
        cfg["schedule"] = sched
        write_json(CONFIG, cfg)
        return False

    try:
        next_dt = datetime.strptime(next_run_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        preview = get_schedule_preview(sched, now)
        sched["next_run"] = preview["next_run"]
        sched["points"] = preview["points"]
        cfg["schedule"] = sched
        write_json(CONFIG, cfg)
        return False

    if now >= next_dt:
        code, _ = start_mode("incremental", lang=sched.get("lang", "zh"))
        if code == 202:
            sched["last_run"] = now.strftime("%Y-%m-%d %H:%M:%S")
            preview = get_schedule_preview(sched, now)
            sched["next_run"] = preview["next_run"]
            sched["points"] = preview["points"]
            cfg["schedule"] = sched
            write_json(CONFIG, cfg)
            return True
        else:
            # 启动受阻（如已有任务运行冲突或校验未过），智能退避 10 分钟重试
            backoff_dt = now + timedelta(minutes=10)
            sched["next_run"] = backoff_dt.strftime("%Y-%m-%d %H:%M:%S")
            cfg["schedule"] = sched
            write_json(CONFIG, cfg)
            return False
    return False


SCHEDULER_THREAD = None
SCHEDULER_STOP_EVENT = threading.Event()


def scheduler_loop() -> None:
    while not SCHEDULER_STOP_EVENT.is_set():
        try:
            check_and_trigger_schedule()
        except Exception:
            pass
        SCHEDULER_STOP_EVENT.wait(30)


def start_scheduler() -> None:
    global SCHEDULER_THREAD
    if SCHEDULER_THREAD is None or not SCHEDULER_THREAD.is_alive():
        SCHEDULER_STOP_EVENT.clear()
        SCHEDULER_THREAD = threading.Thread(target=scheduler_loop, daemon=True, name="MusicMate-Scheduler")
        SCHEDULER_THREAD.start()


DASHBOARD_HTML = Path(__file__).with_name("dashboard.html")

def render_dashboard() -> str:
    if DASHBOARD_HTML.is_file():
        return DASHBOARD_HTML.read_text(encoding="utf-8")
    config = read_json(CONFIG, {})
    state = read_json(STATUS, {"state": "idle", "message": "请先保存目录"})
    latest = report()
    metrics = latest.get("metrics", {}) if isinstance(latest, dict) else {}
    running, initialized, sampled = state.get("state") == "running", bool(config.get("initialized")), sample_ready(config)
    if running:
        action = "<p>正在运行；页面每 5 秒刷新。</p>"
    elif initialized:
        action = '<button data-mode="incremental">增量整理</button>'
    elif sampled:
        action = '<button data-mode="full">全量整理</button><p class="warn">全量前必须由你清空整理后目录；程序不会自动删除旧文件。</p>'
    else:
        action = '<button data-mode="sample">验证样本</button>'
    choices = "".join(f'<option value="{html.escape(str(path), quote=True)}">{html.escape(str(path))}</option>' for path in accessible_paths())
    labels = (("scanned_total", "扫描"), ("published", "发布"), ("exact_duplicate", "精确重复"), ("same_recording_duplicate", "同录音重复"), ("possible_version_groups", "疑似版本组"), ("lyrics_embedded", "歌词写入"), ("cover_embedded", "封面嵌入"), ("failed", "失败"))
    cards = "".join(f'<div class="card"><b>{int(metrics.get(key, 0))}</b><span>{label}</span></div>' for key, label in labels)
    rows = "".join(f'<tr><td>{html.escape(run["id"])}</td><td>{html.escape(run["mode"])}</td><td>{html.escape(run["status"])}</td><td>{html.escape(run["phase"])}</td></tr>' for run in latest_runs(8)) or '<tr><td colspan="4">暂无记录</td></tr>'
    source = html.escape(config.get("source_dir", DEFAULT_SOURCE), quote=True)
    output = html.escape(config.get("output_dir", DEFAULT_OUTPUT), quote=True)
    refresh = "setTimeout(()=>location.reload(),5000)" if running else ""
    return f'''<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>body{{font:16px system-ui;max-width:960px;margin:30px auto;padding:0 16px;color:#202124}}label,input,select,button{{display:block;width:100%;box-sizing:border-box;margin:8px 0}}button{{width:auto;padding:9px 14px}}.grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:8px}}.card{{padding:12px;background:#f4f5f7;border-radius:8px}}.card b,.card span{{display:block}}.card b{{font-size:24px}}table{{width:100%;border-collapse:collapse}}td,th{{border-bottom:1px solid #ddd;padding:7px;text-align:left}}.warn{{color:#9b4700}}</style><h1>音乐整理</h1><p>{html.escape(str(state.get("message", "")))}</p><form id="cfg"><label>原始文件夹（只读）</label><input id="source" value="{source}" required><select onchange="document.getElementById('source').value=this.value"><option value="">选择已授权目录</option>{choices}</select><label>整理后文件夹（读写）</label><input id="output" value="{output}" required><select onchange="document.getElementById('output').value=this.value"><option value="">选择已授权目录</option>{choices}</select><button>保存两个目录</button></form><h2>操作</h2>{action}<h2>运行摘要</h2><div class="grid">{cards}</div><p>应用状态：{state_size()/1024**2:.1f} MiB / 512 MiB。音频临时文件仅在整理后目录的本次运行期间存在，结束即清理。</p><h2>最近运行</h2><table><tr><th>ID</th><th>模式</th><th>状态</th><th>阶段</th></tr>{rows}</table><p><a href="/api/report">报告 JSON</a> · <a href="/api/log">运行日志</a></p><script>async function post(url,data){{let r=await fetch(url,{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify(data)}});if(!r.ok)alert(await r.text());else location.reload()}}document.getElementById('cfg').onsubmit=e=>{{e.preventDefault();post('/api/config',{{source_dir:source.value.trim(),output_dir:output.value.trim()}})}};document.querySelectorAll('[data-mode]').forEach(x=>x.onclick=()=>post('/api/run',{{mode:x.dataset.mode}}));{refresh}</script>'''


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_: object) -> None:
        pass
    def send(self, code: int, body: str, kind: str = "text/html; charset=utf-8") -> None:
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.end_headers()
        self.wfile.write(data)

    def is_authenticated(self, body: dict | None = None) -> bool:
        cfg = read_json(CONFIG, {})
        req_pwd = cfg.get("admin_password")
        if not req_pwd:
            return True
        # 1. Header
        given = self.headers.get("X-Admin-Password")
        if given and given == req_pwd:
            return True
        # 2. Query param (for browser downloads and links)
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        param_pwd = qs.get("admin_password", [None])[0] or qs.get("token", [None])[0]
        if param_pwd and param_pwd == req_pwd:
            return True
        # 3. Body (for POST requests)
        if body and isinstance(body, dict):
            body_pwd = body.get("admin_password")
            if body_pwd and body_pwd == req_pwd:
                return True
        return False

    def do_HEAD(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        sensitive_paths = {
            "/api/config", "/api/fs/ls", "/api/log", "/api/log/download",
            "/api/report", "/api/runs", "/api/accessible-paths",
            "/api/runs/details", "/api/unresolved"
        }
        if parsed.path in sensitive_paths:
            if not self.is_authenticated():
                self.send(401, json.dumps({"error": "控制台访问受限：管理密码不匹配", "require_password": True}, ensure_ascii=False), "application/json")
                return

        if parsed.path == "/api/config":
            cfg = get_or_init_config()
            resp = dict(cfg)
            resp["accessible_dirs"] = [str(p) for p in accessible_paths()]
            resp["storage_used_bytes"] = state_size()
            resp["storage_limit_bytes"] = 512 * 1024 * 1024
            s_raw, o_raw = cfg.get("source_dir", ""), cfg.get("output_dir", "")
            s_p = Path(s_raw) if s_raw else None
            o_p = Path(o_raw) if o_raw else None
            s_exists = bool(s_p and s_p.exists())
            s_read = bool(s_p and check_readable(s_p))
            o_exists = bool(o_p and (o_p.exists() or (o_p.parent.exists() and check_writable(o_p.parent))))
            o_write = bool(o_p and check_writable(o_p))
            resp["source_status"] = {"exists": s_exists, "readable": s_read}
            resp["output_status"] = {"exists": o_exists, "writable": o_write}
            self.send(200, json.dumps(resp, ensure_ascii=False), "application/json")
        elif parsed.path == "/api/status":
            st = read_json(STATUS, {"state": "idle"})
            if st.get("state") == "running":
                global GLOBAL_PIPELINE_PROC
                proc_dead = False
                if GLOBAL_PIPELINE_PROC is not None and GLOBAL_PIPELINE_PROC.poll() is not None:
                    proc_dead = True
                elif GLOBAL_PIPELINE_PROC is None and not LOCK.exists():
                    proc_dead = True
                if proc_dead:
                    GLOBAL_PIPELINE_PROC = None
                    try:
                        LOCK.unlink(missing_ok=True)
                    except OSError:
                        pass
                    st["state"] = "failed"
                    st["message"] = "检测到整理子进程异常终止，状态已自愈恢复"
                    write_json(STATUS, st)
            cfg = get_or_init_config()
            has_pwd = bool(cfg.get("admin_password"))
            st["has_admin_password"] = has_pwd
            if has_pwd and not self.is_authenticated():
                self.send(200, json.dumps({
                    "state": st.get("state", "idle"),
                    "message": "已启用管理密码保护，请输入密码解锁",
                    "has_admin_password": True,
                    "authenticated": False
                }, ensure_ascii=False), "application/json")
                return
            st["authenticated"] = True
            st["sample_gate_passed"] = sample_ready(cfg)
            st["has_initial_full_run"] = bool(cfg.get("initialized"))
            st["schedule"] = cfg.get("schedule", {})
            rep = report()
            runs = latest_runs(50)
            st["recent_runs"] = runs
            if rep:
                rep["recent_runs"] = runs
                st["report"] = rep
            else:
                st["report"] = {"recent_runs": runs}
            try:
                with connection() as db_conn:
                    unres_row = db_conn.execute("SELECT count(*) FROM source_inventory WHERE unresolvable=1 OR (disposition='failed' AND retry_count >= 2)").fetchone()
                    st["unresolvable_count"] = unres_row[0] if unres_row else 0
                    fb_row = db_conn.execute(f"SELECT count(*) FROM source_inventory WHERE disposition='published' AND {FALLBACK_OUTPUT_COND}").fetchone()
                    st["fallback_count"] = fb_row[0] if fb_row else 0
            except Exception:
                st["unresolvable_count"] = 0
                st["fallback_count"] = 0
            self.send(200, json.dumps(st, ensure_ascii=False), "application/json")
        elif parsed.path == "/api/report":
            self.send(200, json.dumps(report(parse_qs(parsed.query).get("run_id", [None])[0]), ensure_ascii=False), "application/json")
        elif parsed.path == "/api/runs":
            qs = parse_qs(parsed.query)
            try:
                limit = int(qs.get("limit", [50])[0])
            except ValueError:
                limit = 50
            self.send(200, json.dumps(latest_runs(limit), ensure_ascii=False), "application/json")
        elif parsed.path == "/api/runs/details":
            qs = parse_qs(parsed.query)
            run_id = qs.get("id", [""])[0].strip()
            if not run_id:
                self.send(400, "缺少 run_id 参数", "text/plain; charset=utf-8")
                return
            try:
                with connection() as db_conn:
                    run_row = db_conn.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
                    if not run_row:
                        self.send(404, "未找到该运行记录", "text/plain; charset=utf-8")
                        return
                    total_items_row = db_conn.execute("SELECT count(*) FROM items WHERE run_id=?", (run_id,)).fetchone()
                    total_items_count = total_items_row[0] if total_items_row else 0

                    items = db_conn.execute(
                        "SELECT source_path, source_size, source_kind, disposition, output_path, metadata_state, lyrics_state, cover_state, error FROM items WHERE run_id=? ORDER BY id ASC LIMIT 500",
                        (run_id,)
                    ).fetchall()

                    # Map duplicates to their matching winner/target
                    group_rows = db_conn.execute("SELECT winner_source_path, paths_json FROM groups WHERE run_id=?", (run_id,)).fetchall()
                    dup_map = {}
                    for gr in group_rows:
                        w = gr["winner_source_path"]
                        try:
                            paths = json.loads(gr["paths_json"])
                            if isinstance(paths, list):
                                for p in paths:
                                    if p != w:
                                        dup_map[p] = w
                        except Exception:
                            pass

                    item_dicts = []
                    for r in items:
                        d = dict(r)
                        if d["disposition"].startswith("duplicate_") and d["source_path"] in dup_map:
                            d["matched_target"] = dup_map[d["source_path"]]
                        item_dicts.append(d)

                    data = {
                        "run": dict(run_row),
                        "items": item_dicts,
                        "total_items": total_items_count,
                    }
                    if data["run"].get("summary_json"):
                        try:
                            data["run"]["summary"] = json.loads(data["run"]["summary_json"])
                        except Exception:
                            pass
                    summary = data["run"].get("summary") or {}
                    if not isinstance(summary, dict):
                        summary = {}

                    upg_row = db_conn.execute("SELECT count(*) FROM groups WHERE run_id=? AND decision='quality_upgrade'", (run_id,)).fetchone()
                    upg_count = upg_row[0] if upg_row else 0
                    fb_row = db_conn.execute(
                        f"SELECT count(*) FROM items WHERE run_id=? AND disposition IN ('published', 'sample_validated') AND {FALLBACK_OUTPUT_COND}",
                        (run_id,)
                    ).fetchone()
                    fallback_count = fb_row[0] if fb_row else 0

                    if not summary.get("metrics"):
                        disp_counts = {}
                        disp_rows = db_conn.execute("SELECT disposition, count(*) FROM items WHERE run_id=? GROUP BY disposition", (run_id,)).fetchall()
                        for r_d, r_c in disp_rows:
                            disp_counts[r_d] = r_c
                        summary["metrics"] = {
                            "scanned_total": total_items_count,
                            "published": disp_counts.get("published", 0),
                            "exact_duplicate": disp_counts.get("duplicate_exact", 0),
                            "same_recording_duplicate": disp_counts.get("duplicate_same_recording", 0),
                            "quality_upgraded": upg_count,
                            "failed": disp_counts.get("failed", 0),
                            "fallback_published": fallback_count,
                        }
                    else:
                        summary["metrics"]["quality_upgraded"] = upg_count
                        if "fallback_published" not in summary["metrics"]:
                            summary["metrics"]["fallback_published"] = fallback_count

                    data["run"]["summary"] = summary
                    self.send(200, json.dumps(data, ensure_ascii=False), "application/json")
            except Exception as exc:
                self.send(500, f"查询失败: {exc}", "text/plain; charset=utf-8")
        elif parsed.path == "/api/unresolved":
            try:
                with connection() as db_conn:
                    unresolved_rows = db_conn.execute(
                        "SELECT source_path, size_bytes, mtime_ns, disposition, output_path, retry_count, retry_reason, updated_at FROM source_inventory WHERE unresolvable=1 OR (disposition='failed' AND retry_count >= 2) ORDER BY updated_at DESC LIMIT 200"
                    ).fetchall()
                    fallback_rows = db_conn.execute(
                        f"SELECT source_path, size_bytes, mtime_ns, disposition, output_path, retry_count, retry_reason, updated_at FROM source_inventory WHERE disposition='published' AND {FALLBACK_OUTPUT_COND} ORDER BY updated_at DESC LIMIT 200"
                    ).fetchall()
                    self.send(200, json.dumps({
                        "unresolved": [dict(r) for r in unresolved_rows],
                        "fallbacks": [dict(r) for r in fallback_rows],
                    }, ensure_ascii=False), "application/json")
            except Exception as exc:
                self.send(500, f"查询失败: {exc}", "text/plain; charset=utf-8")
        elif parsed.path == "/api/accessible-paths":
            paths = [str(p) for p in accessible_paths()]
            self.send(200, json.dumps(paths, ensure_ascii=False), "application/json")
        elif parsed.path == "/api/fs/ls":
            qs = parse_qs(parsed.query)
            target_path_str = qs.get("path", [""])[0].strip()
            acc = accessible_paths()
            lang = self.headers.get("Accept-Language", "zh")
            
            # If no path specified, return the authorized roots
            if not target_path_str:
                roots_data = []
                for p in acc:
                    # Only top-level / authorized roots that don't have parents in acc
                    if not any(parent in acc for parent in p.parents):
                        roots_data.append({
                            "name": p.name or str(p),
                            "path": str(p),
                            "readable": check_readable(p),
                            "writable": check_writable(p),
                        })
                self.send(200, json.dumps({"current": "", "entries": roots_data}, ensure_ascii=False), "application/json")
                return

            target = Path(target_path_str).resolve(strict=False)
            if not authorized(target) or not target.is_dir():
                self.send(403, json.dumps({"error": get_error_message("unauthorized_path", lang), "entries": []}, ensure_ascii=False), "application/json")
                return

            entries = []
            target_writable = check_writable(target)
            target_readable = check_readable(target)
            try:
                raw_children = list(target.iterdir())
            except Exception as exc:
                self.send(500, json.dumps({"error": f"读取目录失败: {exc}", "entries": []}, ensure_ascii=False), "application/json")
                return

            valid_children = []
            for child in raw_children:
                try:
                    if child.name.startswith((".", "@")):
                        continue
                    if child.is_dir():
                        valid_children.append(child)
                except (OSError, PermissionError):
                    continue

            for child in sorted(valid_children, key=lambda c: c.name.lower()):
                try:
                    entries.append({
                        "name": child.name,
                        "path": str(child),
                        "readable": target_readable or check_readable(child),
                        "writable": target_writable or check_writable(child),
                    })
                except (OSError, PermissionError):
                    continue

            parent_path = str(target.parent) if authorized(target.parent) else ""
            self.send(200, json.dumps({
                "current": str(target),
                "parent": parent_path,
                "readable": target_readable,
                "writable": target_writable,
                "entries": entries,
            }, ensure_ascii=False), "application/json")
        elif parsed.path in ("/favicon.ico", "/favicon.png", "/icon.png"):
            icon_file = Path(__file__).with_name("favicon.ico" if parsed.path == "/favicon.ico" else "favicon.png")
            if not icon_file.is_file():
                icon_file = Path(__file__).with_name("favicon.png")
            if icon_file.is_file():
                ctype = "image/x-icon" if icon_file.suffix == ".ico" else "image/png"
                data = icon_file.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "public, max-age=86400")
                self.end_headers()
                self.wfile.write(data)
                return
            self.send(404, "Icon not found", "text/plain")
        elif parsed.path == "/api/sponsor-qr":
            qr_type = parse_qs(parsed.query).get("type", ["wechat"])[0]
            filename = "wechat_pay.png" if qr_type == "wechat" else "alipay_pay.png"
            qr_file = STATE / filename
            if not qr_file.is_file():
                alt = STATE / (filename.rsplit(".", 1)[0] + ".jpg")
                if alt.is_file():
                    qr_file = alt
            if not qr_file.is_file():
                app_dir = Path(__file__).parent
                qr_file = app_dir / filename
                if not qr_file.is_file():
                    alt = app_dir / (filename.rsplit(".", 1)[0] + ".jpg")
                    if alt.is_file():
                        qr_file = alt
            if qr_file.is_file():
                content_type = "image/png" if qr_file.suffix.lower() == ".png" else "image/jpeg"
                data = qr_file.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(data)
                return
            self.send(404, "QR not found", "text/plain; charset=utf-8")
        elif parsed.path == "/api/log":
            if not LOG.exists():
                self.send(200, "尚无日志\n", "text/plain; charset=utf-8")
                return
            try:
                content = LOG.read_text(encoding="utf-8")
                qs = parse_qs(parsed.query)
                tail_param = qs.get("tail", [None])[0]
                if tail_param:
                    try:
                        count = int(tail_param)
                        lines = content.splitlines()
                        if len(lines) > count:
                            content = f"... (已隐藏较早日志，仅展示最新 {count} 行；可点击「下载完整日志」导出全量记录)\n\n" + "\n".join(lines[-count:]) + "\n"
                    except ValueError:
                        pass
                self.send(200, content, "text/plain; charset=utf-8")
            except Exception as exc:
                self.send(500, f"读取日志异常: {exc}", "text/plain; charset=utf-8")
        elif parsed.path == "/api/log/download":
            if LOG.exists():
                try:
                    data = LOG.read_bytes()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream; charset=utf-8")
                    self.send_header("Content-Disposition", 'attachment; filename="musicmate-runtime.log"')
                    self.send_header("Content-Length", str(len(data)))
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    self.wfile.write(data)
                    return
                except Exception as exc:
                    self.send(500, f"下载日志异常: {exc}", "text/plain; charset=utf-8")
                    return
            self.send(404, "日志文件尚不存在", "text/plain; charset=utf-8")
        else:
            self.send(200, render_dashboard())

    def do_POST(self) -> None:
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8") or "{}")
        except (UnicodeDecodeError, json.JSONDecodeError):
            self.send(400, "请求格式错误")
            return

        if not isinstance(body, dict):
            self.send(400, "请求格式错误，必须为 JSON 对象")
            return

        if self.path == "/api/verify-password":
            if self.is_authenticated(body):
                self.send(200, json.dumps({"ok": True, "valid": True}, ensure_ascii=False), "application/json")
            else:
                self.send(401, json.dumps({"ok": False, "valid": False, "error": "管理密码不正确"}, ensure_ascii=False), "application/json")
            return

        if not self.is_authenticated(body):
            self.send(401, json.dumps({"error": "控制台访问受限：管理密码不匹配", "require_password": True}, ensure_ascii=False), "application/json")
            return

        if self.path == "/api/config":
            if read_json(STATUS, {}).get("state") == "running":
                self.send(409, "任务运行中，不能更改目录")
                return
            new = {
                "source_dir": str(body.get("source_dir", "")).strip(),
                "output_dir": str(body.get("output_dir", "")).strip(),
            }
            proxy_val = str(body.get("proxy", "")).strip()
            if proxy_val:
                new["proxy"] = proxy_val
            if "offline_mode" in body:
                new["offline_mode"] = bool(body.get("offline_mode"))
            if "admin_password" in body:
                p_val = str(body.get("admin_password", "")).strip()
                if p_val:
                    new["admin_password"] = p_val
            lang = self.headers.get("Accept-Language", "zh")
            valid, message = config_valid(new, lang)
            if not valid:
                self.send(400, message)
                return
            old = read_json(CONFIG, {})
            if old.get("source_dir") == new["source_dir"] and old.get("output_dir") == new["output_dir"]:
                for key in ("sample_verified", "initialized"):
                    if key in old:
                        new[key] = old[key]
            if "schedule" in old:
                new["schedule"] = old["schedule"]
            write_json(CONFIG, new)
            # Record mount roles so sync-accessible-mounts.sh can enforce :ro on source_dir
            try:
                roles_file = Path("/appdata/mount-roles.json")
                roles_file.parent.mkdir(parents=True, exist_ok=True)
                roles_file.write_text(json.dumps({
                    "source_dir": new.get("source_dir", ""),
                    "output_dir": new.get("output_dir", "")
                }, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass
            msg = "配置已保存，随时可以开始整理" if sample_ready(new) else "配置已保存；请先验证样本"
            write_json(STATUS, {"state": "idle", "message": msg})
            self.send(200, json.dumps(new, ensure_ascii=False), "application/json")
            return
        if self.path == "/api/run":
            lang = self.headers.get("Accept-Language", "zh")
            code, message = start_mode(str(body.get("mode", "")), lang)
            self.send(code, message, "application/json" if code == 202 else "text/plain; charset=utf-8")
            return
        if self.path == "/api/stop":
            stop_pipeline()
            self.send(200, json.dumps({"stopped": True, "message": "任务已停止"}, ensure_ascii=False), "application/json")
            return
        if self.path == "/api/log/clear":
            try:
                LOG.parent.mkdir(parents=True, exist_ok=True)
                LOG.write_text("", encoding="utf-8")
            except OSError:
                pass
            self.send(200, json.dumps({"cleared": True}, ensure_ascii=False), "application/json")
            return
        if self.path == "/api/reset":
            st = read_json(STATUS, {})
            if st.get("state") == "running" or is_pipeline_running():
                self.send(409, json.dumps({"ok": False, "error": "整理任务正在运行中，禁止执行系统重置", "code": "task_running"}, ensure_ascii=False), "application/json")
                return
            try:
                res = reset_system(
                    clear_history=bool(body.get("clear_history", True)),
                    clear_inventory=bool(body.get("clear_inventory", True)),
                    clear_kb=bool(body.get("clear_kb", body.get("clear_knowledge_base", False))),
                    clear_ncm=bool(body.get("clear_ncm", body.get("clear_ncm_cache", False)))
                )
                self.send(200, json.dumps(res, ensure_ascii=False), "application/json")
                return
            except Exception as exc:
                self.send(400, json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), "application/json")
                return
        if self.path == "/api/schedule":
            cfg = get_or_init_config()
            sched = cfg.get("schedule", {})
            enabled = bool(body.get("enabled", False))
            rule = str(body.get("rule", sched.get("rule", "daily"))).strip().lower()
            custom_time = str(body.get("custom_time", body.get("time", sched.get("custom_time", sched.get("time", "03:00"))))).strip()
            anchor_time = str(body.get("anchor_time", sched.get("anchor_time", "00:00"))).strip()
            interval_hours = int(body.get("interval_hours", sched.get("interval_hours", 6)))
            days = body.get("days", sched.get("days", [1, 2, 3, 4, 5, 6, 7]))
            lang = self.headers.get("Accept-Language", "zh")

            sched["enabled"] = enabled
            sched["rule"] = rule
            sched["time"] = custom_time
            sched["custom_time"] = custom_time
            sched["anchor_time"] = anchor_time
            sched["interval_hours"] = interval_hours
            sched["days"] = days
            sched["lang"] = lang

            if enabled:
                preview = get_schedule_preview(sched, get_local_now())
                sched["next_run"] = preview["next_run"]
                sched["points"] = preview["points"]
            else:
                sched["next_run"] = None
                sched["points"] = []

            cfg["schedule"] = sched
            write_json(CONFIG, cfg)
            msg = "定时设置已保存" if lang == "zh" else "Schedule settings saved"
            self.send(200, json.dumps({
                "ok": True,
                "schedule": sched,
                "message": msg
            }, ensure_ascii=False), "application/json")
            return
        if self.path == "/api/schedule/run-now":
            lang = self.headers.get("Accept-Language", "zh")
            code, message = start_mode("incremental", lang)
            self.send(code, message, "application/json" if code == 202 else "text/plain; charset=utf-8")
            return
        if self.path == "/api/unresolved/action":
            action = str(body.get("action", "")).strip()
            path_arg = str(body.get("path", "")).strip()
            lang = self.headers.get("Accept-Language", "zh")
            cfg = get_or_init_config()
            output_dir_str = str(cfg.get("output_dir", "")).strip()
            output_dir = Path(output_dir_str) if output_dir_str else None

            if action == "retry_all":
                with connection() as db_conn:
                    db_conn.execute("UPDATE source_inventory SET unresolvable=0, retry_count=0 WHERE unresolvable=1 OR (disposition='failed' AND retry_count >= 2)")
                    db_conn.commit()
                msg = "已重置疑难文件重试计数，下次增量整理将重新尝试" if lang == "zh" else "Unresolved retry counters reset"
                self.send(200, json.dumps({"ok": True, "message": msg}, ensure_ascii=False), "application/json")
                return
            elif action == "ignore_all":
                with connection() as db_conn:
                    db_conn.execute("UPDATE source_inventory SET unresolvable=1 WHERE disposition='failed' OR unresolvable=1")
                    db_conn.commit()
                msg = "已全部标记为忽略" if lang == "zh" else "All marked as ignored"
                self.send(200, json.dumps({"ok": True, "message": msg}, ensure_ascii=False), "application/json")
                return
            elif action == "fallback_publish_all":
                if not output_dir or not output_dir.is_dir():
                    self.send(400, "整理后目录无效或未配置", "text/plain; charset=utf-8")
                    return
                import pipeline
                published_count = 0
                with connection() as db_conn:
                    rows = db_conn.execute("SELECT source_path FROM source_inventory WHERE unresolvable=1 OR (disposition='failed' AND retry_count >= 2)").fetchall()
                for r in rows:
                    src_p = Path(r["source_path"])
                    if src_p.is_file():
                        try:
                            pipeline.fallback_publish_one(src_p, output_dir)
                            published_count += 1
                        except Exception:
                            pass
                msg = f"已将 {published_count} 首疑难曲目原样安全归档入库" if lang == "zh" else f"Published {published_count} unresolved tracks as fallback"
                self.send(200, json.dumps({"ok": True, "published_count": published_count, "message": msg}, ensure_ascii=False), "application/json")
                return
            elif action == "fallback_publish_one" and path_arg:
                if not output_dir or not output_dir.is_dir():
                    self.send(400, "整理后目录无效或未配置", "text/plain; charset=utf-8")
                    return
                import pipeline
                src_p = Path(path_arg)
                if src_p.is_file():
                    try:
                        target = pipeline.fallback_publish_one(src_p, output_dir)
                        msg = f"已原样安全入库至: {target.name if target else ''}" if lang == "zh" else "Published as fallback"
                        self.send(200, json.dumps({"ok": True, "target": str(target), "message": msg}, ensure_ascii=False), "application/json")
                        return
                    except Exception as exc:
                        self.send(500, json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), "application/json")
                        return
                self.send(400, "文件不存在", "text/plain; charset=utf-8")
                return
            self.send(400, "未知操作", "text/plain; charset=utf-8")
            return
        self.send(404, "不存在")


if __name__ == "__main__":
    STATE.mkdir(parents=True, exist_ok=True)
    start_scheduler()
    # Startup disaster recovery: clear stale lock files from abnormal restart/kill
    try:
        LOCK.unlink(missing_ok=True)
    except OSError:
        pass

    curr_status = read_json(STATUS, {})
    if curr_status.get("state") == "running":
        write_json(STATUS, {
            "state": "idle",
            "message": "服务已启动就绪（上次整理因服务重启已自动释放锁，可继续整理）"
        })
        try:
            with connection() as db_conn:
                db_conn.execute("UPDATE runs SET status='stopped',finished_at=CURRENT_TIMESTAMP WHERE status='running'")
                db_conn.commit()
        except Exception:
            pass
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
