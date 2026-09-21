#!/usr/bin/env python3
"""Bounded, source-preserving music pipeline for the fnOS two-directory app."""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import signal
import sqlite3
import struct
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def log(message: str) -> None:
    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now_str}] {message}", flush=True)


def _sigterm_handler(signum, frame):
    log("收到终止信号 (SIGTERM)，正在安全退出并触发沙箱清理...")
    raise SystemExit(1)


try:
    signal.signal(signal.SIGTERM, _sigterm_handler)
except (ValueError, AttributeError):
    pass

try:
    import mediafile
except ImportError:
    mediafile = None

try:
    import domestic_provider
except ImportError:
    domestic_provider = None

try:
    os.umask(0)
except Exception:
    pass

STATE = Path(os.environ.get("MUSIC_STATE", os.environ.get("MUSIC_DATA", "/state")))
CONFIG, STATUS, LEDGER, DUPSONIC_DB = STATE / "config.json", STATE / "status.json", STATE / "ledger-v6.sqlite", STATE / "dupsonic-v6.sqlite"
STATE_LIMIT = int(os.environ.get("MUSIC_STATE_LIMIT_BYTES", str(512 * 1024**2)))
SAFE_SIMILARITY, SAFE_DURATION = float(os.environ.get("MUSIC_SAFE_SIMILARITY", "0.997")), float(os.environ.get("MUSIC_SAFE_DURATION_DELTA", "4.0"))
AUDIO = {".mp3", ".flac", ".m4a", ".aac", ".ape", ".wav", ".ogg", ".opus", ".wma", ".aiff", ".wv", ".tta", ".mp4"}
FORMAT_TIERS: dict[str, int] = {
    ".dsd": 1100, ".dff": 1100, ".dsf": 1100,
    ".flac": 1000, ".alac": 950, ".ape": 950, ".wav": 900, ".aiff": 900,
    ".m4a": 600, ".aac": 580, ".opus": 550, ".ogg": 550,
    ".mp3": 400, ".wma": 300,
}
DB_WRITE_LOCK = threading.Lock()
RUN_PREFIX = ".music-rebuild-run-"
# Superseded (lower-quality) files are moved here instead of being deleted.  The
# leading dot keeps the directory out of list_sources(), out of the library scan
# and out of fnOS media indexing while still living on the same volume.
ARCHIVE_DIRNAME = ".music-archive"
# Bump this whenever a decision rule changes (dedupe tolerance, variant handling,
# matching, tagging, container tag repair …).  Every decision is stamped with the
# version that produced it, so when the rules move on the incremental gate can name
# exactly which past decisions are stale instead of guessing from file timestamps.
#   1 → 判定口径含 2 秒容差、Radio Edit 误互斥、镜像未装载国内源、WAV 标签被 INFO 遮蔽
#   2 → 容差 4.5 秒、附注词归一、国内源(QQ Smartbox/网易云)真正启用、WAV 标签修复
#   3 → 真实 Chromaprint 声波指纹比对、两阶段提交品质升级、存量曲库 SQLite 账本纳管、统一熔断机制
RULES_VERSION = 3
NO_EMBED_LYRICS_FORMATS = {".wav", ".aiff", ".aif"}
# Tracks that end up here were published without usable tags; the fnOS indexer
# cannot file them, so they are re-enriched on the next run.
UNKNOWN_ARTIST, UNKNOWN_ALBUM = "Unknown Artist", "Unknown Album"
FALLBACK_OUTPUT_COND = (
    "(output_path LIKE '%未知艺术家%' OR output_path LIKE '%Unknown Artist%' "
    "OR output_path LIKE '%未知专辑%' OR output_path LIKE '%未分类专辑%' OR output_path LIKE '%Unknown Album%' "
    "OR metadata_state IN ('metadata_not_found', 'fallback_tags'))"
)
VARIANT_WORDS = (
    "live", "remaster", "remastered", "acoustic", "radio edit", "radio-edit", " edit",
    "伴奏", "instrumental", "inst", "karaoke", "卡拉ok", "卡拉 ok", "纯音乐",
    "翻唱", "cover", "dj", "remix", "深情版", "钢琴版", "吉他版", "古筝版", "女声版", "男声版", "童声版"
)
DESTINATION_LOCK = threading.Lock()



def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def read_json(path: Path, fallback: dict) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def cfg() -> dict:
    return read_json(CONFIG, {})


def save_cfg(value: dict) -> None:
    atomic_json(CONFIG, value)


def directory_size(root: Path) -> int:
    total = 0
    if root.exists():
        for path in root.rglob("*"):
            if "runs" in path.parts:
                continue
            try:
                if path.is_file():
                    total += path.stat().st_size
            except OSError:
                pass
    return total


def assert_state_budget() -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    runs_dir = STATE / "runs"
    if runs_dir.is_dir():
        try:
            shutil.rmtree(runs_dir, ignore_errors=True)
        except Exception:
            pass

    forbidden = [
        path for path in STATE.rglob("*")
        if path.is_file()
        and "runs" not in path.parts
        and path.suffix.lower() in AUDIO | {".ncm", ".jpg", ".jpeg", ".png", ".webp"}
    ]
    if forbidden:
        raise RuntimeError(f"应用状态目录不得保存音频或图片：{forbidden[0]}")
    used = directory_size(STATE)
    if used > STATE_LIMIT:
        log(f"[资源保护 / Maintenance] 状态目录使用量接近上限 ({used / 1024**2:.1f} MiB / {STATE_LIMIT / 1024**2:.0f} MiB)，正在执行自动压缩与维护...")
        for db_file in [LEDGER, DUPSONIC_DB]:
            if db_file.is_file():
                try:
                    c = sqlite3.connect(db_file, timeout=10)
                    try:
                        c.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                        c.execute("DELETE FROM events WHERE created_at < datetime('now', '-14 days');")
                        c.commit()
                        c.execute("VACUUM;")
                    finally:
                        c.close()
                except Exception:
                    pass
        log_file = STATE / "rebuild.log"
        if log_file.is_file() and log_file.stat().st_size > 10 * 1024 * 1024:
            try:
                content = log_file.read_text(encoding="utf-8", errors="replace")
                lines = content.splitlines()
                log_file.write_text("\n".join(lines[-3000:]) + "\n", encoding="utf-8")
            except Exception:
                pass
        used = directory_size(STATE)
        if used > STATE_LIMIT:
            raise RuntimeError(f"应用状态目录大小 ({used / 1024**2:.1f} MiB) 超过上限 ({STATE_LIMIT / 1024**2:.0f} MiB)")



def update_state(message: str, **details: object) -> None:
    assert_state_budget()
    atomic_json(STATUS, {"updated_at": time.time(), "message": message, **details})
    log(message)


def workers() -> int:
    try:
        if hasattr(os, "sched_getaffinity"):
            cores = len(os.sched_getaffinity(0))
            if cores > 0:
                return cores
    except (OSError, ValueError):
        pass
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()[:2]
        if quota != "max":
            return max(1, int(int(quota) / int(period)))
    except (OSError, ValueError, ZeroDivisionError):
        pass
    try:
        quota = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").read_text().strip())
        period = int(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us").read_text().strip())
        if quota > 0 and period > 0:
            return max(1, quota // period)
    except (OSError, ValueError, ZeroDivisionError):
        pass
    return max(1, os.cpu_count() or 1)


def metadata_workers() -> int:
    raw = os.environ.get("MUSIC_METADATA_WORKERS", "").strip().lower()
    if raw and raw != "auto":
        try:
            val = int(raw)
            if val > 0:
                return val
        except (ValueError, TypeError):
            pass
    cores = workers()
    # Adaptive sizing: bounded between 2 and 8 to protect public APIs (MusicBrainz rate-limit)
    return max(2, min(8, cores))



def media(path: Path) -> bool:
    return path.suffix.lower() in AUDIO


def list_sources(root: Path) -> list[Path]:
    result: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if any(part.startswith(".") or part in {"隔离", "失败", "审核", "重复组"} for part in relative.parts):
            continue
        if media(path) or path.suffix.lower() == ".ncm":
            result.append(path)
    return sorted(result, key=lambda value: str(value).casefold())


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ffprobe_media(path: Path) -> tuple[float, int, dict[str, str], bool]:
    """Raw ffprobe view of a file (no mutagen fallback)."""
    result = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=duration,size:format_tags:stream_disposition", "-of", "json", str(path)], text=True, capture_output=True, errors="replace")
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "ffprobe rejected the file")
    try:
        payload = json.loads(result.stdout)
        fmt = payload["format"]
        duration, size = float(fmt.get("duration", 0)), int(float(fmt.get("size", 0)))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("ffprobe returned incomplete media data") from exc
    if duration <= 0 or size <= 0:
        raise RuntimeError("audio duration or size is zero")
    tags = {str(key).lower(): str(value).strip() for key, value in (fmt.get("tags") or {}).items()}
    artwork = any((stream.get("disposition") or {}).get("attached_pic") == 1 for stream in payload.get("streams", []))
    return duration, size, tags, artwork


def tag_is_unreadable(value: str | None) -> bool:
    """True when a tag is missing or decoded into placeholder characters.

    Legacy RIFF INFO chunks are codepage-encoded, so CJK text arrives as '?' (or
    U+FFFD) — indistinguishable from "no tag at all" as far as matching goes.
    """
    text = (value or "").strip()
    return not text or set(text) <= {"?", "�"}


def merge_embedded_tags(path: Path, tags: dict[str, str]) -> dict[str, str]:
    """Recover tags that ffprobe cannot see but mutagen can.

    ffprobe reports only the RIFF INFO chunk on WAV/AIFF and hides the ID3 chunk we
    write ourselves, so the pipeline would keep re-scraping files that are in fact
    already tagged.
    """
    if mediafile is None or not (tag_is_unreadable(tags.get("title")) or tag_is_unreadable(tags.get("artist"))):
        return tags
    try:
        embedded = mediafile.MediaFile(str(path))
    except Exception:
        return tags
    merged = dict(tags)
    for key, value in (
        ("title", embedded.title), ("artist", embedded.artist), ("album", embedded.album),
        ("albumartist", embedded.albumartist), ("track", embedded.track), ("date", embedded.year),
        ("lyrics", embedded.lyrics),
    ):
        text = "" if value is None else str(value).strip()
        if text and tag_is_unreadable(merged.get(key)):
            merged[key] = text
    return merged


def probe(path: Path) -> tuple[float, int, dict[str, str], bool]:
    duration, size, tags, artwork = ffprobe_media(path)
    return duration, size, merge_embedded_tags(path, tags), artwork


def strip_riff_chunk(path: Path, chunk_id: bytes) -> bool:
    """Drop one top-level RIFF chunk in place; the audio data is streamed, never rewritten."""
    try:
        source = path.open("rb")
    except OSError:
        return False
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex[:8]}.riff")
    removed, written = False, 0
    try:
        with source, temporary.open("wb") as target:
            header = source.read(12)
            if header[:4] != b"RIFF" or header[8:12] != b"WAVE":
                return False
            target.write(b"RIFF" + b"\0\0\0\0" + b"WAVE")
            MAX_SAFE_CHUNK = 200 * 1024 * 1024  # 200MB max per chunk to prevent memory exhaustion
            while True:
                chunk_header = source.read(8)
                if len(chunk_header) < 8:
                    break
                chunk_name = chunk_header[:4]
                size = struct.unpack("<I", chunk_header[4:])[0]
                total_to_read = size + (size & 1)
                if chunk_name == chunk_id:
                    if chunk_id == b"LIST":
                        sub_id = source.read(4)
                        source.seek(-4, os.SEEK_CUR)
                        if sub_id != b"INFO":
                            # Preserve non-INFO LIST chunks (e.g. LIST-adtl cue markers)
                            target.write(chunk_header)
                            written += 8
                            remaining = total_to_read
                            while remaining > 0:
                                read_len = min(remaining, 64 * 1024)
                                chunk_buf = source.read(read_len)
                                if not chunk_buf:
                                    break
                                target.write(chunk_buf)
                                written += len(chunk_buf)
                                remaining -= len(chunk_buf)
                            continue
                    removed = True
                    source.seek(total_to_read, os.SEEK_CUR)
                    continue
                target.write(chunk_header)
                written += 8
                # Stream chunk data in 64KB blocks instead of read(size) all at once
                remaining = total_to_read
                while remaining > 0:
                    read_len = min(remaining, 64 * 1024)
                    chunk_buf = source.read(read_len)
                    if not chunk_buf:
                        break
                    target.write(chunk_buf)
                    written += len(chunk_buf)
                    remaining -= len(chunk_buf)
            if not removed:
                return False
            target.seek(4)
            target.write(struct.pack("<I", written + 4))
    except (OSError, struct.error):
        return False
    finally:
        if not removed:
            temporary.unlink(missing_ok=True)
    os.replace(temporary, path)
    return True


def repair_shadowed_wav_tags(path: Path) -> bool:
    """Clear the stale RIFF INFO chunk that hides the ID3 tags of a WAV file.

    ffmpeg (and anything built on it) prefers the codepage-encoded INFO chunk when
    both exist, so freshly written title/artist/album stay invisible and the file
    keeps landing under Unknown Artist.  Dropping only that chunk leaves the audio
    and the ID3 (cover + lyrics) untouched and makes every reader agree.
    """
    if path.suffix.lower() != ".wav" or mediafile is None:
        return False
    try:
        _, _, visible, _ = ffprobe_media(path)
        embedded = mediafile.MediaFile(str(path))
    except Exception:
        return False
    if not (tag_is_unreadable(visible.get("title")) or tag_is_unreadable(visible.get("artist"))):
        return False
    if tag_is_unreadable(embedded.title) and tag_is_unreadable(embedded.artist):
        return False
    if strip_riff_chunk(path, b"LIST"):
        log(f"[格式 / Format] {path.name}: 已清除遮蔽 ID3 的 RIFF INFO 标签块，标签对飞牛/ffmpeg 可见")
        return True
    return False


def command(args: list[str], env: dict[str, str] | None = None, accepted: tuple[int, ...] = (0,), timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(args, text=True, capture_output=True, env=env, timeout=timeout, errors="replace")
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"命令超时：{' '.join(args[:2])} 超过 {timeout}s 未返回") from exc
    if result.returncode not in accepted:
        raise RuntimeError((result.stderr or result.stdout or "command failed").strip())
    return result


class ManagedConnection(sqlite3.Connection):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()


_INITIALIZED_DBS: set[str] = set()
_DB_INIT_LOCK = threading.Lock()


def init_db(ledger_path: Path | None = None) -> None:
    path = ledger_path or LEDGER
    key = str(path.resolve(strict=False))
    if key in _INITIALIZED_DBS:
        return
    with _DB_INIT_LOCK:
        if key in _INITIALIZED_DBS:
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, timeout=60)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=60000;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.executescript("""
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, mode TEXT NOT NULL, source_dir TEXT NOT NULL, output_dir TEXT NOT NULL, status TEXT NOT NULL, phase TEXT NOT NULL DEFAULT 'starting', phase_done INTEGER NOT NULL DEFAULT 0, phase_total INTEGER NOT NULL DEFAULT 0, started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, finished_at TEXT, summary_json TEXT NOT NULL DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), source_path TEXT NOT NULL, source_size INTEGER NOT NULL, source_mtime_ns INTEGER NOT NULL, source_sha256 TEXT, audio_sha256 TEXT, duration REAL, source_kind TEXT NOT NULL, disposition TEXT NOT NULL DEFAULT 'pending', output_path TEXT, metadata_state TEXT NOT NULL DEFAULT 'pending', lyrics_state TEXT NOT NULL DEFAULT 'pending', cover_state TEXT NOT NULL DEFAULT 'pending', error TEXT, UNIQUE(run_id, source_path));
            CREATE TABLE IF NOT EXISTS source_inventory (source_path TEXT PRIMARY KEY, size_bytes INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, sha256 TEXT NOT NULL, disposition TEXT NOT NULL, output_path TEXT, metadata_state TEXT, lyrics_state TEXT, cover_state TEXT, duration REAL, rules_version INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS groups (id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), tool TEXT NOT NULL, similarity REAL, winner_source_path TEXT, decision TEXT NOT NULL, paths_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), action TEXT NOT NULL, status TEXT NOT NULL, source_path TEXT, detail TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE IF NOT EXISTS knowledge_base (
                audio_sha256 TEXT PRIMARY KEY,
                artist TEXT NOT NULL,
                album TEXT NOT NULL,
                title TEXT NOT NULL,
                track_number TEXT,
                year INTEGER,
                lyrics TEXT,
                has_cover INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_kb_artist_title ON knowledge_base(artist, title);
            """)
            for col in ("metadata_state", "lyrics_state", "cover_state", "duration", "artist", "title", "retry_reason", "fingerprint", "title_key"):
                try:
                    conn.execute(f"ALTER TABLE source_inventory ADD COLUMN {col} TEXT")
                except sqlite3.OperationalError:
                    pass
            for col in ("fingerprint",):
                try:
                    conn.execute(f"ALTER TABLE items ADD COLUMN {col} TEXT")
                except sqlite3.OperationalError:
                    pass
            for col in ("norm_stem",):
                try:
                    conn.execute(f"ALTER TABLE knowledge_base ADD COLUMN {col} TEXT")
                except sqlite3.OperationalError:
                    pass
            for col in ("retry_count", "unresolvable"):
                try:
                    conn.execute(f"ALTER TABLE source_inventory ADD COLUMN {col} INTEGER NOT NULL DEFAULT 0")
                except sqlite3.OperationalError:
                    pass
            # Rows written before the stamp existed default to 0, i.e. "decided by rules
            # older than the current ones" — exactly what the incremental gate must redo.
            try:
                conn.execute("ALTER TABLE source_inventory ADD COLUMN rules_version INTEGER NOT NULL DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_sha256 ON source_inventory(sha256)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_output ON source_inventory(output_path)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_unres ON source_inventory(unresolvable)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_title_key ON source_inventory(title_key)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_inv_size ON source_inventory(size_bytes)")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_kb_norm_stem ON knowledge_base(norm_stem)")
            except sqlite3.OperationalError:
                pass
            conn.commit()
            _INITIALIZED_DBS.add(key)
        finally:
            conn.close()


def db() -> sqlite3.Connection:
    init_db()
    connection = sqlite3.connect(LEDGER, timeout=60, factory=ManagedConnection)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL;")
    connection.execute("PRAGMA busy_timeout=60000;")
    connection.execute("PRAGMA synchronous=NORMAL;")
    return connection


def compute_chromaprint(path: Path) -> tuple[float, list[int]]:
    """Compute Chromaprint uncompressed 32-bit integer array and duration using fpcalc."""
    real_path = path.resolve(strict=False)
    if not real_path.is_file():
        return 0.0, []
    try:
        res = subprocess.run(["fpcalc", "-length", "120", "-raw", "-json", str(real_path)], capture_output=True, text=True, timeout=30)
        if res.returncode == 0 and res.stdout.strip():
            data = json.loads(res.stdout)
            dur = float(data.get("duration", 0.0))
            fp = [int(x) for x in data.get("fingerprint", [])]
            return dur, fp
    except Exception as exc:
        log(f"[声纹 / Chromaprint] 计算异常 {path.name}: {exc}")
    return 0.0, []


def pack_fingerprint(fp: list[int]) -> str:
    if not fp:
        return ""
    try:
        import array, zlib, base64
        return base64.b64encode(zlib.compress(array.array('I', fp).tobytes())).decode('ascii')
    except Exception:
        return ""


def unpack_fingerprint(packed: str) -> list[int]:
    if not packed:
        return []
    try:
        import array, zlib, base64
        return list(array.array('I', zlib.decompress(base64.b64decode(packed))))
    except Exception:
        return []


def chromaprint_similarity(fp1: list[int], fp2: list[int]) -> float:
    """Compute normalized bit-level cross-correlation similarity between two Chromaprints."""
    if not fp1 or not fp2:
        return 0.0
    len1, len2 = len(fp1), len(fp2)
    max_offset = min(40, max(len1, len2) // 4)
    best_similarity = 0.0
    for offset in range(-max_offset, max_offset + 1):
        if offset >= 0:
            sub1 = fp1[offset:]
            sub2 = fp2
        else:
            sub1 = fp1
            sub2 = fp2[-offset:]
        match_len = min(len(sub1), len(sub2))
        min_match = min(25, min(len1, len2))
        if match_len < min_match:
            continue
        total_bits = match_len * 32
        diff_bits = sum((sub1[i] ^ sub2[i]).bit_count() for i in range(match_len))
        similarity = 1.0 - (diff_bits / total_bits)
        if similarity > best_similarity:
            best_similarity = similarity
            if best_similarity >= 0.98:
                break
    return best_similarity


def kb_get(connection: sqlite3.Connection, digest: str, artist: str = "", title: str = "", norm_stem: str = "") -> dict | None:
    if digest:
        row = connection.execute("SELECT * FROM knowledge_base WHERE audio_sha256=?", (digest,)).fetchone()
        if row:
            return dict(row)
    if artist and title:
        row = connection.execute("SELECT * FROM knowledge_base WHERE artist=? AND title=? AND lyrics IS NOT NULL AND lyrics != '' LIMIT 1", (artist, title)).fetchone()
        if row:
            return dict(row)
    if norm_stem:
        row = connection.execute("SELECT * FROM knowledge_base WHERE norm_stem=? AND lyrics IS NOT NULL AND lyrics != '' LIMIT 1", (norm_stem,)).fetchone()
        if row:
            return dict(row)
    return None


def kb_put(connection: sqlite3.Connection, digest: str, artist: str, album: str, title: str, track_number: str = "", year: int | None = None, lyrics: str | None = None, has_cover: bool = False, norm_stem: str = "") -> None:
    if not digest or not title:
        return
    with DB_WRITE_LOCK:
        connection.execute("""
        INSERT INTO knowledge_base(audio_sha256, artist, album, title, track_number, year, lyrics, has_cover, norm_stem, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(audio_sha256) DO UPDATE SET
            artist=CASE WHEN excluded.artist != '' THEN excluded.artist ELSE knowledge_base.artist END,
            album=CASE WHEN excluded.album != '' THEN excluded.album ELSE knowledge_base.album END,
            title=CASE WHEN excluded.title != '' THEN excluded.title ELSE knowledge_base.title END,
            track_number=CASE WHEN excluded.track_number != '' THEN excluded.track_number ELSE knowledge_base.track_number END,
            year=COALESCE(excluded.year, knowledge_base.year),
            lyrics=COALESCE(excluded.lyrics, knowledge_base.lyrics),
            has_cover=MAX(knowledge_base.has_cover, excluded.has_cover),
            norm_stem=COALESCE(excluded.norm_stem, knowledge_base.norm_stem),
            updated_at=CURRENT_TIMESTAMP
        """, (digest, artist or "", album or "", title, track_number or "", year, lyrics, 1 if has_cover else 0, norm_stem or None))
        try:
            connection.commit()
        except Exception:
            pass


def event(connection: sqlite3.Connection, run_id: str, action: str, status: str, source: Path | None = None, detail: str = "") -> None:
    connection.execute("INSERT INTO events(run_id,action,status,source_path,detail) VALUES(?,?,?,?,?)", (run_id, action, status, str(source) if source else None, detail[:4000]))


def update_phase(run_id: str, phase: str, done: int, total: int, message: str) -> None:
    with db() as connection:
        connection.execute("UPDATE runs SET phase=?,phase_done=?,phase_total=? WHERE id=?", (phase, done, total, run_id))
    update_state(message, state="running", run_id=run_id, phase=phase, phase_done=done, phase_total=total, state_bytes=directory_size(STATE))


def check_writable_path(path: Path) -> bool:
    try:
        cand = path.resolve(strict=False)
        # 1. If path exists and is a directory, probe write
        if cand.exists() and cand.is_dir():
            if os.access(cand, os.W_OK):
                return True
            probe = cand / f".perm_probe_{uuid.uuid4().hex[:6]}.tmp"
            try:
                probe.write_text("ok", encoding="utf-8")
                probe.unlink(missing_ok=True)
                return True
            except (OSError, PermissionError):
                pass
        # 2. If path doesn't exist, try mkdir probe
        if not cand.exists():
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
        # 3. Parent inheritance: nearest existing parent probe
        curr = cand if not cand.exists() else cand.parent
        while str(curr) != "/" and not curr.exists():
            curr = curr.parent
        if curr.exists():
            if os.access(curr, os.W_OK):
                return True
            probe = curr / f".perm_probe_{uuid.uuid4().hex[:6]}.tmp"
            try:
                probe.write_text("ok", encoding="utf-8")
                probe.unlink(missing_ok=True)
                return True
            except (OSError, PermissionError):
                pass
    except Exception:
        pass
    return False


def check_readable_path(path: Path) -> bool:
    try:
        cand = path.resolve(strict=False)
        if cand.exists():
            if os.access(cand, os.R_OK):
                return True
            try:
                next(cand.iterdir(), None)
                return True
            except (OSError, PermissionError):
                pass
        curr = cand.parent
        while str(curr) != "/" and not curr.exists():
            curr = curr.parent
        if curr.exists() and os.access(curr, os.R_OK):
            return True
    except Exception:
        pass
    return False


def validate_settings(settings: dict) -> tuple[Path, Path]:
    if not isinstance(settings, dict):
        raise RuntimeError("配置格式无效 / Invalid configuration format")
    if any(k in settings for k in ("seed_dir", "input_dir", "work_dir")):
        raise RuntimeError("旧版配置已不再支持：配置只能包含原始文件夹、整理后文件夹及可选代理")
    proxy = str(settings.get("proxy", "")).strip()
    if proxy:
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
            os.environ[k] = proxy
    source_raw, output_raw = settings.get("source_dir"), settings.get("output_dir")
    if not all(isinstance(value, str) and (value.startswith("/vol") or value.startswith("/music")) for value in (source_raw, output_raw)):
        raise RuntimeError("只能使用 /music/... 或真实 /volN/... 路径 / Only valid /music/... or /volN/... paths can be used")
    source, output = Path(source_raw), Path(output_raw)
    source_resolved, output_resolved = source.resolve(strict=False), output.resolve(strict=False)
    if source_resolved == output_resolved or source_resolved in output_resolved.parents or output_resolved in source_resolved.parents:
        raise RuntimeError("两个目录不能相同，也不能互相包含 / Source and output directories cannot be identical or nested")
    if not source.is_dir():
        raise RuntimeError(f"原始文件夹必须存在 / Source directory does not exist: {source}")
    if not check_readable_path(source):
        raise RuntimeError(f"原始文件夹不可读 / Source directory is not readable: {source}")

    # Auto-create output directory if it doesn't exist yet
    if not output.exists():
        try:
            output.mkdir(parents=True, exist_ok=True)
            log(f"[发布 / Publish] 自动创建整理后曲库目录 / Automatically created output directory: {output}")
        except OSError as exc:
            raise RuntimeError(f"无法创建整理后文件夹 / Cannot create output directory: {exc}")

    if not check_writable_path(output):
        raise RuntimeError(f"整理后文件夹不可读写 / Output directory is not writable: {output}")
    return source, output


def snapshot(path: Path, connection: sqlite3.Connection) -> tuple[int, int, str]:
    stat = path.stat()
    row = connection.execute("SELECT sha256 FROM source_inventory WHERE source_path=? AND size_bytes=? AND mtime_ns=?", (str(path), stat.st_size, stat.st_mtime_ns)).fetchone()
    if not row:
        row = connection.execute("SELECT audio_sha256 AS sha256 FROM items WHERE source_path=? AND source_size=? AND source_mtime_ns=? AND audio_sha256 IS NOT NULL ORDER BY id DESC LIMIT 1", (str(path), stat.st_size, stat.st_mtime_ns)).fetchone()
    return stat.st_size, stat.st_mtime_ns, str(row["sha256"]) if row else sha256(path)


def clone_file(source: Path, target: Path) -> None:
    if os.name != "nt":
        try:
            res = subprocess.run(["cp", "--reflink=auto", "-p", str(source), str(target)], capture_output=True)
            if res.returncode == 0:
                return
        except (OSError, subprocess.SubprocessError):
            pass
    shutil.copy2(source, target)


def copy_checked(source: Path, target: Path, digest: str | None = None) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.incoming")
    try:
        clone_file(source, temporary)
        if source.stat().st_size != temporary.stat().st_size or (digest or sha256(source)) != sha256(temporary):
            raise RuntimeError("copy verification failed")
        os.replace(temporary, target)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def decode_to_directory(source: Path, root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    private = root / f"decode-{uuid.uuid4().hex}"
    private.mkdir()
    try:
        command(["ncmdump", str(source), "-o", str(private)])
        files = [path for path in private.rglob("*") if path.is_file() and media(path)]
        if len(files) != 1:
            raise RuntimeError(f"ncmdump produced {len(files)} audio files")
        result = root / f"{source.stem[:80]}-{uuid.uuid4().hex[:12]}{files[0].suffix.lower()}"
        os.replace(files[0], result)
        probe(result)
        return result
    finally:
        shutil.rmtree(private, ignore_errors=True)


def json_array(text: str) -> list[dict]:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "[":
            continue
        try:
            value, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def analyse_regular(run_id: str, paths: list[Path]) -> dict[Path, dict]:
    output: dict[Path, dict] = {}
    connection = db()
    regular = [path for path in paths if media(path)]
    update_phase(run_id, "validate", 0, len(paths), f"正在校验 0/{len(regular)} 个未加密原始音频文件 (待解密 NCM 共 {len(paths) - len(regular)} 个)")
    def one(path: Path) -> tuple[Path, int, int, str, float]:
        with db() as local:
            size, mtime, digest = snapshot(path, local)
        duration, _, _, _ = probe(path)
        return path, size, mtime, digest, duration
    with ThreadPoolExecutor(max_workers=workers()) as pool:
        futures = {pool.submit(one, path): path for path in regular}
        for index, future in enumerate(as_completed(futures), 1):
            path = futures[future]
            try:
                item, size, mtime, digest, duration = future.result()
                output[item] = {"size": size, "mtime": mtime, "digest": digest, "duration": duration}
                connection.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,source_sha256,audio_sha256,duration,source_kind,disposition) VALUES(?,?,?,?,?,?,?,?,?)", (run_id, str(item), size, mtime, digest, digest, duration, "audio", "candidate"))
            except Exception as exc:
                try:
                    st = path.stat()
                    st_size, st_mtime = st.st_size, st.st_mtime_ns
                except OSError:
                    st_size, st_mtime = 0, 0
                output[path] = {"error": repr(exc)}
                connection.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,source_kind,disposition,error) VALUES(?,?,?,?,?,?,?)", (run_id, str(path), st_size, st_mtime, "audio", "failed", repr(exc)))
                event(connection, run_id, "validate", "failed", path, repr(exc))
                try:
                    connection.execute(
                        "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,retry_count,unresolvable,retry_reason,rules_version) "
                        "VALUES(?,?,?,'','failed',2,1,?,?) ON CONFLICT(source_path) DO UPDATE SET "
                        "size_bytes=excluded.size_bytes,mtime_ns=excluded.mtime_ns,disposition='failed',retry_count=2,unresolvable=1,retry_reason=excluded.retry_reason,rules_version=excluded.rules_version,updated_at=CURRENT_TIMESTAMP",
                        (str(path), st_size, st_mtime, repr(exc)[:200], RULES_VERSION)
                    )
                except Exception:
                    pass
            if index == len(futures) or index % 25 == 0:
                connection.commit()
                update_phase(run_id, "validate", index, len(paths), f"正在校验 {index}/{len(regular)} 个未加密原始音频文件 (待解密 NCM 共 {len(paths) - len(regular)} 个)")
    connection.commit()
    connection.close()
    return output


def scan_dupsonic(paths: list[Path]) -> None:
    if not paths:
        return
    chunk_size = 150
    for i in range(0, len(paths), chunk_size):
        chunk = paths[i:i + chunk_size]
        command(["dupsonic", "--db", str(DUPSONIC_DB), "scan", "-j", str(workers()), *map(str, chunk)])
    assert_state_budget()


def group_files(group: dict) -> list[dict]:
    return [item for item in group.get("files", []) if isinstance(item, dict) and isinstance(item.get("path"), str)]


def group_is_safe(group: dict) -> bool:
    try:
        similarity = float(group.get("similarity", 0))
    except (TypeError, ValueError):
        return False
    files = group_files(group)
    durations = [float(item["duration_secs"]) for item in files if item.get("duration_secs") is not None]
    variants = set()
    for item in files:
        tags = item.get("tags") if isinstance(item.get("tags"), dict) else {}
        text = normalize_track_stem(f"{Path(item.get('path', '')).stem} {tags.get('title', '')} {tags.get('album', '')}")
        variants.add(tuple(word for word in VARIANT_WORDS if word in text))
    return similarity >= SAFE_SIMILARITY and (not durations or max(durations) - min(durations) <= SAFE_DURATION) and len(variants) == 1


def dupsonic_keeper(anchor: Path) -> Path | None:
    result = command(["dupsonic", "--db", str(DUPSONIC_DB), "find-dupes", "--for", str(anchor), "--threshold", str(SAFE_SIMILARITY), "--details", "--keep", "best", "--exec", "true"], accepted=(0, 1))
    keepers = [Path(value.strip()) for value in re.findall(r"^\s*KEEP\s+(.+)$", result.stdout, flags=re.MULTILINE)]
    return keepers[0] if len(keepers) == 1 else None


def mark(connection: sqlite3.Connection, run_id: str, path: Path, disposition: str, error: str | None = None) -> None:
    connection.execute("UPDATE items SET disposition=?,error=COALESCE(?,error) WHERE run_id=? AND source_path=?", (disposition, error, run_id, str(path)))


def exact_dedupe(run_id: str, source: Path, candidates: set[Path], library: Path | None, decoded_map: dict[Path, Path] | None = None, run_root: Path | None = None, hash_cache: dict[Path, str] | None = None) -> set[Path]:
    total_cands = len(candidates)
    update_phase(run_id, "exact_dedupe", 0, total_cands or 1, f"正在进行完全相同文件去重 0/{total_cands}")
    if not candidates:
        update_phase(run_id, "exact_dedupe", 1, 1, "完全相同文件去重完成")
        return candidates

    if hash_cache is None:
        hash_cache = {}

    connection = db()
    rev_map: dict[str, Path] = {}
    if decoded_map:
        for orig, dec in decoded_map.items():
            rev_map[str(dec)] = orig
            rev_map[str(dec.resolve())] = orig

    # 1. 物理大小 (Size) 粗筛桶
    file_sizes: dict[Path, int] = {}
    size_to_cands: dict[int, list[Path]] = {}
    for p in candidates:
        real_p = decoded_map.get(p, p) if decoded_map else p
        try:
            sz = real_p.stat().st_size
            file_sizes[p] = sz
            size_to_cands.setdefault(sz, []).append(p)
        except OSError:
            pass

    library_sizes: set[int] = set()
    if library and library.exists():
        try:
            sz_rows = connection.execute("SELECT DISTINCT size_bytes FROM source_inventory WHERE disposition='published' AND size_bytes > 0").fetchall()
            library_sizes = {r[0] for r in sz_rows}
        except Exception:
            pass

    def get_hash(p: Path, real_p: Path) -> str:
        if p in hash_cache:
            return hash_cache[p]
        if real_p in hash_cache:
            h = hash_cache[real_p]
            hash_cache[p] = h
            return h
        h = sha256(real_p)
        hash_cache[p] = h
        hash_cache[real_p] = h
        return h

    seen_hashes: dict[str, Path] = {}
    to_discard = []
    ordered_cands = sorted(candidates, key=lambda value: str(value).casefold())

    done_count = 0
    for p in ordered_cands:
        done_count += 1
        real_p = decoded_map.get(p, p) if decoded_map else p
        if not real_p.is_file():
            continue
        sz = file_sizes.get(p, 0)
        has_internal_collision = len(size_to_cands.get(sz, [])) >= 2
        has_library_collision = bool(library_sizes and sz in library_sizes)

        # 仅对存在物理大小碰撞嫌疑的文件计算或提取哈希
        if has_internal_collision or has_library_collision:
            h = get_hash(p, real_p)
            if has_internal_collision:
                if h in seen_hashes:
                    winner = seen_hashes[h]
                    to_discard.append((p, winner, h))
                else:
                    seen_hashes[h] = p

        if done_count == total_cands or done_count % 25 == 0:
            update_phase(run_id, "exact_dedupe", done_count, total_cands, f"正在进行完全相同文件去重 {done_count}/{total_cands}")

    for p, winner, h in to_discard:
        candidates.discard(p)
        mark(connection, run_id, p, "duplicate_exact")
        group_id = f"exact-{h}"
        connection.execute(
            "INSERT OR REPLACE INTO groups(id,run_id,tool,similarity,winner_source_path,decision,paths_json) VALUES(?,?,?,?,?,?,?)",
            (group_id, run_id, "indexed_sha256", 1.0, str(winner), "keep", json.dumps([str(winner), str(p)], ensure_ascii=False))
        )

    # 2. Candidate vs Library deduplication using SQLite source_inventory index
    if library and library.exists() and library_sizes:
        for p in list(candidates):
            sz = file_sizes.get(p, 0)
            if sz not in library_sizes:
                continue
            real_p = decoded_map.get(p, p) if decoded_map else p
            if not real_p.is_file():
                continue
            h = get_hash(p, real_p)
            row = connection.execute(
                "SELECT output_path, source_path FROM source_inventory WHERE sha256=? AND disposition='published' AND output_path IS NOT NULL LIMIT 1",
                (h,)
            ).fetchone()
            if row and row["output_path"]:
                out_p = Path(row["output_path"])
                if out_p.is_file() and row["source_path"] != str(p):
                    candidates.discard(p)
                    mark(connection, run_id, p, "duplicate_exact")
                    group_id = f"exact-{h}"
                    connection.execute(
                        "INSERT OR REPLACE INTO groups(id,run_id,tool,similarity,winner_source_path,decision,paths_json) VALUES(?,?,?,?,?,?,?)",
                        (group_id, run_id, "indexed_sha256", 1.0, str(out_p), "keep", json.dumps([str(out_p), str(p)], ensure_ascii=False))
                    )

    connection.commit()
    connection.close()
    update_phase(run_id, "exact_dedupe", total_cands, total_cands, "完全相同文件去重完成 (基于账本索引快速比对)")
    return candidates


def normalize_track_stem(stem: str) -> str:
    s = stem.replace("（", "(").replace("）", ")").replace("［", "[").replace("］", "]").replace("【", "[").replace("】", "]")
    s = s.replace("？", "?").replace("！", "!").replace("，", ",").replace("：", ":").replace("、", ",").replace("；", ";")
    s = s.replace("－", "-").replace("–", "-").replace("—", "-").replace("　", " ")
    s = re.sub(r"\s*-\s*副本(?:\s*\(\d+\))?$", "", s)
    s = re.sub(r"\s*\(\d+\)$", "", s)
    s = re.sub(r"[\[\(][^\]\)]*?(?:flac|ape|wav|alac|aiff|mp3|aac|m4a|ogg|opus|wma|320k|128k|192k|24bit|16bit|96k|44\.1k|hi-res|hires|mqms2|kbps)[^\]\)]*?[\]\)]", "", s, flags=re.IGNORECASE)
    s = re.sub(r"[\[\(]\s*(?:radio[\s\-_]*edit|single\s*version|album\s*version|deluxe|bonus\s*track|clean\s*version|explicit|original\s*mix|remaster(?:ed)?)\s*[\]\)]", "", s, flags=re.IGNORECASE)
    s = re.sub(r"(?:www\.[a-z0-9\-\.]+\.[a-z]{2,4})", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip(" .-_")
    return s.casefold()


def extract_artist_and_title(stem: str) -> tuple[str, str]:
    """Split "歌手 - 歌名" (or a bare "歌名") out of a file name.

    Used both as a clustering key and as the last-resort metadata source for files
    whose container carries no usable tags at all.
    """
    cleaned = normalize_track_stem(stem)
    cleaned = re.sub(r"^\d{1,3}\s*[-_\.]\s*", "", cleaned)
    parts = re.split(r"\s+-\s+", cleaned, maxsplit=1)
    if len(parts) == 2 and parts[0].strip() and parts[1].strip():
        return parts[0].strip(), parts[1].strip()
    # "周杰伦 - 晴天" or "周杰伦-晴天":
    # If unspaced dash is used (e.g. "周杰伦-晴天"), require that the left side looks like an artist name
    # and not common Chinese structural / subtitle words (如 "第x集/篇/章", "序曲", "上/下篇", "变奏")
    if cleaned.count("-") == 1:
        left, right = (side.strip() for side in cleaned.split("-", 1))
        structural_words = ("第", "篇", "章", "部", "卷", "集", "序", "尾声", "变奏", "插曲", "片头", "片尾", "主题")
        is_structural = any(left.startswith(w) or left.endswith(w) for w in structural_words)
        if left and right and re.search(r"[\u4e00-\u9fff]", left) and re.search(r"[\u4e00-\u9fff]", right) and len(left) <= 8 and not is_structural:
            return left, right
    return "", cleaned.strip()


def simplify_chinese(text: str) -> str:
    """Fold traditional spellings onto their simplified form for comparison.

    A library that mixes 周杰倫 (Taiwan/HK release) with 周杰伦 (mainland release)
    would otherwise file the same recording twice.
    """
    if not text or domestic_provider is None:
        return text
    try:
        return domestic_provider.to_simplified(text)
    except Exception:
        return text


def normalize_artist_name(artist: str) -> str:
    s = re.sub(r"[,/&、+;]|(?:\s+(?:feat\.?|ft\.?|and|vs\.?)\s+)", " ", simplify_chinese(artist).casefold())
    s = re.sub(r"[^\w\u4e00-\u9fff]", "", s)
    return s.strip()


def artists_compatible(art1: str, art2: str) -> bool:
    if not art1 or not art2:
        return True
    n1, n2 = normalize_artist_name(art1), normalize_artist_name(art2)
    if not n1 or not n2:
        return True
    return n1 == n2 or n1 in n2 or n2 in n1


def hidden_under(path: Path, root: Path) -> bool:
    """True when any path component below `root` is hidden (e.g. .music-archive)."""
    try:
        relative = path.relative_to(root)
    except ValueError:
        return False
    return any(part.startswith(".") for part in relative.parts)


def audio_quality_score(path: Path, decoded_map: dict[Path, Path] | None = None) -> tuple[int, int, str]:
    real_path = decoded_map.get(path, path) if decoded_map else path
    ext = real_path.suffix.lower()
    tier = FORMAT_TIERS.get(ext, 200)
    try:
        size = real_path.stat().st_size
    except OSError:
        size = 0
    return (tier, size, str(real_path).casefold())


def path_key(path: Path) -> str:
    """Stable per-path identifier.  Hashing the file *content* would collide for two
    different tracks that happen to hold identical bytes, silently merging their
    ledger rows."""
    return hashlib.sha256(str(path).casefold().encode("utf-8")).hexdigest()[:16]


def archive_superseded(path: Path, output: Path, connection: sqlite3.Connection | None = None) -> Path | None:
    """Move a superseded library file (plus its sidecar) into the hidden archive.

    The original source file is never touched, so the library copy can always be
    regenerated; keeping it archived rather than deleted makes an upgrade auditable
    and reversible.  The ledger row is re-pointed so incremental runs neither
    re-publish nor lose track of it.
    """
    try:
        relative = path.relative_to(output)
    except ValueError:
        return None
    archived = output / ARCHIVE_DIRNAME / relative
    try:
        archived.parent.mkdir(parents=True, exist_ok=True)
        if archived.exists():
            archived = archived.with_name(f"{archived.stem}-{uuid.uuid4().hex[:6]}{archived.suffix}")
        os.replace(path, archived)
    except OSError as exc:
        log(f"[品质升级 / Upgrade] 旧文件归档失败 {path}: {exc}")
        return None
    sidecar = path.with_suffix(".lrc")
    if sidecar.is_file():
        try:
            os.replace(sidecar, archived.with_suffix(".lrc"))
        except OSError:
            pass
    # Update database ledger to reflect the move. Retry on transient locks.
    # If the ledger cannot be updated, roll back the physical file move to prevent orphaned records.
    stamp = "UPDATE source_inventory SET disposition='superseded', output_path=?, rules_version=?, updated_at=CURRENT_TIMESTAMP WHERE output_path=?"
    params = (str(archived), RULES_VERSION, str(path))
    db_updated = False
    for attempt in range(5):
        try:
            if connection is not None:
                connection.execute(stamp, params)
            else:
                with db() as own_connection:
                    own_connection.execute(stamp, params)
            db_updated = True
            break
        except sqlite3.OperationalError:
            time.sleep(0.1 * (attempt + 1))
        except sqlite3.Error as exc:
            log(f"[品质升级 / Upgrade] 账本记录更新失败 {path}: {exc}")
            break

    if not db_updated:
        log(f"[品质升级 / Upgrade] 数据库未同步，回滚文件移动: {archived} -> {path}")
        try:
            os.replace(archived, path)
            if sidecar.is_file() and archived.with_suffix(".lrc").is_file():
                os.replace(archived.with_suffix(".lrc"), sidecar)
        except OSError as exc:
            log(f"[品质升级 / Upgrade] 回滚文件移动失败: {exc}")
        return None

    return archived


def apply_quality_upgrade(winner: Path, sub_group: list[Path], own: list[Path], output: Path | None, candidates: set[Path], decoded_map: dict[Path, Path] | None, connection: sqlite3.Connection, run_id: str) -> Path:
    """Smart Quality Upgrade.

    A newly imported file that outranks the library copy takes its place, and every
    lower-quality *library* copy in the same group is moved into the hidden archive
    instead of being left behind in the published tree.
    """
    if output is None or not own or not sub_group:
        return winner
    if winner not in candidates and winner.is_file():
        best_candidate = max(own, key=lambda p: audio_quality_score(p, decoded_map))
        if audio_quality_score(best_candidate, decoded_map) <= audio_quality_score(winner, decoded_map):
            return winner
        winner = best_candidate
    superseded = [p for p in sub_group if p != winner and p not in candidates and p.is_file() and (audio_quality_score(p, decoded_map) < audio_quality_score(winner, decoded_map) or (winner in candidates and audio_quality_score(p, decoded_map) == audio_quality_score(winner, decoded_map)))]
    if not superseded:
        return winner
    group_id = f"upgrade-{path_key(superseded[0])}"
    archived = [str(path) for path in (archive_superseded(path, output, connection) for path in superseded) if path]
    detail = json.dumps({"winner": str(winner), "superseded": [str(p) for p in superseded], "archived": archived}, ensure_ascii=False)
    for path in superseded:
        log(f"[品质升级 / Upgrade] 库中 {path.name} ({path.suffix}) 将被更优版本 {winner.name} ({winner.suffix}) 取代，移入 {ARCHIVE_DIRNAME}")
    connection.execute(
        "INSERT OR REPLACE INTO groups(id,run_id,tool,similarity,winner_source_path,decision,paths_json) VALUES(?,?,?,?,?,?,?)",
        (group_id, run_id, "quality_upgrade", 1.0, str(winner), "quality_upgrade", detail),
    )
    event(connection, run_id, "quality_upgrade", "ok", superseded[0], detail)
    return winner


def has_incompatible_variants(files: list[Path]) -> bool:
    variants = set()
    for p in files:
        # Compare on the normalised stem so that packaging annotations
        # (Radio Edit / Single Version / Remaster) fall into the same bucket as the
        # plain title, while genuinely different recordings (Live / Acoustic) stay
        # separated.
        text = normalize_track_stem(p.stem)
        found = tuple(word for word in VARIANT_WORDS if word in text)
        variants.add(found)
    return len(variants) > 1


def prune_dupsonic_ghosts() -> None:
    if DUPSONIC_DB.exists():
        try:
            conn = sqlite3.connect(DUPSONIC_DB, timeout=5)
            conn.execute("DELETE FROM files WHERE path LIKE ? OR path LIKE ?", (f"%{RUN_PREFIX}%", "%/tmp/%"))
            conn.commit()
            conn.execute("VACUUM")
            conn.close()
        except Exception:
            pass


def acoustic_dedupe(run_id: str, source: Path, candidates: set[Path], library: Path | None, decoded_map: dict[Path, Path] | None = None, run_root: Path | None = None) -> set[Path]:
    update_phase(run_id, "acoustic_dedupe", 0, 1, "正在进行跨格式同录音识别")
    if not candidates:
        update_phase(run_id, "acoustic_dedupe", 1, 1, "跨格式同录音识别完成")
        return candidates

    if os.environ.get("MUSIC_ENABLE_DUPSONIC") == "1":
        roots = [source] + ([library] if library and library.exists() else [])
        if run_root and (run_root / "ncm").is_dir():
            roots.append(run_root / "ncm")
        scan_dupsonic(roots)
        output = command(["dupsonic", "--db", str(DUPSONIC_DB), "find-dupes", "--threshold", str(SAFE_SIMILARITY), "--details", "--format", "json"], accepted=(0, 1)).stdout
        connection = db()
        rev_map: dict[str, Path] = {}
        if decoded_map:
            for orig, dec in decoded_map.items():
                rev_map[str(dec)] = orig
                rev_map[str(dec.resolve())] = orig

        for group in json_array(output):
            raw_paths = [Path(item["path"]) for item in group_files(group)]
            mapped_paths = [rev_map.get(str(p), rev_map.get(str(p.resolve()), p)) for p in raw_paths]
            if library:
                mapped_paths = [p for p in mapped_paths if not hidden_under(p, library)]
            own = [p for p in mapped_paths if p in candidates]
            if not own or len(mapped_paths) < 2:
                continue
            raw_winner = dupsonic_keeper(raw_paths[0]) if group_is_safe(group) else None
            winner = rev_map.get(str(raw_winner), rev_map.get(str(raw_winner.resolve()), raw_winner)) if raw_winner else None
            decision = "keep_dupsonic_best" if winner in mapped_paths else "keep_all_ambiguous"
            if decision == "keep_dupsonic_best":
                winner = apply_quality_upgrade(winner, mapped_paths, own, library, candidates, decoded_map, connection, run_id)
                for p in own:
                    if p != winner:
                        candidates.discard(p)
                        mark(connection, run_id, p, "duplicate_same_recording")
            group_id = str(group.get("id") or hashlib.sha256(json.dumps(group, sort_keys=True).encode()).hexdigest())
            connection.execute("INSERT OR REPLACE INTO groups(id,run_id,tool,similarity,winner_source_path,decision,paths_json) VALUES(?,?,?,?,?,?,?)", (group_id, run_id, "dupsonic", float(group.get("similarity", 0) or 0), str(winner) if winner else None, decision, json.dumps([str(p) for p in mapped_paths], ensure_ascii=False)))
        connection.commit()
        connection.close()
        update_phase(run_id, "acoustic_dedupe", 1, 1, "跨格式同录音识别完成")
        return candidates

    # Hierarchical Candidate Blocking + Chromaprint Waveform Fingerprinting
    connection = db()
    dur_rows = connection.execute("SELECT source_path, duration, fingerprint FROM items WHERE run_id=?", (run_id,)).fetchall()
    durations: dict[str, float] = {row["source_path"]: float(row["duration"] or 0) for row in dur_rows}
    fingerprint_cache: dict[str, list[int]] = {}
    for row in dur_rows:
        if row["fingerprint"]:
            unp = unpack_fingerprint(row["fingerprint"])
            if unp:
                fingerprint_cache[row["source_path"]] = unp

    file_metadata: dict[str, tuple[str, str]] = {}
    buckets: dict[str, list[Path]] = {}

    # 1. Index candidate files first
    cand_title_keys: set[str] = set()
    for p in candidates:
        stem_art, stem_tit = extract_artist_and_title(p.stem)
        tag_art, tag_tit = "", ""
        real_p = decoded_map.get(p, p) if decoded_map else p
        if real_p.is_file():
            if str(p) not in durations or durations[str(p)] <= 0:
                try:
                    d, _, tags, _ = probe(real_p)
                    durations[str(p)] = d
                    durations[str(real_p)] = d
                    tag_art = tags.get("artist") or tags.get("album_artist", "")
                    tag_tit = tags.get("title", "")
                except Exception:
                    pass
        final_art = tag_art or stem_art
        final_tit = tag_tit or stem_tit or p.stem
        file_metadata[str(p)] = (final_art, final_tit)
        title_key = re.sub(r"[^\w\u4e00-\u9fff]", "", simplify_chinese(final_tit).casefold()) or final_tit.casefold() or p.stem.casefold()
        cand_title_keys.add(title_key)
        buckets.setdefault(title_key, []).append(p)

    # 2. Query library files exclusively via SQLite ledger index (Chunked WHERE title_key IN (...))
    if library and library.exists() and cand_title_keys:
        cand_keys_list = list(cand_title_keys)
        chunk_size = 500
        inv_rows = []
        for i in range(0, len(cand_keys_list), chunk_size):
            chunk = cand_keys_list[i:i + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"SELECT output_path, duration, COALESCE(artist, ''), COALESCE(title, ''), COALESCE(fingerprint, ''), COALESCE(title_key, '') "
                f"FROM source_inventory WHERE title_key IN ({placeholders}) AND output_path IS NOT NULL AND disposition='published'",
                chunk
            ).fetchall()
            inv_rows.extend(rows)

        # 兼容老版本或未建立 title_key 索引的历史曲目（若全库已索引，此处命中为 0 行，几乎零开销）
        legacy_rows = connection.execute(
            "SELECT output_path, duration, COALESCE(artist, ''), COALESCE(title, ''), COALESCE(fingerprint, ''), COALESCE(title_key, '') "
            "FROM source_inventory WHERE (title_key IS NULL OR title_key = '') AND output_path IS NOT NULL AND disposition='published'"
        ).fetchall()
        inv_rows.extend(legacy_rows)

        for row in inv_rows:
            out_str = row[0]
            if not out_str:
                continue
            out_p = Path(out_str)
            dur = float(row[1] or 0)
            art, tit = row[2], row[3]
            fp_packed = row[4]
            title_key = row[5]
            if not title_key:
                if not tit:
                    art, tit = extract_artist_and_title(out_p.stem)
                title_key = re.sub(r"[^\w\u4e00-\u9fff]", "", simplify_chinese(tit).casefold()) or tit.casefold() or out_p.stem.casefold()
            if out_p.is_file():
                durations[str(out_p)] = dur
                file_metadata[str(out_p)] = (art, tit)
                if fp_packed and str(out_p) not in fingerprint_cache:
                    unp = unpack_fingerprint(fp_packed)
                    if unp:
                        fingerprint_cache[str(out_p)] = unp
                buckets.setdefault(title_key, []).append(out_p)

    # 3. Clustering with Chromaprint Acoustic Verification
    for key, group in buckets.items():
        if len(group) < 2:
            continue

        clusters: list[list[Path]] = []
        for p in group:
            p_dur = durations.get(str(p), 0.0)
            p_art, _ = file_metadata.get(str(p), (extract_artist_and_title(p.stem)[0], ""))
            placed = False
            for cluster in clusters:
                can_join = True
                for c in cluster:
                    c_dur = durations.get(str(c), 0.0)
                    c_art, _ = file_metadata.get(str(c), (extract_artist_and_title(c.stem)[0], ""))
                    # Physical duration gatekeeper and artist compatibility check
                    if (p_dur > 0 and c_dur > 0 and abs(p_dur - c_dur) > SAFE_DURATION) or not artists_compatible(p_art, c_art):
                        can_join = False
                        break

                    # Chromaprint Acoustic Fingerprint Confirmation
                    dur_diff = abs(p_dur - c_dur) if (p_dur > 0 and c_dur > 0) else 0.0
                    if can_join and dur_diff > 0.4:
                        fp_p = fingerprint_cache.get(str(p))
                        if fp_p is None:
                            real_p = decoded_map.get(p, p) if decoded_map else p
                            _, fp_p = compute_chromaprint(real_p)
                            fingerprint_cache[str(p)] = fp_p
                            if fp_p:
                                packed = pack_fingerprint(fp_p)
                                connection.execute("UPDATE items SET fingerprint=? WHERE run_id=? AND source_path=?", (packed, run_id, str(p)))
                        fp_c = fingerprint_cache.get(str(c))
                        if fp_c is None:
                            real_c = decoded_map.get(c, c) if decoded_map else c
                            _, fp_c = compute_chromaprint(real_c)
                            fingerprint_cache[str(c)] = fp_c
                            if fp_c:
                                packed = pack_fingerprint(fp_c)
                                connection.execute("UPDATE items SET fingerprint=? WHERE run_id=? AND source_path=?", (packed, run_id, str(c)))
                        if fp_p and fp_c:
                            sim = chromaprint_similarity(fp_p, fp_c)
                            if sim < 0.72:
                                # Dissimilar waveforms: genuine different recordings
                                can_join = False
                                break
                if can_join:
                    cluster.append(p)
                    placed = True
                    break
            if not placed:
                clusters.append([p])

        for sub_group in clusters:
            if len(sub_group) < 2:
                continue
            own = [p for p in sub_group if p in candidates]
            if not own:
                continue

            durs = [durations.get(str(p), 0.0) for p in sub_group if durations.get(str(p), 0.0) > 0]
            is_safe_duration = (len(durs) == len(sub_group)) and (max(durs) - min(durs) <= SAFE_DURATION)
            is_safe_variant = not has_incompatible_variants(sub_group)
            duration_spread = (max(durs) - min(durs)) if durs else 999
            is_near_identical = is_safe_duration and duration_spread <= 0.5

            if is_safe_duration and (is_safe_variant or is_near_identical):
                winner = max(sub_group, key=lambda p: audio_quality_score(p, decoded_map))
                # Two-Phase Staged Quality Upgrade
                winner = apply_quality_upgrade(winner, sub_group, own, library, candidates, decoded_map, connection, run_id)
                group_id = f"blocking-{path_key(winner)}" if winner.is_file() else f"blocking-{uuid.uuid4().hex}"
                connection.execute(
                    "INSERT OR REPLACE INTO groups(id,run_id,tool,similarity,winner_source_path,decision,paths_json) VALUES(?,?,?,?,?,?,?)",
                    (group_id, run_id, "hierarchical_blocking", 1.0, str(winner), "keep_best", json.dumps([str(p) for p in sub_group], ensure_ascii=False))
                )
                for p in own:
                    if p != winner:
                        # Only discard/mark duplicate if winner is a genuinely distinct source/library item
                        winner_owner = recorded_source_for(winner)
                        if winner_owner and winner_owner == p:
                            continue
                        candidates.discard(p)
                        mark(connection, run_id, p, "duplicate_same_recording")
            else:
                group_id = f"ambiguous-{uuid.uuid4().hex[:16]}"
                connection.execute(
                    "INSERT OR REPLACE INTO groups(id,run_id,tool,similarity,winner_source_path,decision,paths_json) VALUES(?,?,?,?,?,?,?)",
                    (group_id, run_id, "hierarchical_blocking", 0.0, None, "keep_all_ambiguous", json.dumps([str(p) for p in sub_group], ensure_ascii=False))
                )

    connection.commit()
    connection.close()
    update_phase(run_id, "acoustic_dedupe", 1, 1, "跨格式同录音识别完成")
    return candidates


def ncm_decode(run_id: str, paths: list[Path], candidates: set[Path], run_root: Path, output: Path) -> dict[Path, Path]:
    active_ncms = [p for p in paths if p in candidates]
    if not active_ncms:
        return {}
    update_phase(run_id, "ncm_decode", 0, len(active_ncms), f"正在隔离解密 NCM 文件：0/{len(active_ncms)}")
    ncm_tmp = run_root / "ncm"
    ncm_tmp.mkdir(parents=True, exist_ok=True)

    def decode_single(src: Path):
        try:
            cached = find_cached_publish(src, output)
            if cached:
                return src, cached[0], 0, None, None
            stat = src.stat()
            cache_key = f"{stat.st_size}_{stat.st_mtime_ns}"
            for candidate in ncm_tmp.glob(f"{src.stem[:80]}__{cache_key}.*"):
                if candidate.is_file() and candidate.stat().st_size > 0:
                    try:
                        dur, _, _, _ = probe(candidate)
                        return src, candidate, dur, sha256(candidate), None
                    except Exception:
                        candidate.unlink(missing_ok=True)

            t = decode_to_directory(src, ncm_tmp)
            dest = ncm_tmp / f"{src.stem[:80]}__{cache_key}{t.suffix.lower()}"
            if t != dest and t.is_file():
                try:
                    os.replace(t, dest)
                    t = dest
                except OSError:
                    pass
            dur, _, _, _ = probe(t)
            return src, t, dur, sha256(t), None
        except Exception as exc:
            return src, None, 0, None, exc

    decoded_map: dict[Path, Path] = {}
    connection = db()
    done_decode = 0
    with ThreadPoolExecutor(max_workers=workers()) as pool:
        futures = {pool.submit(decode_single, p): p for p in active_ncms}
        for future in as_completed(futures):
            src, t, dur, digest, exc = future.result()
            done_decode += 1
            if exc:
                candidates.discard(src)
                mark(connection, run_id, src, "failed", repr(exc))
                event(connection, run_id, "ncm", "failed", src, repr(exc))
            elif t is not None:
                decoded_map[src] = t
                connection.execute("UPDATE items SET duration=?,audio_sha256=? WHERE run_id=? AND source_path=?", (dur, digest, run_id, str(src)))
            if done_decode == len(active_ncms) or done_decode % 25 == 0:
                connection.commit()
                update_phase(run_id, "ncm_decode", done_decode, len(active_ncms), f"正在隔离解密 NCM 文件：{done_decode}/{len(active_ncms)}")
    connection.commit()
    connection.close()
    return decoded_map


def beets_env(root: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["BEETSDIR"], env["HOME"], env["XDG_CACHE_HOME"] = str(root), str(root / "home"), str(root / "cache")
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy", "NO_PROXY", "no_proxy"):
        if key in os.environ:
            env[key] = os.environ[key]
    return env


def is_offline_mode() -> bool:
    if os.environ.get("MUSIC_OFFLINE_MODE") == "1":
        return True
    try:
        return bool(cfg().get("offline_mode"))
    except Exception:
        return False


def enrich(temporary: Path, run_root: Path, digest: str | None = None, source_stem: str | None = None) -> tuple[str, str, str]:
    if not digest:
        digest = sha256(temporary)
    before_tags, before_art = probe(temporary)[2:]
    before_lyrics = any((k.startswith("lyrics") or k in ("unsyncedlyrics", "uslt")) and bool(v.strip()) for k, v in before_tags.items())
    original_complete = bool(before_tags.get("title") and (before_tags.get("artist") or before_tags.get("album_artist")))
    clean_stem = normalize_track_stem(source_stem or temporary.stem)
    if original_complete and before_lyrics and before_art:
        with db() as conn:
            kb_put(conn, digest, before_tags.get("artist") or before_tags.get("album_artist", ""), before_tags.get("album", ""), before_tags.get("title", ""), before_tags.get("track", ""), None, before_tags.get("lyrics", ""), True, norm_stem=clean_stem)
        return "existing_tags", "lyrics_already_present", "cover_already_present"

    # Try local Knowledge Base (0ms network)
    with db() as conn:
        kb_entry = kb_get(conn, digest, before_tags.get("artist", ""), before_tags.get("title", ""), norm_stem=clean_stem)

    if kb_entry and kb_entry.get("lyrics") and mediafile is not None:
        try:
            mf = mediafile.MediaFile(str(temporary))
            if not mf.title and kb_entry.get("title"):
                mf.title = kb_entry["title"]
            if not mf.artist and kb_entry.get("artist"):
                mf.artist = kb_entry["artist"]
            if not mf.album and kb_entry.get("album"):
                mf.album = kb_entry["album"]
            if kb_entry.get("lyrics") and not mf.lyrics:
                mf.lyrics = kb_entry["lyrics"]
            if kb_entry.get("track_number") and not mf.track:
                try:
                    mf.track = int(kb_entry["track_number"])
                except (ValueError, TypeError):
                    pass
            mf.save()
            _, _, after_tags, after_art = probe(temporary)
            return "existing_tags", "lyrics_embedded", "cover_already_present" if after_art else "cover_not_found"
        except Exception as exc:
            log(f"知识库注入元数据异常，回退至刮削：{exc}")

    cur_dur, _, cur_tags, cur_art = probe(temporary)
    cur_lyrics = any((k.startswith("lyrics") or k in ("unsyncedlyrics", "uslt")) and bool(v.strip()) for k, v in cur_tags.items())
    cur_complete = bool(cur_tags.get("title") and (cur_tags.get("artist") or cur_tags.get("album_artist")))

    # Detect format capabilities for lyrics embedding
    ext = temporary.suffix.lower()
    can_embed_lyrics = ext not in NO_EMBED_LYRICS_FORMATS
    # Check for existing sidecar .lrc file
    sidecar_lrc = temporary.with_suffix(".lrc")
    has_sidecar_lrc = sidecar_lrc.is_file() and sidecar_lrc.stat().st_size > 0
    if has_sidecar_lrc:
        cur_lyrics = True

    # P4: Enhanced filename-to-tag extraction for songs with empty tags.
    # The sandbox name carries a random suffix, so the *original* file name is the
    # only trustworthy source of "歌手 - 歌名".
    stem_art, stem_tit = extract_artist_and_title(source_stem or temporary.stem)

    if is_offline_mode():
        metadata = "existing_tags" if cur_complete else "metadata_not_found"
        lyrics = "lyrics_already_present" if cur_lyrics else "lyrics_not_found"
        cover = "cover_already_present" if cur_art else "cover_not_found"
        return metadata, lyrics, cover

    # Fast Domestic Dual Engine FIRST (QQ Music + NetEase, ~200ms, skips slow overseas beets if complete)
    failures = set()
    dom_source = ""
    dom_lyrics = ""
    if domestic_provider is not None and (not cur_lyrics or not cur_art or not cur_complete):
        try:
            # P4: prefer tag values, but fall back to filename-extracted values
            art = cur_tags.get("artist") or cur_tags.get("album_artist") or before_tags.get("artist", "") or stem_art
            tit = cur_tags.get("title") or before_tags.get("title", "") or stem_tit or temporary.stem
            alb = cur_tags.get("album") or before_tags.get("album", "")
            dom_res = domestic_provider.enrich_domestic(
                temporary,
                current_artist=art,
                current_title=tit,
                current_album=alb,
                need_metadata=not cur_complete,
                need_lyrics=not cur_lyrics,
                need_cover=not cur_art,
                target_duration=cur_dur,
            )
            dom_source = dom_res.get("source", "")
            dom_lyrics = dom_res.get("lyrics", "")
            _, _, cur_tags, cur_art = probe(temporary)
            cur_lyrics = any((k.startswith("lyrics") or k in ("unsyncedlyrics", "uslt")) and bool(v.strip()) for k, v in cur_tags.items())
            # Re-check sidecar .lrc (domestic_provider may have written one for WAV)
            if not cur_lyrics and sidecar_lrc.is_file() and sidecar_lrc.stat().st_size > 0:
                cur_lyrics = True
                has_sidecar_lrc = True
            cur_complete = bool(cur_tags.get("title") and (cur_tags.get("artist") or cur_tags.get("album_artist")))
            if cur_complete and cur_lyrics and cur_art:
                # Fully enriched via fast domestic engines in ~200ms! Skip slow overseas Beets entirely!
                art_val = cur_tags.get("artist") or cur_tags.get("album_artist", "")
                alb_val = cur_tags.get("album", "")
                tit_val = cur_tags.get("title", "")
                lrc_val = cur_tags.get("lyrics") or cur_tags.get("unsyncedlyrics") or cur_tags.get("uslt") or dom_lyrics or ""
                trk_val = cur_tags.get("track") or cur_tags.get("tracknumber", "")
                with db() as conn:
                    kb_put(conn, digest, art_val, alb_val, tit_val, trk_val, None, lrc_val, True, norm_stem=clean_stem)
                meta_st = "existing_tags" if original_complete else "metadata_matched"
                lrc_st = "lyrics_already_present" if before_lyrics else "lyrics_embedded"
                cov_st = "cover_already_present" if before_art else "cover_embedded"
                return meta_st, lrc_st, cov_st
        except Exception as exc:
            log(f"[主要源 / Primary Engine] 检索异常: {exc}")

    # P4: If still no tags, try writing filename-derived metadata before Beets
    if not cur_complete and mediafile is not None and (stem_art or stem_tit):
        try:
            mf = mediafile.MediaFile(str(temporary))
            modified = False
            if not mf.title and stem_tit:
                mf.title = stem_tit
                modified = True
            if not mf.artist and stem_art:
                mf.artist = stem_art
                modified = True
            if modified:
                mf.save()
                _, _, cur_tags, _ = probe(temporary)
                cur_complete = bool(cur_tags.get("title") and (cur_tags.get("artist") or cur_tags.get("album_artist")))
                log(f"[元数据 / Metadata] {temporary.name}: 从文件名提取元数据 → artist={stem_art or '?'}, title={stem_tit or '?'}")
        except Exception as exc:
            log(f"[元数据 / Metadata] {temporary.name}: 写入文件名元数据失败: {exc}")

    # Global Catalog Fallback (MusicBrainz / LRCLIB, for tracks still missing metadata, lyrics, or cover)
    beets_needed = []
    if not cur_complete:
        beets_needed.append("元数据")
    if not cur_lyrics and can_embed_lyrics:
        beets_needed.append("歌词")
    if not cur_art:
        beets_needed.append("封面")
    if beets_needed:
        log(f"[全球源 / Global Fallback] {temporary.name}: 主要流媒体源({dom_source or '未命中'})未补齐，补充检索: {', '.join(beets_needed)}")

    has_clean_tags = bool(cur_tags.get("title") and (cur_tags.get("artist") or cur_tags.get("album_artist")) and cur_tags.get("album"))
    beets = run_root / "beets" / uuid.uuid4().hex
    beets.mkdir(parents=True)
    (beets / "home").mkdir()
    config = "\n".join([
        f"library: {beets / 'library.db'}",
        f"directory: {beets / 'files'}",
        "plugins: musicbrainz fromfilename fetchart embedart lyrics",
        "import:",
        "  copy: no",
        "  move: no",
        "  write: yes",
        "  quiet: yes",
        "  quiet_fallback: asis",
        "musicbrainz:",
        "  searchlimit: 2",
        "lyrics:",
        "  auto: yes",
        "  force: yes",
        "  sources: [lrclib, lrcmux]",
        "fetchart:",
        "  auto: yes",
        "  cautious: yes",
        "embedart:",
        "  auto: yes",
        "  maxwidth: 1200",
        "",
    ])
    (beets / "config.yaml").write_text(config, encoding="utf-8")
    env = beets_env(beets)

    if not cur_complete or not has_clean_tags:
        import_cmd = ["beet", "import", "-q", "-s", "-C", "-I"]
        if has_clean_tags:
            import_cmd.append("-A")
        import_cmd.append(str(temporary))
        try:
            command(import_cmd, env, timeout=5)
        except Exception as exc:
            failures.add("metadata")
            log(f"[全球源 / Global Catalog] {temporary.name}: 元数据匹配失败 (MusicBrainz 超时或无结果): {exc}")

    # P3: Only attempt Beets lyrics for formats that support embedding
    if not cur_lyrics and can_embed_lyrics:
        try:
            command(["beet", "lyrics", "--force"], env, timeout=5)
            command(["beet", "write"], env, timeout=5)
        except Exception as exc:
            failures.add("lyrics")
            log(f"[全球源 / Global Lyrics] {temporary.name}: 歌词获取失败 (LRCLIB/lrcmux): {exc}")
        _, _, cur_tags, _ = probe(temporary)
        cur_lyrics = any((k.startswith("lyrics") or k in ("unsyncedlyrics", "uslt")) and bool(v.strip()) for k, v in cur_tags.items())

    # Sidecar fallback: WAV/AIFF cannot embed lyrics, and tag writers on other
    # containers occasionally fail silently.  A same-name .lrc next to the audio
    # file keeps the lyrics usable in the fnOS player either way.
    if not cur_lyrics and not has_sidecar_lrc:
        if dom_lyrics:
            try:
                sidecar_lrc.write_text(dom_lyrics, encoding="utf-8")
                has_sidecar_lrc = True
                cur_lyrics = True
                reason = f"格式 {ext} 不支持内嵌歌词" if not can_embed_lyrics else "内嵌歌词未生效"
                log(f"[格式 / Format] {temporary.name}: {reason}，将随发布生成同名外挂 .lrc")
            except OSError as exc:
                failures.add("lyrics")
                log(f"[歌词 / Lyrics] {temporary.name}: 外挂 .lrc 写入失败: {exc}")
        elif not can_embed_lyrics:
            log(f"[歌词 / Lyrics] {temporary.name}: 格式 {ext} 不支持内嵌歌词，且主要流媒体源未收录该曲歌词")

    if not cur_art:
        try:
            command(["beet", "embedart", "-y"], env, timeout=5)
        except Exception as exc:
            failures.add("cover")
            log(f"[全球源 / Global Cover] {temporary.name}: 封面嵌入失败: {exc}")

    _, _, after_tags, after_art = probe(temporary)
    shutil.rmtree(beets, ignore_errors=True)

    # Save newly scraped metadata to knowledge base
    try:
        art_val = after_tags.get("artist") or after_tags.get("album_artist") or before_tags.get("artist", "")
        alb_val = after_tags.get("album") or before_tags.get("album", "")
        tit_val = after_tags.get("title") or before_tags.get("title", "")
        lrc_val = after_tags.get("lyrics") or after_tags.get("unsyncedlyrics") or after_tags.get("uslt") or dom_lyrics or ""
        trk_val = after_tags.get("track") or after_tags.get("tracknumber", "")
        if tit_val:
            with db() as conn:
                kb_put(conn, digest, art_val, alb_val, tit_val, trk_val, None, lrc_val, bool(after_art or before_art), norm_stem=clean_stem)
    except Exception:
        pass

    complete = bool(after_tags.get("title") and (after_tags.get("artist") or after_tags.get("album_artist")))
    metadata = "existing_tags" if original_complete and "metadata" not in failures else "existing_tags_tool_error" if original_complete else "metadata_matched" if complete and after_tags != before_tags else "metadata_from_filename" if complete else "metadata_network_error" if "metadata" in failures else "metadata_not_found"
    after_has_lyrics = any((k.startswith("lyrics") or k in ("unsyncedlyrics", "uslt")) and bool(v.strip()) for k, v in after_tags.items()) or has_sidecar_lrc
    lyrics = "lyrics_already_present" if before_lyrics else "lyrics_embedded" if after_has_lyrics else "lyrics_tool_error" if "lyrics" in failures else "lyrics_not_found"
    cover = "cover_already_present" if before_art else "cover_embedded" if after_art else "cover_tool_error" if "cover" in failures else "cover_not_found"
    if os.environ.get("MUSIC_DEBUG") == "1" or "error" in metadata or "error" in lyrics or "error" in cover:
        log(f"[流水线 / Pipeline] {temporary.name}: 主要源={dom_source or '未命中'} → 元数据={metadata} / 歌词={lyrics}{'(外挂 .lrc)' if has_sidecar_lrc else ''} / 封面={cover}")
    return metadata, lyrics, cover


def clean_track_number(value: str) -> str:
    if not value:
        return ""
    part = value.split("/")[0].split("-")[0].strip()
    return clean(part, "")


WINDOWS_RESERVED = {
    "con", "prn", "aux", "nul",
    "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9",
    "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
}


def truncate_utf8_bytes(s: str, max_bytes: int) -> str:
    """Safely truncate string to at most max_bytes without splitting multi-byte UTF-8 characters."""
    encoded = s.encode("utf-8")
    if len(encoded) <= max_bytes:
        return s
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def clean(value: str, fallback: str, max_bytes: int = 180) -> str:
    value = "".join(char for char in value.replace("/", "-").replace("\\", "-").strip() if char not in '<>:\\|?*\0')
    value = value.strip().rstrip(". ").lstrip(". ")
    value = truncate_utf8_bytes(value, max_bytes)
    res = value or fallback
    parts = res.split(".")
    if parts[0].casefold() in WINDOWS_RESERVED:
        res = f"{parts[0]}_{('.' + '.'.join(parts[1:])) if len(parts) > 1 else ''}"
    return res


def untagged_output(path: Path) -> bool:
    """True when a published path shows the track still has no usable tags (missing artist)."""
    return UNKNOWN_ARTIST in path.parts


def destination(root: Path, source: Path, digest: str, fallback_stem: str | None = None) -> Path:
    _, _, tags, _ = probe(source)
    artist = clean(tags.get("artist") or tags.get("album_artist", ""), UNKNOWN_ARTIST, max_bytes=100)
    album = clean(tags.get("album", ""), UNKNOWN_ALBUM, max_bytes=120)
    title = clean(tags.get("title", ""), fallback_stem or source.stem, max_bytes=160)
    number = clean_track_number(tags.get("track") or tags.get("tracknumber", ""))

    disc_raw = str(tags.get("disc") or tags.get("discnumber") or "").split("/")[0].strip()
    disc = int(disc_raw) if disc_raw.isdigit() else 0
    disctotal_raw = str(tags.get("disctotal") or tags.get("totaldiscs") or "").split("/")[0].strip()
    disctotal = int(disctotal_raw) if disctotal_raw.isdigit() else 0
    if disc > 0 and (disc > 1 or disctotal > 1):
        folder = root / artist / album / f"CD{disc}"
    else:
        folder = root / artist / album

    prefix = f"{number} - " if number else ""
    ext = source.suffix.lower()
    max_title_bytes = max(20, 240 - len(prefix.encode("utf-8")) - len(ext.encode("utf-8")))
    safe_title = truncate_utf8_bytes(title, max_title_bytes)
    return folder / f"{prefix}{safe_title}{ext}"


def find_cached_publish(source: Path, output: Path) -> tuple[Path, tuple[str, str, str], str] | None:
    try:
        stat = source.stat()
    except OSError:
        return None
    conn = db()
    try:
        row = conn.execute(
            "SELECT output_path, sha256, metadata_state, lyrics_state, cover_state, rules_version FROM source_inventory WHERE source_path=? AND size_bytes=? AND mtime_ns=? AND disposition='published'",
            (str(source), stat.st_size, stat.st_mtime_ns),
        ).fetchone()
        if not row:
            row = conn.execute(
                "SELECT output_path, audio_sha256 AS sha256, metadata_state, lyrics_state, cover_state, 0 AS rules_version FROM items WHERE source_path=? AND source_size=? AND source_mtime_ns=? AND disposition='published' AND output_path IS NOT NULL ORDER BY id DESC LIMIT 1",
                (str(source), stat.st_size, stat.st_mtime_ns),
            ).fetchone()
        if row and row["output_path"]:
            # A result produced by older rules is not a valid cache entry: the whole
            # point of re-running is to re-decide it.
            if int(row["rules_version"] or 0) < RULES_VERSION:
                return None
            candidate = Path(row["output_path"])
            try:
                candidate.resolve().relative_to(output.resolve())
            except ValueError:
                return None
            if candidate.is_file() and candidate.stat().st_size > 0:
                meta = row["metadata_state"] or "existing_tags"
                lrc = row["lyrics_state"] or "lyrics_already_present"
                cov = row["cover_state"] or "cover_already_present"
                if "error" in meta or "error" in lrc:
                    return None
                # A track that landed under Unknown Artist/Unknown Album was never
                # properly tagged — rebuild it now that the fallbacks are in place.
                if untagged_output(candidate):
                    return None
                return candidate, (meta, lrc, cov), str(row["sha256"] or "")
    finally:
        conn.close()
    return None


def recorded_output(source: Path) -> Path | None:
    """Where this source was last published (whatever the disposition)."""
    try:
        with db() as connection:
            row = connection.execute(
                "SELECT output_path FROM source_inventory WHERE source_path=? AND output_path IS NOT NULL",
                (str(source),),
            ).fetchone()
    except sqlite3.Error:
        return None
    return Path(row["output_path"]) if row else None


def recorded_source_for(output_path: Path) -> Path | None:
    """Reverse lookup: which source file owns a given library path."""
    try:
        with db() as connection:
            row = connection.execute(
                "SELECT source_path FROM source_inventory WHERE output_path=?",
                (str(output_path),),
            ).fetchone()
    except sqlite3.Error:
        return None
    return Path(row["source_path"]) if row else None


def archive_previous_output(source: Path, output: Path, target: Path) -> None:
    """Retire the copy left behind when a track's destination path changes.

    Re-enriching a track that used to sit under Unknown Artist publishes it into a
    real artist/album folder; without this the old path would linger and the library
    would carry two copies of the same song.
    """
    stale = recorded_output(source)
    if stale is None or stale == target or not stale.is_file() or hidden_under(stale, output):
        return
    if archive_superseded(stale, output):
        log(f"[发布 / Publish] {source.name}: 位置更新 {stale.parent.name}/ → {target.parent.name}/，旧副本已移入 {ARCHIVE_DIRNAME}")


def clean_lrc(payload: str) -> str:
    """Strip noisy metadata tags while preserving genuine lyric lines."""
    if not payload:
        return ""
    lines: list[str] = []
    for line in payload.splitlines():
        trimmed = line.strip()
        if not trimmed:
            continue
        # Drop redundant metadata headers that confuse minimal players
        if re.match(r"^\[(?:by|re|ve|length|encoding|tool):.*\]", trimmed, flags=re.IGNORECASE) or re.match(r"^\[offset:\s*0\s*\]", trimmed, flags=re.IGNORECASE):
            continue
        lines.append(trimmed)
    return "\n".join(lines)


def find_sibling_cue(source: Path) -> Path | None:
    """Find sibling .cue sheet next to source audio."""
    cand1 = source.with_suffix(".cue")
    if cand1.is_file() and cand1.stat().st_size > 0:
        return cand1
    cand2 = source.with_name(source.name + ".cue")
    if cand2.is_file() and cand2.stat().st_size > 0:
        return cand2
    return None


def publish_cue(target: Path, source: Path) -> None:
    """Copy sibling .cue index sheet to target directory alongside published audio."""
    cue = find_sibling_cue(source)
    if cue is None:
        return
    target_cue = target.with_suffix(".cue")
    try:
        if not target_cue.exists() or target_cue.stat().st_size == 0:
            shutil.copy2(str(cue), str(target_cue))
            log(f"[CUE索引 / CUE] {target.name}: 已同步伴随分轨文件 {target_cue.name}")
    except Exception as exc:
        log(f"[CUE索引 / CUE] {target.name}: 伴随分轨文件复制失败: {exc}")


def publish_sidecar(target: Path, source: Path | None) -> None:
    """Place a generated .lrc next to its published audio file.

    WAV/AIFF cannot embed lyrics, and tag writers occasionally fail on other
    containers; a same-name sidecar is what the fnOS player and most desktop
    players load automatically, so the lyrics are never lost with the sandbox.
    """
    if source is None or not source.is_file():
        return
    try:
        payload = source.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return
    payload = clean_lrc(payload)
    if not payload.strip():
        return
    sidecar = target.with_suffix(".lrc")
    if sidecar.is_file():
        try:
            if sidecar.read_text(encoding="utf-8") == payload:
                return
        except (OSError, UnicodeDecodeError):
            pass
    try:
        sidecar.write_text(payload, encoding="utf-8")
        log(f"[歌词 / Lyrics] {target.name}: 已随发布写入外挂 {sidecar.name}")
    except OSError as exc:
        log(f"[歌词 / Lyrics] {target.name}: 外挂 {sidecar.name} 写入失败: {exc}")


def restore_sidecar(target: Path, digest: str) -> bool:
    """Re-create a missing .lrc from the knowledge base (cache-hit path)."""
    if target.suffix.lower() not in NO_EMBED_LYRICS_FORMATS or not digest:
        return False
    sidecar = target.with_suffix(".lrc")
    if sidecar.is_file() and sidecar.stat().st_size > 0:
        return False
    try:
        with db() as connection:
            row = connection.execute("SELECT lyrics FROM knowledge_base WHERE audio_sha256=?", (digest,)).fetchone()
    except sqlite3.Error:
        return False
    if not row or not row["lyrics"]:
        return False
    try:
        sidecar.write_text(clean_lrc(row["lyrics"]), encoding="utf-8")
    except OSError:
        return False
    log(f"[歌词 / Lyrics] {target.name}: 复用缓存，已从知识库恢复外挂 {sidecar.name}")
    return True


def publish_one(source: Path, run_root: Path, output: Path, real: bool, decoded_source: Path | None = None, kept: set[Path] | None = None) -> tuple[Path | None, tuple[str, str, str]]:
    cached = find_cached_publish(source, output)
    if cached:
        target, states, digest = cached
        restore_sidecar(target, digest)
        return (target if real else None), states

    inflight = run_root / "inflight"
    inflight.mkdir(parents=True, exist_ok=True)
    song_sandbox = inflight / f"song-{uuid.uuid4().hex[:12]}"
    song_sandbox.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        if source.suffix.lower() == ".ncm":
            if decoded_source and decoded_source.is_file():
                temporary = song_sandbox / f"{decoded_source.stem[:80]}-{uuid.uuid4().hex[:8]}{decoded_source.suffix.lower()}"
                copy_checked(decoded_source, temporary)
            else:
                temporary = decode_to_directory(source, song_sandbox)
        else:
            temporary = song_sandbox / f"{source.stem[:80]}-{uuid.uuid4().hex[:8]}{source.suffix.lower()}"
            copy_checked(source, temporary)
        repair_shadowed_wav_tags(temporary)
        duration, size, _, _ = probe(temporary)
        digest = sha256(temporary)
        states = enrich(temporary, run_root, digest, source.stem)
        new_duration, new_size, _, _ = probe(temporary)
        if abs(duration - new_duration) > .05 or new_size <= 0:
            raise RuntimeError("metadata output validation failed")
        sidecar_source = temporary.with_suffix(".lrc")
        if not sidecar_source.is_file() or sidecar_source.stat().st_size == 0:
            sidecar_source = None
        if not real:
            return None, states

        target = destination(output, temporary, digest, source.stem)
        final_digest = sha256(temporary)
        temp_size = temporary.stat().st_size

        with DESTINATION_LOCK:
            target.parent.mkdir(parents=True, exist_ok=True)
            owner = recorded_output(source)
            occupant = recorded_source_for(target) if target.exists() else None
            # Overwrite in place (never fork a "(2)" copy) when the path is either this
            # source's own previous output, or the output of a source that this run has
            # already decided against — a dedupe loser must not keep a library copy.
            losing_occupant = occupant is not None and occupant != source and kept is not None and occupant not in kept
            
            target_matches_digest = False
            if target.exists() and target.stat().st_size == temp_size:
                target_h = sha256(target)
                target_matches_digest = (target_h == final_digest or target_h == digest)

            if target.exists() and target_matches_digest:
                temporary.unlink(missing_ok=True)
                temporary = None
            elif target.exists() and (owner == target or losing_occupant):
                if losing_occupant:
                    archive_superseded(target, output)
                    log(f"[发布 / Publish] {source.name}: 取代同曲旧件 {target.parent.name}/{target.name}（旧件移入 {ARCHIVE_DIRNAME}）")
                else:
                    log(f"[发布 / Publish] {source.name}: 就地更新 {target.parent.name}/{target.name}（不再产生 (2) 副本）")
                shutil.move(str(temporary), str(target))
                temporary = None
            elif target.exists():
                is_same_song = False
                try:
                    target_dur, _, _, _ = probe(target)
                    temp_dur, _, _, _ = probe(temporary)
                    if target_dur > 0 and temp_dur > 0 and abs(target_dur - temp_dur) <= 1.0:
                        is_same_song = True
                except Exception:
                    pass
                if not is_same_song and target.stat().st_size == temp_size:
                    is_same_song = True

                if is_same_song:
                    score_new = audio_quality_score(temporary)
                    score_old = audio_quality_score(target)
                    if score_new > score_old:
                        archive_superseded(target, output)
                        log(f"[发布 / Publish] {source.name}: 品质更高 ({score_new} > {score_old})，取代现有曲目 {target.parent.name}/{target.name}（旧件移入 {ARCHIVE_DIRNAME}）")
                        shutil.move(str(temporary), str(target))
                    else:
                        log(f"[发布 / Publish] {source.name}: 目的库已存在同名且品质相当/更优曲目 ({target.parent.name}/{target.name})，复用现有曲目")
                        temporary.unlink(missing_ok=True)
                    temporary = None
                else:
                    base_stem = target.stem
                    counter = 2
                    while target.exists():
                        if target.stat().st_size == temp_size and (sha256(target) in (final_digest, digest)):
                            break
                        target = target.with_name(f"{base_stem} ({counter}){target.suffix}")
                        counter += 1
                    if target.exists() and target.stat().st_size == temp_size and (sha256(target) in (final_digest, digest)):
                        temporary.unlink(missing_ok=True)
                    else:
                        shutil.move(str(temporary), str(target))
                    temporary = None
            else:
                shutil.move(str(temporary), str(target))
                temporary = None
        publish_sidecar(target, sidecar_source)
        publish_cue(target, source)
        archive_previous_output(source, output, target)
        try:
            stat = source.stat()
            with DB_WRITE_LOCK:
                conn = db()
                try:
                    art = target.parent.parent.name if target.parent.parent != output else ""
                    tit = target.stem
                    is_meta_error = "error" in str(states[0]) if states else False
                    if is_meta_error:
                        conn.execute(
                            "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,metadata_state,lyrics_state,cover_state,duration,artist,title,retry_count,unresolvable,rules_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1,0,?) ON CONFLICT(source_path) DO UPDATE SET size_bytes=excluded.size_bytes,mtime_ns=excluded.mtime_ns,sha256=excluded.sha256,disposition=excluded.disposition,output_path=excluded.output_path,metadata_state=excluded.metadata_state,lyrics_state=excluded.lyrics_state,cover_state=excluded.cover_state,duration=excluded.duration,artist=excluded.artist,title=excluded.title,retry_count=source_inventory.retry_count+1,unresolvable=CASE WHEN source_inventory.retry_count>=2 THEN 1 ELSE 0 END,rules_version=excluded.rules_version,updated_at=CURRENT_TIMESTAMP",
                            (str(source), stat.st_size, stat.st_mtime_ns, digest, "published", str(target), *states, duration, art, tit, RULES_VERSION),
                        )
                    else:
                        conn.execute(
                            "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,metadata_state,lyrics_state,cover_state,duration,artist,title,retry_count,unresolvable,rules_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0,0,?) ON CONFLICT(source_path) DO UPDATE SET size_bytes=excluded.size_bytes,mtime_ns=excluded.mtime_ns,sha256=excluded.sha256,disposition=excluded.disposition,output_path=excluded.output_path,metadata_state=excluded.metadata_state,lyrics_state=excluded.lyrics_state,cover_state=excluded.cover_state,duration=excluded.duration,artist=excluded.artist,title=excluded.title,retry_count=0,unresolvable=0,rules_version=excluded.rules_version,updated_at=CURRENT_TIMESTAMP",
                            (str(source), stat.st_size, stat.st_mtime_ns, digest, "published", str(target), *states, duration, art, tit, RULES_VERSION),
                        )
                    conn.commit()
                finally:
                    conn.close()
        except Exception:
            pass
        return target, states
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)
        shutil.rmtree(song_sandbox, ignore_errors=True)


def fallback_publish_one(source: Path, output: Path) -> Path | None:
    """Safely publish an unresolvable track to a designated fallback location in output directory."""
    if not source.is_file():
        return None
    dest_dir = output / "未知艺术家" / "未分类专辑"
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / source.name
    if target.exists() and sha256(target) == sha256(source):
        pass
    elif target.exists():
        counter = 2
        base_stem = source.stem
        while target.exists() and sha256(target) != sha256(source):
            target = dest_dir / f"{base_stem} ({counter}){source.suffix}"
            counter += 1
        if not target.exists():
            copy_checked(source, target)
    else:
        copy_checked(source, target)

    stat = source.stat()
    dur = 0.0
    try:
        dur, _, _, _ = probe(source)
    except Exception:
        pass
    with db() as conn:
        conn.execute(
            """INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,metadata_state,lyrics_state,cover_state,duration,artist,title,retry_count,unresolvable,rules_version)
               VALUES(?,?,?,?,?,?,'fallback_tags','none','none',?,'未知艺术家',?,0,0,?)
               ON CONFLICT(source_path) DO UPDATE SET
                 disposition=excluded.disposition,output_path=excluded.output_path,
                 metadata_state=excluded.metadata_state,retry_count=0,unresolvable=0,updated_at=CURRENT_TIMESTAMP""",
            (str(source), stat.st_size, stat.st_mtime_ns, sha256(source), "published", str(target), dur, source.stem, RULES_VERSION)
        )
        conn.commit()
    return target


def get_run_root(run_id: str, output: Path | None = None) -> Path:
    if output and output.is_dir():
        root = output / f"{RUN_PREFIX}{run_id}"
    else:
        root = Path("/tmp") / f"{RUN_PREFIX}{run_id}"
    root.mkdir(parents=True, exist_ok=True)
    return root


def cleanup_run_root(root: Path) -> None:
    shutil.rmtree(root, ignore_errors=True)
    if root.exists():
        raise RuntimeError(f"无法清理临时目录：{root}")


def cleanup_stale_runs(output: Path) -> None:
    try:
        for path in output.iterdir():
            if path.is_dir() and path.name.startswith(RUN_PREFIX):
                cleanup_run_root(path)
    except Exception:
        pass
    tmp_dir = Path("/tmp")
    if tmp_dir.is_dir():
        try:
            for path in tmp_dir.iterdir():
                if path.is_dir() and path.name.startswith(RUN_PREFIX):
                    cleanup_run_root(path)
        except Exception:
            pass
    runs_dir = STATE / "runs"
    if runs_dir.is_dir():
        try:
            shutil.rmtree(runs_dir, ignore_errors=True)
        except Exception:
            pass


def output_is_clean(output: Path) -> bool:
    return not any(not path.name.startswith(".") for path in output.iterdir())


def persist_inventory(run_id: str) -> None:
    with db() as connection:
        rows = connection.execute("SELECT source_path,source_size,source_mtime_ns,source_sha256,duration,fingerprint,disposition,output_path FROM items WHERE run_id=? AND source_sha256 IS NOT NULL", (run_id,)).fetchall()
        for row in rows:
            # If an existing source is already marked 'published' with an existing file on disk,
            # do not overwrite its disposition to 'duplicate_*' during incremental retries.
            prev = connection.execute("SELECT disposition, output_path FROM source_inventory WHERE source_path=?", (row["source_path"],)).fetchone()
            if prev and prev["disposition"] == "published" and str(row["disposition"]).startswith("duplicate_"):
                out_p = Path(prev["output_path"]) if prev["output_path"] else None
                if out_p and out_p.is_file():
                    continue
            out_str = row["output_path"]
            art, tit = "", ""
            title_key = ""
            if out_str:
                out_p = Path(out_str)
                art = out_p.parent.parent.name if out_p.parent.parent.name else ""
                tit = out_p.stem
                title_key = re.sub(r"[^\w\u4e00-\u9fff]", "", simplify_chinese(tit).casefold()) or tit.casefold()
            connection.execute(
                "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,duration,artist,title,fingerprint,title_key,rules_version) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_path) DO UPDATE SET "
                "size_bytes=excluded.size_bytes,mtime_ns=excluded.mtime_ns,sha256=excluded.sha256,disposition=excluded.disposition,"
                "output_path=excluded.output_path,duration=COALESCE(excluded.duration, source_inventory.duration),"
                "artist=COALESCE(excluded.artist, source_inventory.artist),title=COALESCE(excluded.title, source_inventory.title),"
                "fingerprint=COALESCE(excluded.fingerprint, source_inventory.fingerprint),title_key=COALESCE(excluded.title_key, source_inventory.title_key),"
                "rules_version=excluded.rules_version,updated_at=CURRENT_TIMESTAMP",
                (row["source_path"], row["source_size"], row["source_mtime_ns"], row["source_sha256"], row["disposition"], row["output_path"],
                 row["duration"], art, tit, row["fingerprint"], title_key, RULES_VERSION)
            )
    assert_state_budget()


CONFLICT_COPY_RE = re.compile(r" \(\d+\)$")


def sweep_orphan_conflict_copies(output: Path, connection: sqlite3.Connection, target_dirs: set[Path] | None = None) -> int:
    """归档历史上分叉出来的「名 (2).ext」孤儿副本。

    旧发布逻辑在"同一逻辑曲目重发但字节不同"时会新开 (2) 而不是替换，账本里又没有任何
    一行指向这些副本 —— 它们既不参与去重也不会被更新，是曲库里对不上数的净增量。
    只认「本名文件在账本里有主、而 (N) 副本无人认领」这一种组合，其它一律不碰。
    """
    archived = 0
    candidate_files: list[Path] = []
    if target_dirs is not None:
        for d in target_dirs:
            if d.is_dir():
                try:
                    for p in d.iterdir():
                        if p.is_file() and media(p) and not p.name.startswith("."):
                            candidate_files.append(p)
                except OSError:
                    pass
    else:
        try:
            candidate_files = [p for p in output.rglob("*") if p.is_file() and media(p) and not p.name.startswith(".") and not hidden_under(p, output)]
        except OSError:
            pass

    for path in candidate_files:
        if not CONFLICT_COPY_RE.search(path.stem):
            continue
        base = path.with_name(f"{CONFLICT_COPY_RE.sub('', path.stem)}{path.suffix}")
        if not base.is_file():
            continue
        owned = connection.execute("SELECT 1 FROM source_inventory WHERE output_path=? LIMIT 1", (str(base),)).fetchone()
        orphan = connection.execute("SELECT 1 FROM source_inventory WHERE output_path=? LIMIT 1", (str(path),)).fetchone()
        if (owned and not orphan) or (orphan and not owned):
            # Verify that the conflict file actually has identical or nearly identical duration to base,
            # ensuring we never delete a genuinely different user track that happened to be named 'Song (2)'.
            is_dup = False
            try:
                base_dur, _, _, _ = probe(base)
                orphan_dur, _, _, _ = probe(path)
                if base_dur > 0 and orphan_dur > 0 and abs(base_dur - orphan_dur) <= 1.0:
                    is_dup = True
                elif base_dur > 0 and orphan_dur > 0 and abs(base_dur - orphan_dur) > 1.0:
                    log(f"[清理 / Cleanup] {path.parent.name}/{path.name}: 与原件时长差异较大({abs(base_dur - orphan_dur):.1f}s)，判定为不同曲目，跳过自动清理")
                    continue
            except Exception:
                pass

            if not is_dup and base.stat().st_size == path.stat().st_size:
                is_dup = True

            if not is_dup and owned and not orphan:
                is_dup = True

            if not is_dup:
                continue

            if orphan and not owned:
                connection.execute("UPDATE source_inventory SET output_path=? WHERE output_path=?", (str(base), str(path)))
                connection.execute("UPDATE items SET output_path=? WHERE output_path=?", (str(base), str(path)))

            if archive_superseded(path, output, connection):
                log(f"[清理 / Cleanup] {path.parent.name}/{path.name}: 验证为同曲历史冲突副本，已移入 {ARCHIVE_DIRNAME}")
                archived += 1
    return archived


def report(run_id: str) -> dict:
    connection = db()
    try:
        run = connection.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        if run is None:
            return {}
        def counts(column: str) -> dict[str, int]:
            return {str(row["key"]): int(row["value"]) for row in connection.execute(f"SELECT {column} AS key,count(*) AS value FROM items WHERE run_id=? GROUP BY {column}", (run_id,))}
        dispositions, metadata, lyrics, covers = counts("disposition"), counts("metadata_state"), counts("lyrics_state"), counts("cover_state")
        groups = {str(row["key"]): int(row["value"]) for row in connection.execute("SELECT decision AS key,count(*) AS value FROM groups WHERE run_id=? GROUP BY decision", (run_id,))}
        fb_row = connection.execute(
            f"SELECT count(*) FROM items WHERE run_id=? AND disposition IN ('published', 'sample_validated') AND {FALLBACK_OUTPUT_COND}",
            (run_id,)
        ).fetchone()
        fallback_published = fb_row[0] if fb_row else 0
    finally:
        connection.close()
    metrics = {
        "scanned_total": sum(dispositions.values()),
        "published": dispositions.get("published", 0),
        "exact_duplicate": dispositions.get("duplicate_exact", 0),
        "same_recording_duplicate": dispositions.get("duplicate_same_recording", 0),
        "possible_version_groups": groups.get("keep_all_ambiguous", 0),
        "quality_upgraded": groups.get("quality_upgrade", 0),
        "failed": dispositions.get("failed", 0),
        "fallback_published": fallback_published,
        "metadata_matched": metadata.get("metadata_matched", 0),
        "lyrics_embedded": lyrics.get("lyrics_embedded", 0),
        "lyrics_already_present": lyrics.get("lyrics_already_present", 0),
        "lyrics_total_with": lyrics.get("lyrics_already_present", 0) + lyrics.get("lyrics_embedded", 0),
        "lyrics_not_found": lyrics.get("lyrics_not_found", 0),
        "cover_embedded": covers.get("cover_embedded", 0),
        "cover_already_present": covers.get("cover_already_present", 0),
        "cover_total_with": covers.get("cover_already_present", 0) + covers.get("cover_embedded", 0),
        "cover_not_found": covers.get("cover_not_found", 0),
        "new_files": 0,
        "modified_files": 0,
        "stale_decisions": 0,
        "retry_files": 0,
        "orphans_archived": 0,
        "state_bytes": directory_size(STATE),
        "temporary_audio_bytes": 0,
        "acoustic_duplicates": dispositions.get("duplicate_same_recording", 0),
        "exact_duplicates": dispositions.get("duplicate_exact", 0),
        "scanned": sum(dispositions.values()),
        "version_groups": groups.get("keep_all_ambiguous", 0),
        "upgraded": groups.get("quality_upgrade", 0),
        "cover_art_embedded": covers.get("cover_embedded", 0),
        "cover_art_already_present": covers.get("cover_already_present", 0),
    }
    return {"run_id": run_id, "status": run["status"], "phase": {"name": run["phase"], "done": run["phase_done"], "total": run["phase_total"]}, "started_at": run["started_at"], "finished_at": run["finished_at"], "dispositions": dispositions, "metadata": metadata, "lyrics": lyrics, "covers": covers, "groups": groups, "metrics": metrics}


def run_batch(mode: str, source: Path, output: Path, paths: list[Path], real: bool, compare_library: bool, classification: dict[str, int] | None = None) -> dict:
    run_id = f"{mode}-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    run_root = get_run_root(run_id, output)
    prune_dupsonic_ghosts()
    with db() as connection:
        connection.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES(?,?,?,?,?)", (run_id, mode, str(source), str(output), "running"))
    try:
        analysed = analyse_regular(run_id, paths)
        hash_cache: dict[Path, str] = {}
        for p_item, info in analysed.items():
            if isinstance(info, dict) and info.get("digest"):
                hash_cache[p_item] = str(info["digest"])

        candidates = {path for path, item in analysed.items() if "error" not in item}
        ncm_paths = [path for path in paths if path.suffix.lower() == ".ncm"]
        with db() as connection:
            for path in ncm_paths:
                size, mtime, digest = snapshot(path, connection)
                connection.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,source_sha256,source_kind,disposition) VALUES(?,?,?,?,?,?,?)", (run_id, str(path), size, mtime, digest, "ncm", "candidate"))
                candidates.add(path)
                if digest:
                    hash_cache[path] = str(digest)
        decoded_map = ncm_decode(run_id, ncm_paths, candidates, run_root, output)
        candidates = exact_dedupe(run_id, source, candidates, output if compare_library else None, decoded_map=decoded_map, run_root=run_root, hash_cache=hash_cache)
        candidates = acoustic_dedupe(run_id, source, candidates, output if compare_library else None, decoded_map=decoded_map, run_root=run_root)
        ordered = sorted(candidates, key=lambda value: str(value).casefold())
        kept_candidates = set(ordered)
        orphans_archived = 0
        published_dirs: set[Path] = set()
        pool_workers = metadata_workers()
        cores = workers()
        update_phase(run_id, "publish", 0, len(ordered), f"正在并发处理并发布 0/{len(ordered)} 个最终文件 (自适应线程: {pool_workers}，基于系统 {cores} 核)")
        def do_publish(item_path: Path):
            try:
                target, states = publish_one(item_path, run_root, output, real, decoded_source=decoded_map.get(item_path), kept=kept_candidates)
                return item_path, target, states, None
            except Exception as exc:
                return item_path, None, None, exc
        connection = db()
        done_count = 0
        with ThreadPoolExecutor(max_workers=pool_workers) as pool:
            futures = {pool.submit(do_publish, path): path for path in ordered}
            for future in as_completed(futures):
                path, target, states, exc = future.result()
                done_count += 1
                if exc:
                    connection.execute("UPDATE items SET disposition='failed',error=? WHERE run_id=? AND source_path=?", (repr(exc), run_id, str(path)))
                    event(connection, run_id, "publish", "failed", path, repr(exc))
                    try:
                        st = path.stat()
                        connection.execute(
                            "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,retry_count,unresolvable,retry_reason,rules_version) "
                            "VALUES(?,?,?,?,'failed',1,0,?,?) ON CONFLICT(source_path) DO UPDATE SET "
                            "size_bytes=excluded.size_bytes,mtime_ns=excluded.mtime_ns,disposition='failed',retry_count=source_inventory.retry_count+1,"
                            "unresolvable=CASE WHEN source_inventory.retry_count>=1 THEN 1 ELSE 0 END,retry_reason=excluded.retry_reason,rules_version=excluded.rules_version,updated_at=CURRENT_TIMESTAMP",
                            (str(path), st.st_size, st.st_mtime_ns, "", repr(exc)[:200], RULES_VERSION)
                        )
                    except Exception:
                        pass
                    try:
                        upg_rows = connection.execute("SELECT paths_json FROM groups WHERE run_id=? AND decision='quality_upgrade' AND winner_source_path=?", (run_id, str(path))).fetchall()
                        for u_row in upg_rows:
                            detail_obj = json.loads(u_row["paths_json"])
                            for superseded_path_str in detail_obj.get("superseded", []):
                                orig_p = Path(superseded_path_str)
                                arch_rel = orig_p.relative_to(output) if orig_p.is_relative_to(output) else orig_p.name
                                arch_p = output / ARCHIVE_DIRNAME / arch_rel
                                if arch_p.is_file() and not orig_p.is_file():
                                    orig_p.parent.mkdir(parents=True, exist_ok=True)
                                    os.replace(arch_p, orig_p)
                                    connection.execute("UPDATE source_inventory SET disposition='published', output_path=? WHERE output_path=?", (str(orig_p), str(arch_p)))
                                    log(f"[品质升级 / Upgrade] {path.name} 发布失败，已将归档旧版本 {orig_p.name} 恢复至原位")
                    except Exception:
                        pass
                else:
                    disposition = "published" if real else "sample_validated"
                    connection.execute("UPDATE items SET disposition=?,output_path=?,metadata_state=?,lyrics_state=?,cover_state=? WHERE run_id=? AND source_path=?", (disposition, str(target) if target else None, *states, run_id, str(path)))
                    if real and disposition == "published" and target:
                        published_dirs.add(target.parent)

                        try:
                            st = path.stat()
                            art = target.parent.parent.name if target.parent.parent != output else ""
                            tit = target.stem
                            title_key = re.sub(r"[^\w\u4e00-\u9fff]", "", simplify_chinese(tit).casefold()) or tit.casefold()
                            item_row = connection.execute("SELECT duration, fingerprint FROM items WHERE run_id=? AND source_path=?", (run_id, str(path))).fetchone()
                            dur = float(item_row["duration"] or 0) if item_row else 0.0
                            fp = item_row["fingerprint"] if item_row else ""
                            connection.execute(
                                "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,metadata_state,lyrics_state,cover_state,duration,artist,title,fingerprint,title_key,retry_count,unresolvable,rules_version) "
                                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,?) "
                                "ON CONFLICT(source_path) DO UPDATE SET "
                                "size_bytes=excluded.size_bytes,mtime_ns=excluded.mtime_ns,sha256=excluded.sha256,disposition=excluded.disposition,"
                                "output_path=excluded.output_path,metadata_state=excluded.metadata_state,lyrics_state=excluded.lyrics_state,cover_state=excluded.cover_state,"
                                "duration=excluded.duration,artist=excluded.artist,title=excluded.title,fingerprint=excluded.fingerprint,title_key=excluded.title_key,"
                                "retry_count=0,unresolvable=0,rules_version=excluded.rules_version,updated_at=CURRENT_TIMESTAMP",
                                (str(path), st.st_size, st.st_mtime_ns, hash_cache.get(path) or sha256(path), disposition, str(target), states[0], states[1], states[2], dur, art, tit, fp, title_key, RULES_VERSION)
                            )
                        except OSError:
                            pass
                connection.commit()
                if done_count == len(ordered) or done_count % 10 == 0:
                    update_phase(run_id, "publish", done_count, len(ordered), f"正在并发处理并发布 {done_count}/{len(ordered)} 个最终文件 (自适应线程: {pool_workers})")
        connection.commit()
        connection.close()
        if real and published_dirs:
            with db() as sweep_connection:
                orphans_archived = sweep_orphan_conflict_copies(output, sweep_connection, target_dirs=published_dirs)
        persist_inventory(run_id)
        cleanup_run_root(run_root)
        with db() as connection:
            connection.execute("UPDATE runs SET status='done',phase='done',phase_done=1,phase_total=1,finished_at=CURRENT_TIMESTAMP WHERE id=?", (run_id,))
        result = report(run_id)
        result.setdefault("metrics", {})["orphans_archived"] = orphans_archived
        if classification:
            result["metrics"].update(classification)
        with db() as connection:
            connection.execute("UPDATE runs SET summary_json=? WHERE id=?", (json.dumps(result, ensure_ascii=False), run_id))
        return result
    except Exception as exc:
        cleanup_run_root(run_root)
        with db() as connection:
            connection.execute("UPDATE runs SET status='failed',finished_at=CURRENT_TIMESTAMP,summary_json=? WHERE id=?", (json.dumps({"error": repr(exc)}, ensure_ascii=False), run_id))
        raise


def sample(paths: list[Path]) -> list[Path]:
    result: list[Path] = []
    def add(values: list[Path]) -> None:
        for value in values:
            if value not in result and len(result) < 10:
                result.append(value)
    add([path for path in paths if path.suffix.lower() == ".ncm"][:1])
    add([path for path in paths if re.search(r"live|remaster|acoustic|radio edit", path.stem, re.I)])
    by_stem: dict[str, list[Path]] = {}
    for path in paths:
        by_stem.setdefault(re.sub(r"\s+", "", path.stem).casefold(), []).append(path)
    for values in by_stem.values():
        if len({path.suffix.lower() for path in values}) > 1:
            add(values[:2])
    add(paths)
    return result


def sample_is_verified(settings: dict) -> bool:
    marker = settings.get("sample_verified") or {}
    run_id = marker.get("run_id") if isinstance(marker, dict) else None
    if not run_id:
        return False
    with db() as connection:
        return bool(connection.execute("SELECT 1 FROM runs WHERE id=? AND mode='sample' AND status='done'", (run_id,)).fetchone())


def run_sample(settings: dict) -> dict:
    source, output = validate_settings(settings)
    cleanup_stale_runs(output)
    paths = sample(list_sources(source))
    if not paths:
        raise RuntimeError("原始文件夹没有可处理的音频或 NCM")
    return run_batch("sample", source, output, paths, False, False)


def run_full(settings: dict, incremental: bool = False) -> dict:
    if not sample_is_verified(settings):
        raise RuntimeError("必须先完成可核验的样本验证")
    source, output = validate_settings(settings)
    cleanup_stale_runs(output)
    all_paths = list_sources(source)
    if not all_paths:
        raise RuntimeError("原始文件夹没有可处理文件")

    if incremental:
        buckets = classify_sources(source, output)
        paths = flatten_buckets(buckets)
        counts = classify_counts(buckets)
        skipped = len(all_paths) - len(paths)
        if skipped > 0:
            log(f"[增量门禁 / Gatekeeper] 源目录 {len(all_paths)} 首：本次处理 {len(paths)} 首（真新增 {counts['new_files']} · 源文件改动 {counts['modified_files']} · 旧规则重判 {counts['stale_decisions']} · 未完成回补 {counts['retry_files']}），跳过 {skipped} 首")
            update_state(f"增量：处理 {len(paths)} 首（新增 {counts['new_files']} / 改动 {counts['modified_files']} / 重判 {counts['stale_decisions']} / 回补 {counts['retry_files']}）", state="running", phase="starting", phase_done=0, phase_total=len(paths))
        if not paths:
            log("[增量门禁 / Gatekeeper] 没有新增、改动、待重判或待回补的曲目，曲库已是最新状态。")
            update_state(f"源目录 {len(all_paths)} 首均已按当前规则处理完成，无需重复处理", state="idle", phase="done", phase_done=1, phase_total=1)
            conn = db()
            try:
                last_run = conn.execute("SELECT id FROM runs WHERE mode='full' AND status='done' ORDER BY started_at DESC LIMIT 1").fetchone()
                if last_run:
                    rep = report(last_run["id"])
                    rep["message"] = f"全部 {len(all_paths)} 首曲目均已在目标目录就绪"
                    return rep
            finally:
                conn.close()
            return {"status": "done", "phase": {"name": "done", "done": 1, "total": 1}, "metrics": {"scanned_total": len(all_paths), "published": len(all_paths), "state_bytes": directory_size(STATE), **counts}, "message": "曲库已是最新状态"}
    else:
        paths = all_paths
        counts = None
        log(f"[全量整理 / Full Run] 开始全量整理：对全部 {len(all_paths)} 首输入曲目执行全局去重并与物理磁盘对齐。")

    return run_batch("full", source, output, paths, True, True, classification=counts)


def classify_sources(source: Path, output: Path | None = None) -> dict[str, list[Path]]:
    """Split the source tree into the things an incremental pass can act on.

    新增 new          — 账本里根本没有这条源文件
    改动 modified     — 有记录，但源文件的大小或修改时间变了
    重判 stale        — 有记录，但当时的判定出自更旧的规则版本
    回补 retry        — 上次没做完：单次偶发失败、输出丢失、输出未打标签
    疑难 unresolvable — 多次重试仍无法解析、或已熔断待人工决策的曲目（不再自动回补拖慢定时）
    """
    buckets: dict[str, list[Path]] = {"new": [], "modified": [], "stale": [], "retry": [], "unresolvable": []}
    conn = db()
    try:
        has_any_output = bool(output and output.exists() and any(p for p in output.rglob("*") if p.is_file() and media(p)))
        for path in list_sources(source):
            try:
                stat = path.stat()
            except OSError:
                continue
            row = conn.execute(
                "SELECT size_bytes,mtime_ns,disposition,output_path,metadata_state,lyrics_state,rules_version,retry_count,unresolvable FROM source_inventory WHERE source_path=?",
                (str(path),),
            ).fetchone()
            if not row:
                row = conn.execute(
                    "SELECT source_size AS size_bytes, source_mtime_ns AS mtime_ns, disposition, output_path, metadata_state, lyrics_state, 0 AS rules_version, 0 AS retry_count, 0 AS unresolvable FROM items WHERE source_path=? AND disposition='published' AND output_path IS NOT NULL ORDER BY id DESC LIMIT 1",
                    (str(path),),
                ).fetchone()
            if row is None:
                buckets["new"].append(path)
                continue
            if row["size_bytes"] != stat.st_size or row["mtime_ns"] != stat.st_mtime_ns:
                buckets["modified"].append(path)
                continue
            disposition = str(row["disposition"])
            unres = bool(row["unresolvable"]) if "unresolvable" in row.keys() else False
            retries = int(row["retry_count"]) if "retry_count" in row.keys() else 0
            if unres or retries >= 2:
                buckets["unresolvable"].append(path)
                continue
            if disposition == "failed":
                if unres or retries >= 2:
                    buckets["unresolvable"].append(path)
                else:
                    buckets["retry"].append(path)
                continue
            if disposition == "superseded":
                # 已被淘汰归档的旧件不复活，哪怕它的判定出自更旧的规则。
                continue
            if disposition.startswith("duplicate_") and not has_any_output:
                buckets["retry"].append(path)
                continue
            if disposition == "published":
                out_str = row["output_path"]
                out_path = Path(out_str) if out_str else None
                meta, lrc = row["metadata_state"] or "", row["lyrics_state"] or ""
                if out_path is None or not out_path.is_file() or out_path.stat().st_size == 0:
                    buckets["retry"].append(path)
                    continue
                if untagged_output(out_path):
                    buckets["retry"].append(path)
                    continue
                if "error" in meta:
                    if unres or retries >= 2:
                        buckets["unresolvable"].append(path)
                    else:
                        buckets["retry"].append(path)
                    continue
                if "error" in lrc:
                    if not unres and retries < 1:
                        buckets["retry"].append(path)
                    continue
            # Only queue for stale re-judgement if explicitly enabled via environment variable
            # (otherwise old runs with rules_version=0 would force every track into a full re-scan).
            if os.environ.get("MUSIC_REJUDGE_ALL") == "1" and int(row["rules_version"] or 0) < RULES_VERSION:
                buckets["stale"].append(path)
                continue
    finally:
        conn.close()
    return buckets


def flatten_buckets(buckets: dict[str, list[Path]]) -> list[Path]:
    seen: set[Path] = set()
    result: list[Path] = []
    for key in ("new", "modified", "stale", "retry"):
        for path in buckets.get(key, []):
            if path not in seen:
                seen.add(path)
                result.append(path)
    return result


def classify_counts(buckets: dict[str, list[Path]]) -> dict[str, int]:
    res = {
        "new_files": len(buckets.get("new", [])),
        "modified_files": len(buckets.get("modified", [])),
        "stale_decisions": len(buckets.get("stale", [])),
        "retry_files": len(buckets.get("retry", [])),
    }
    if buckets.get("unresolvable"):
        res["unresolvable_files"] = len(buckets["unresolvable"])
    return res


def changed_sources(source: Path, output: Path | None = None) -> list[Path]:
    """Every source file the next incremental pass has to touch (all four reasons)."""
    return flatten_buckets(classify_sources(source, output))


def run_incremental(settings: dict) -> dict:
    if not settings.get("initialized"):
        raise RuntimeError("尚未完成首次全量整理")
    source, output = validate_settings(settings)
    cleanup_stale_runs(output)
    buckets = classify_sources(source, output)
    paths = flatten_buckets(buckets)
    counts = classify_counts(buckets)
    unres_cnt = counts.get("unresolvable_files", 0)
    if not paths:
        unres_note = f"（已熔断跳过 {unres_cnt} 首需人工决策的疑难曲目）" if unres_cnt else ""
        return {"metrics": {"scanned_total": 0, "published": 0, "state_bytes": directory_size(STATE), "orphans_archived": 0, **counts}, "message": f"没有新增、改动或待回补的曲目{unres_note}"}
    unres_note = f" · 熔断待处理 {unres_cnt}" if unres_cnt else ""
    log(f"[增量门禁 / Gatekeeper] 本次处理 {len(paths)} 首：真新增 {counts['new_files']} · 源文件改动 {counts['modified_files']} · 旧规则重判 {counts['stale_decisions']} · 未完成回补 {counts['retry_files']}{unres_note}")
    return run_batch("incremental", source, output, paths, True, True, classification=counts)


def recover_interrupted_runs() -> None:
    with db() as connection:
        for row in connection.execute("SELECT id,output_dir FROM runs WHERE status='running'").fetchall():
            output = Path(row["output_dir"])
            if output.is_dir():
                cleanup_stale_runs(output)
            connection.execute("UPDATE runs SET status='interrupted',finished_at=CURRENT_TIMESTAMP,summary_json=? WHERE id=?", (json.dumps({"error": "container interrupted; temporary audio cleaned"}, ensure_ascii=False), row["id"]))


def run(mode: str) -> None:
    assert_state_budget()
    recover_interrupted_runs()
    settings = cfg()
    validate_settings(settings)
    labels = {"sample": "开始验证样本", "full": "开始全量整理", "incremental": "开始增量整理（新增/改动/重判/回补）"}
    if mode not in labels:
        raise RuntimeError("unknown mode")
    update_state(labels[mode], state="running", workers=workers(), state_bytes=directory_size(STATE))
    try:
        if mode == "sample":
            result = run_sample(settings)
            settings["sample_verified"] = {"run_id": result["run_id"], "at": time.time()}
            save_cfg(settings)
            message = "样本验证完成；整理后目录未写入音乐"
        elif mode == "full":
            result = run_full(settings)
            settings["initialized"] = True
            save_cfg(settings)
            log("=" * 64)
            log("【全量整理完成】曲库已成功发布并与物理磁盘对齐！")
            log("💡 拾遗提示：若源目录中仍有少量遗留文件，通常是由于网络瞬时抖动或冷门歌曲初次未命中。推荐直接再次点击【增量整理】或【重新执行全量对齐】复跑一次，系统支持声纹缓存，耗时极短。")
            log("⚠️ 顽固文件：若复跑后仍有极少数顽固文件（本地命名严重残缺、翻唱争议或重复冲突），本程序遵循安全审慎原则未强行挪动。程序并非万能，这类文件建议您在源目录手动核对或直接删除。")
            log("=" * 64)
            message = "全量整理完成；临时音频已清理"
        else:
            result = run_incremental(settings)
            metrics = result.get("metrics", {})
            log("=" * 64)
            log("【增量整理完成】新增/改动/重判/回补曲目已成功同步！")
            log("💡 提示：若源目录仍有极少数疑难顽固文件未被处理，程序秉承安全第一原则未强行猜测改写，建议您在源目录手动核对或清理。")
            log("=" * 64)
            message = "增量整理完成：新增 {new} · 改动 {mod} · 重判 {stale} · 回补 {retry}；临时音频已清理".format(
                new=metrics.get("new_files", 0), mod=metrics.get("modified_files", 0),
                stale=metrics.get("stale_decisions", 0), retry=metrics.get("retry_files", 0),
            )
        update_state(message, state="done", report=result, state_bytes=directory_size(STATE))
    except Exception as exc:
        update_state("整理失败；原始音乐未被修改，临时音频已清理", state="failed", error=repr(exc), state_bytes=directory_size(STATE))
        raise


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in {"sample", "full", "incremental"}:
        raise SystemExit("usage: pipeline.py sample|full|incremental")
    run(sys.argv[1])
