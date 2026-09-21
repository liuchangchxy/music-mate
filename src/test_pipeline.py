#!/usr/bin/env python3
"""Regression tests for v6 bounded-storage behavior."""
from __future__ import annotations

import atexit
import base64
import importlib.util
import json
import os
import re
import shutil
import sqlite3
import struct
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

# STATE/STATUS/LEDGER are resolved once at import time, so the suite must point
# MUSIC_STATE at a scratch directory *before* loading the modules: patching
# pipeline.STATE inside a test does not move the derived paths, and a run on a
# host without a writable /state would die on PermissionError.
_TEST_STATE = Path(tempfile.mkdtemp(prefix="musicmate-tests-"))
atexit.register(shutil.rmtree, _TEST_STATE, ignore_errors=True)
os.environ["MUSIC_STATE"] = str(_TEST_STATE)


def load(name: str, filename: str):
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


pipeline = load("music_pipeline_v6", "pipeline.py")
app = load("music_app_v6", "app.py")


def riff_file(*chunks: tuple[bytes, bytes]) -> bytes:
    """Assemble a minimal RIFF/WAVE payload from (chunk_id, data) pairs."""
    body = b"".join(cid + struct.pack("<I", len(data)) + data + (b"\0" if len(data) % 2 else b"") for cid, data in chunks)
    return b"RIFF" + struct.pack("<I", len(body) + 4) + b"WAVE" + body


def riff_chunk_ids(payload: bytes) -> list[bytes]:
    ids, pos = [], 12
    while pos + 8 <= len(payload):
        size = struct.unpack("<I", payload[pos + 4:pos + 8])[0]
        ids.append(payload[pos:pos + 4])
        pos += 8 + size + (size & 1)
    return ids


class PipelineTests(unittest.TestCase):
    def test_json_parser_ignores_command_noise(self) -> None:
        self.assertEqual(pipeline.json_array("notice\n[{\"id\":\"group\"}]\n"), [{"id": "group"}])

    def test_source_scan_skips_hidden_and_old_queue_directories(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name in ("ok.mp3", ".hidden/ignored.flac", "失败/ignored.ogg"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            self.assertEqual(pipeline.list_sources(root), [root / "ok.mp3"])

    def test_state_directory_rejects_audio_and_images(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)):
            (Path(directory) / "bad.mp3").touch()
            with self.assertRaisesRegex(RuntimeError, "不得保存"):
                pipeline.assert_state_budget()

    def test_state_limit_is_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "STATE_LIMIT", 4):
            (Path(directory) / "ledger-v6.sqlite").write_bytes(b"12345")
            with self.assertRaisesRegex(RuntimeError, "超过上限"):
                pipeline.assert_state_budget()

    def test_full_refuses_nonempty_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            output.mkdir()
            (output / "existing.mp3").touch()
            self.assertFalse(pipeline.output_is_clean(output))

    def test_pipeline_temp_is_not_formal_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            output.mkdir()
            (output / ".music-rebuild-run-stale").mkdir()
            self.assertTrue(pipeline.output_is_clean(output))
            pipeline.cleanup_stale_runs(output)
            self.assertEqual(list(output.iterdir()), [])

    def test_copy_checked_does_not_touch_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, target = root / "source.mp3", root / "out" / "target.mp3"
            source.write_bytes(b"immutable source")
            before = pipeline.sha256(source)
            pipeline.copy_checked(source, target, before)
            self.assertEqual(pipeline.sha256(source), before)
            self.assertEqual(target.read_bytes(), b"immutable source")

    def test_clone_file_calls_reflink_or_copy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            s = Path(directory) / "src.mp3"
            t = Path(directory) / "dst.mp3"
            s.write_bytes(b"reflink clone content")
            pipeline.clone_file(s, t)
            self.assertEqual(t.read_bytes(), b"reflink clone content")

    def test_destination_sanitizes_tags_and_conflicts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root, source = Path(directory), Path(directory) / "in.mp3"
            source.touch()
            with patch.object(pipeline, "probe", return_value=(1.0, 1, {"artist": "A/B", "album": "X", "title": "T"}, False)), patch.object(pipeline, "sha256", return_value="a" * 64):
                self.assertEqual(pipeline.destination(root, source, "a" * 64).relative_to(root), Path("A-B") / "X" / "T.mp3")

    def test_sample_is_bounded_and_prefers_ncm(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / "song.ncm", root / "song.mp3", root / "song.flac"] + [root / f"x{index}.mp3" for index in range(20)]
            for path in paths: path.touch()
            chosen = pipeline.sample(paths)
            self.assertEqual(chosen[0], root / "song.ncm")
            self.assertLessEqual(len(chosen), 10)

    def test_cli_rejects_old_four_directory_configuration(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "只能包含"):
            pipeline.validate_settings({"seed_dir": "/vol1/a", "input_dir": "/vol1/b", "output_dir": "/vol1/c", "work_dir": "/vol1/d"})

    def test_full_requires_verified_sample_before_path_access(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "样本"):
            pipeline.run_full({"source_dir": "/vol1/source", "output_dir": "/vol1/output"})

    def test_report_has_storage_and_duplicate_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            connection = pipeline.db()
            try:
                connection.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('r','sample','/vol1/a','/vol1/b','done')")
                connection.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,source_kind,disposition,lyrics_state,cover_state) VALUES('r','a.mp3',1,1,'audio','duplicate_exact','lyrics_embedded','cover_embedded')")
                connection.execute("INSERT INTO groups(id,run_id,tool,decision,paths_json) VALUES('g','r','dupsonic','keep_all_ambiguous','[]')")
                connection.commit()
            finally:
                connection.close()
            result = pipeline.report("r")
            self.assertEqual(result["metrics"]["exact_duplicate"], 1)
            self.assertEqual(result["metrics"]["lyrics_embedded"], 1)
            self.assertIn("state_bytes", result["metrics"])

    def test_adaptive_metadata_workers(self) -> None:
        with patch.dict(os.environ, {"MUSIC_METADATA_WORKERS": "auto"}, clear=False):
            with patch.object(pipeline, "workers", return_value=6):
                self.assertEqual(pipeline.metadata_workers(), 6)
            with patch.object(pipeline, "workers", return_value=16):
                self.assertEqual(pipeline.metadata_workers(), 8)
            with patch.object(pipeline, "workers", return_value=1):
                self.assertEqual(pipeline.metadata_workers(), 2)
        with patch.dict(os.environ, {"MUSIC_METADATA_WORKERS": "4"}, clear=False):
            self.assertEqual(pipeline.metadata_workers(), 4)

    def test_find_cached_publish(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            src = Path(directory) / "song.mp3"
            src.write_bytes(b"content")
            out_dir = Path(directory) / "out"
            out_dir.mkdir()
            target = out_dir / "Artist" / "Album" / "song.mp3"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"content")

            stat = src.stat()
            conn = pipeline.db()
            try:
                conn.execute(
                    "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,metadata_state,lyrics_state,cover_state,rules_version) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (str(src), stat.st_size, stat.st_mtime_ns, "dummy", "published", str(target), "existing_tags", "lyrics_embedded", "cover_embedded", pipeline.RULES_VERSION)
                )
                conn.commit()
            finally:
                conn.close()
            cached = pipeline.find_cached_publish(src, out_dir)
            self.assertIsNotNone(cached)
            self.assertEqual(cached[0], target)
            self.assertEqual(cached[1], ("existing_tags", "lyrics_embedded", "cover_embedded"))
            self.assertEqual(cached[2], "dummy")

    def test_untagged_published_tracks_are_not_treated_as_cached(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            src = Path(directory) / "song.wav"
            src.write_bytes(b"content")
            out_dir = Path(directory) / "out"
            target = out_dir / "Unknown Artist" / "Unknown Album" / "song.wav"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"content")

            stat = src.stat()
            conn = pipeline.db()
            try:
                conn.execute(
                    "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,metadata_state,lyrics_state,cover_state) VALUES(?,?,?,?,?,?,?,?,?)",
                    (str(src), stat.st_size, stat.st_mtime_ns, "dummy", "published", str(target), "metadata_not_found", "lyrics_not_found", "cover_not_found"),
                )
                conn.commit()
            finally:
                conn.close()
            # Re-running must re-enrich it rather than silently reusing the unsorted copy.
            self.assertIsNone(pipeline.find_cached_publish(src, out_dir))
            self.assertTrue(pipeline.untagged_output(target))
            self.assertFalse(pipeline.untagged_output(out_dir / "周杰伦" / "叶惠美" / "song.wav"))

    def test_restore_sidecar_from_knowledge_base(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            out_dir = Path(directory) / "out"
            wav = out_dir / "Singer" / "Album" / "Song.wav"
            wav.parent.mkdir(parents=True)
            wav.write_bytes(b"wav content")
            digest = "e" * 64

            self.assertFalse(pipeline.restore_sidecar(wav, digest))
            conn = pipeline.db()
            try:
                conn.execute(
                    "INSERT INTO knowledge_base(audio_sha256,artist,album,title,lyrics,has_cover) VALUES(?,?,?,?,?,?)",
                    (digest, "Singer", "Album", "Song", "[00:01.00]歌词", 0),
                )
                conn.commit()
            finally:
                conn.close()

            self.assertTrue(pipeline.restore_sidecar(wav, digest))
            self.assertEqual(wav.with_suffix(".lrc").read_text(encoding="utf-8"), "[00:01.00]歌词")
            # An embeddable container never gets a sidecar, and an existing one is kept as-is.
            self.assertFalse(pipeline.restore_sidecar(wav, ""))




    def test_publish_one_reuses_decoded_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_root = Path(directory) / "run_root"
            run_root.mkdir()
            out_dir = Path(directory) / "output"
            out_dir.mkdir()
            src = Path(directory) / "song.ncm"
            src.write_bytes(b"encrypted ncm content")
            decoded = run_root / "ncm" / "song.flac"
            decoded.parent.mkdir()
            decoded.write_bytes(b"decoded flac content")

            with patch.object(pipeline, "probe", return_value=(180.0, len(b"decoded flac content"), {"title": "Song", "artist": "Singer"}, False)), \
                 patch.object(pipeline, "enrich", return_value=("existing_tags", "lyrics_already_present", "cover_already_present")), \
                 patch.object(pipeline, "decode_to_directory") as mock_decode:
                target, states = pipeline.publish_one(src, run_root, out_dir, real=True, decoded_source=decoded)
                mock_decode.assert_not_called()
                self.assertIsNotNone(target)
                self.assertEqual(target.read_bytes(), b"decoded flac content")
                self.assertEqual(states, ("existing_tags", "lyrics_already_present", "cover_already_present"))

    def test_publish_one_carries_sidecar_lyrics_into_output(self) -> None:
        """WAV/AIFF lyrics live in a same-name .lrc; it must survive publishing."""
        with tempfile.TemporaryDirectory() as directory:
            run_root = Path(directory) / "run_root"
            run_root.mkdir()
            out_dir = Path(directory) / "output"
            out_dir.mkdir()
            src = Path(directory) / "song.wav"
            src.write_bytes(b"wav payload")

            def fake_enrich(temporary: Path, *_args):
                temporary.with_suffix(".lrc").write_text("[00:01.00]外挂歌词", encoding="utf-8")
                return ("metadata_matched", "lyrics_embedded", "cover_embedded")

            with patch.object(pipeline, "probe", return_value=(180.0, len(b"wav payload"), {"title": "Song", "artist": "Singer"}, False)), \
                 patch.object(pipeline, "enrich", side_effect=fake_enrich):
                target, _states = pipeline.publish_one(src, run_root, out_dir, real=True)
                self.assertIsNotNone(target)
                self.assertEqual(target.suffix, ".wav")
                sidecar = target.with_suffix(".lrc")
                self.assertTrue(sidecar.is_file())
                self.assertEqual(sidecar.read_text(encoding="utf-8"), "[00:01.00]外挂歌词")
            # The sandbox (and any leftover .lrc inside it) is gone after publishing.
            self.assertEqual(list(run_root.rglob("*.lrc")), [])

    def test_publish_one_falls_back_to_the_original_file_name(self) -> None:
        """The sandbox adds a random suffix; the real name must reach the tag fallback."""
        with tempfile.TemporaryDirectory() as directory:
            run_root = Path(directory) / "run_root"
            run_root.mkdir()
            out_dir = Path(directory) / "output"
            out_dir.mkdir()
            src = Path(directory) / "周杰倫 - 東風破.flac"
            src.write_bytes(b"flac payload")

            with patch.object(pipeline, "probe", return_value=(200.0, len(b"flac payload"), {}, False)), \
                 patch.object(pipeline, "enrich", return_value=("existing_tags", "lyrics_not_found", "cover_not_found")) as mock_enrich:
                target, _states = pipeline.publish_one(src, run_root, out_dir, real=True)
                self.assertEqual(mock_enrich.call_args[0][3], "周杰倫 - 東風破")
                # Nothing was written to the tags, so the published name keeps the original title.
                self.assertEqual(target.relative_to(out_dir), Path("Unknown Artist") / "Unknown Album" / "周杰倫 - 東風破.flac")

    def test_publish_one_overwrites_instead_of_forking(self) -> None:
        """同一源重新发布时必须就地覆盖，绝不再分叉出「名 (2).ext」。"""
        with tempfile.TemporaryDirectory() as directory:
            run_root = Path(directory) / "run_root"
            run_root.mkdir()
            out_dir = Path(directory) / "output"
            out_dir.mkdir()
            src = Path(directory) / "song.mp3"
            src.write_bytes(b"new bytes")
            target = out_dir / "Singer" / "Unknown Album" / "Song.mp3"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"old bytes of the same source")

            with patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"), \
                 patch.object(pipeline, "probe", return_value=(180.0, len(b"new bytes"), {"title": "Song", "artist": "Singer"}, False)), \
                 patch.object(pipeline, "enrich", return_value=("existing_tags", "lyrics_embedded", "cover_embedded")):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,rules_version) VALUES(?,?,?,?,'published',?,?)",
                                 (str(src), 9, 1, "old", str(target), pipeline.RULES_VERSION))
                    conn.commit()
                finally:
                    conn.close()
                published, _ = pipeline.publish_one(src, run_root, out_dir, real=True, kept={src})

            self.assertEqual(published, target)
            self.assertEqual(target.read_bytes(), b"new bytes")
            self.assertEqual([p.name for p in target.parent.iterdir()], ["Song.mp3"])

    def test_publish_one_takes_over_a_losing_duplicates_path(self) -> None:
        """去重败者在同一路径留下的旧件要被归档，不能靠新开 (2) 副本绕开。"""
        with tempfile.TemporaryDirectory() as directory:
            run_root = Path(directory) / "run_root"
            run_root.mkdir()
            out_dir = Path(directory) / "output"
            out_dir.mkdir()
            winner = Path(directory) / "winner.mp3"
            winner.write_bytes(b"winner bytes")
            loser = Path(directory) / "loser.mp3"
            loser.write_bytes(b"loser bytes")
            target = out_dir / "Singer" / "Unknown Album" / "Song.mp3"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"loser published bytes")

            with patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"), \
                 patch.object(pipeline, "probe", return_value=(180.0, len(b"winner bytes"), {"title": "Song", "artist": "Singer"}, False)), \
                 patch.object(pipeline, "enrich", return_value=("existing_tags", "lyrics_embedded", "cover_embedded")):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,rules_version) VALUES(?,?,?,?,'published',?,?)",
                                 (str(loser), 11, 1, "loser", str(target), pipeline.RULES_VERSION))
                    conn.commit()
                finally:
                    conn.close()
                published, _ = pipeline.publish_one(winner, run_root, out_dir, real=True, kept={winner})
                conn = pipeline.db()
                try:
                    row = conn.execute("SELECT disposition, output_path, rules_version FROM source_inventory WHERE source_path=?", (str(loser),)).fetchone()
                finally:
                    conn.close()

            self.assertEqual(published, target)
            self.assertEqual(target.read_bytes(), b"winner bytes")
            self.assertEqual(row["disposition"], "superseded")
            self.assertEqual(row["rules_version"], pipeline.RULES_VERSION)   # 不会在下次运行被重复重判
            archived = out_dir / ".music-archive" / "Singer" / "Unknown Album" / "Song.mp3"
            self.assertEqual(archived.read_bytes(), b"loser published bytes")

    def test_sweep_archives_only_unowned_conflict_copies(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            out_dir = Path(directory) / "output"
            folder = out_dir / "阿桑" / "寂寞在唱歌"
            folder.mkdir(parents=True)
            base = folder / "一直很安静.mp3"
            base.write_bytes(b"base")
            orphan = folder / "一直很安静 (2).mp3"
            orphan.write_bytes(b"orphan")
            owned_base = folder / "Other.mp3"
            owned_base.write_bytes(b"o")
            owned_copy = folder / "Other (2).mp3"
            owned_copy.write_bytes(b"o2")
            lonely = folder / "User (2).mp3"
            lonely.write_bytes(b"user")

            with patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
                conn = pipeline.db()
                try:
                    for path in (base, owned_base, owned_copy):
                        conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,rules_version) VALUES(?,?,?,?,'published',?,?)",
                                     (str(path) + ".src", 1, 1, "h", str(path), pipeline.RULES_VERSION))
                    conn.commit()
                    self.assertEqual(pipeline.sweep_orphan_conflict_copies(out_dir, conn), 1)
                finally:
                    conn.close()

            self.assertTrue(base.is_file())
            self.assertFalse(orphan.exists())
            self.assertEqual((out_dir / ".music-archive" / "阿桑" / "寂寞在唱歌" / "一直很安静 (2).mp3").read_bytes(), b"orphan")
            self.assertTrue(owned_copy.is_file())   # 自己有账 → 不动
            self.assertTrue(lonely.is_file())       # 兄弟无账 → 不是我们分叉出来的 → 不动

    def test_sweep_orphan_conflict_copies_recovers_ledger_when_conflict_was_tracked(self) -> None:
        """当账本错误指向了 (2) 冲突件而基准文件在磁盘未登记时，应纠正账本指向基准文件并归档 (2)。"""
        with tempfile.TemporaryDirectory() as directory:
            out_dir = Path(directory) / "output"
            folder = out_dir / "Singer" / "Album"
            folder.mkdir(parents=True)
            base = folder / "Song.mp3"
            base.write_bytes(b"music content")
            copy = folder / "Song (2).mp3"
            copy.write_bytes(b"music content")

            with patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"), \
                 patch.object(pipeline, "probe", return_value=(180.0, len(b"music content"), {}, False)):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,rules_version) VALUES(?,?,?,?,'published',?,?)",
                                 ("/source/song.mp3", len(b"music content"), 1, "hash1", str(copy), pipeline.RULES_VERSION))
                    conn.commit()
                    archived_cnt = pipeline.sweep_orphan_conflict_copies(out_dir, conn)
                    self.assertEqual(archived_cnt, 1)
                    row = conn.execute("SELECT output_path FROM source_inventory WHERE source_path='/source/song.mp3'").fetchone()
                    self.assertEqual(row["output_path"], str(base))
                finally:
                    conn.close()

            self.assertTrue(base.is_file())
            self.assertFalse(copy.exists())
            self.assertTrue((out_dir / ".music-archive" / "Singer" / "Album" / "Song (2).mp3").is_file())

    def test_destination_uses_fallback_stem_when_tags_are_empty(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "song-9f2a1b3c.flac"
            source.touch()
            with patch.object(pipeline, "probe", return_value=(1.0, 1, {}, False)):
                named = pipeline.destination(Path("/out"), source, "digest", "罗大佑 - 皇后大道东")
                plain = pipeline.destination(Path("/out"), source, "digest", None)
            self.assertEqual(named.name, "罗大佑 - 皇后大道东.flac")
            self.assertEqual(plain.name, "song-9f2a1b3c.flac")

    def test_report_total_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            connection = pipeline.db()
            try:
                connection.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('r1','full','/vol1/a','/vol1/b','done')")
                connection.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,source_kind,disposition,lyrics_state,cover_state) VALUES('r1','a.mp3',1,1,'audio','published','lyrics_already_present','cover_embedded')")
                connection.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,source_kind,disposition,lyrics_state,cover_state) VALUES('r1','b.mp3',1,1,'audio','published','lyrics_embedded','cover_already_present')")
                connection.commit()
            finally:
                connection.close()
            rep = pipeline.report("r1")
            m = rep["metrics"]
            self.assertEqual(m["lyrics_embedded"], 1)
            self.assertEqual(m["lyrics_already_present"], 1)
            self.assertEqual(m["lyrics_total_with"], 2)
            self.assertEqual(m["cover_embedded"], 1)
            self.assertEqual(m["cover_already_present"], 1)
            self.assertEqual(m["cover_total_with"], 2)

    def test_changed_sources_recovers_missing_and_failed_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            src_dir = Path(directory) / "source"
            src_dir.mkdir()
            out_dir = Path(directory) / "output"
            out_dir.mkdir()

            s_ok = src_dir / "ok.mp3"
            s_ok.write_bytes(b"ok content")
            t_ok = out_dir / "ok.mp3"
            t_ok.write_bytes(b"ok content")

            s_missing_out = src_dir / "missing_out.mp3"
            s_missing_out.write_bytes(b"missing out content")

            s_failed = src_dir / "failed.mp3"
            s_failed.write_bytes(b"failed content")

            s_new = src_dir / "brand_new.mp3"
            s_new.write_bytes(b"brand new")

            conn = pipeline.db()
            try:
                # ok file: published and output file exists
                conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,rules_version) VALUES(?,?,?,?,'published',?,?)",
                             (str(s_ok), s_ok.stat().st_size, s_ok.stat().st_mtime_ns, "hash1", str(t_ok), pipeline.RULES_VERSION))
                # missing out: published but output file deleted
                conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,rules_version) VALUES(?,?,?,?,'published',?,?)",
                             (str(s_missing_out), s_missing_out.stat().st_size, s_missing_out.stat().st_mtime_ns, "hash2", str(out_dir / "missing_out.mp3"), pipeline.RULES_VERSION))
                # failed file: disposition failed
                conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,rules_version) VALUES(?,?,?,?,'failed',NULL,?)",
                             (str(s_failed), s_failed.stat().st_size, s_failed.stat().st_mtime_ns, "hash3", pipeline.RULES_VERSION))
                conn.commit()
            finally:
                conn.close()

            changed = pipeline.changed_sources(src_dir, out_dir)
            self.assertNotIn(s_ok, changed)
            self.assertIn(s_missing_out, changed)
            self.assertIn(s_failed, changed)
            self.assertIn(s_new, changed)

    def test_changed_sources_retries_tracks_stuck_in_unknown_artist(self) -> None:
        """飞牛音乐按 Artist/Album 建索引，无标签曲目必须重试补全。"""
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            src_dir, out_dir = Path(directory) / "source", Path(directory) / "output"
            src_dir.mkdir()
            out_dir.mkdir()
            stuck = src_dir / "一直很安静.wav"
            stuck.write_bytes(b"wav content")
            stuck_out = out_dir / "Unknown Artist" / "Unknown Album" / "一直很安静.wav"
            stuck_out.parent.mkdir(parents=True)
            stuck_out.write_bytes(b"wav content")

            conn = pipeline.db()
            try:
                conn.execute(
                    "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,metadata_state,lyrics_state) VALUES(?,?,?,?,'published',?,?,?)",
                    (str(stuck), stuck.stat().st_size, stuck.stat().st_mtime_ns, "hash", str(stuck_out), "metadata_not_found", "lyrics_not_found"),
                )
                conn.commit()
            finally:
                conn.close()

            self.assertIn(stuck, pipeline.changed_sources(src_dir, out_dir))

    def test_classify_sources_separates_the_four_reasons(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            src_dir, out_dir = Path(directory) / "source", Path(directory) / "output"
            src_dir.mkdir()
            out_dir.mkdir()

            def make(name: str) -> tuple[Path, Path]:
                path, target = src_dir / name, out_dir / name
                path.write_bytes(b"payload")
                target.write_bytes(b"payload")
                return path, target

            fresh, _ = make("fresh.mp3")                                # 账本无记录
            changed, _ = make("changed.mp3")                            # 源文件改动
            stale, _ = make("stale.mp3")                                # 旧规则判定
            retried, retried_out = make("retried.mp3")                  # 上次歌词失败
            fine, _ = make("fine.mp3")                                  # 已按当前规则完成

            conn = pipeline.db()
            try:
                def insert(path: Path, target: Path, version: int, lyrics: str = "lyrics_embedded", size: int | None = None) -> None:
                    conn.execute(
                        "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,metadata_state,lyrics_state,cover_state,rules_version) VALUES(?,?,?,?,'published',?,'existing_tags',?,'cover_embedded',?)",
                        (str(path), size if size is not None else path.stat().st_size, path.stat().st_mtime_ns, "h", str(target), lyrics, version),
                    )

                insert(changed, out_dir / "changed.mp3", pipeline.RULES_VERSION, size=1)
                insert(stale, out_dir / "stale.mp3", pipeline.RULES_VERSION - 1)
                insert(retried, retried_out, pipeline.RULES_VERSION, lyrics="lyrics_tool_error")
                insert(fine, out_dir / "fine.mp3", pipeline.RULES_VERSION)
                conn.commit()
            finally:
                conn.close()

            with patch.dict(os.environ, {"MUSIC_REJUDGE_ALL": "1"}):
                buckets = pipeline.classify_sources(src_dir, out_dir)
                self.assertEqual([p.name for p in buckets["new"]], ["fresh.mp3"])
                self.assertEqual([p.name for p in buckets["modified"]], ["changed.mp3"])
                self.assertEqual([p.name for p in buckets["stale"]], ["stale.mp3"])
                self.assertEqual([p.name for p in buckets["retry"]], ["retried.mp3"])
                counts = pipeline.classify_counts(buckets)
                self.assertEqual(counts, {"new_files": 1, "modified_files": 1, "stale_decisions": 1, "retry_files": 1})
                self.assertNotIn(fine, pipeline.changed_sources(src_dir, out_dir))

    def test_archived_tracks_are_never_resurrected(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            src_dir, out_dir = Path(directory) / "source", Path(directory) / "output"
            src_dir.mkdir()
            out_dir.mkdir()
            archived_src = src_dir / "old.mp3"
            archived_src.write_bytes(b"payload")
            archive_path = out_dir / ".music-archive" / "A" / "old.mp3"
            archive_path.parent.mkdir(parents=True)
            archive_path.write_bytes(b"payload")

            conn = pipeline.db()
            try:
                # 旧规则淘汰、且版本戳落后：仍然不能再被拉回曲库
                conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,rules_version) VALUES(?,?,?,?,'superseded',?,?)",
                             (str(archived_src), archived_src.stat().st_size, archived_src.stat().st_mtime_ns, "h", str(archive_path), 0))
                conn.commit()
            finally:
                conn.close()

            self.assertEqual(pipeline.changed_sources(src_dir, out_dir), [])

    def test_stale_rule_version_invalidates_the_publish_cache(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            src = Path(directory) / "song.mp3"
            src.write_bytes(b"content")
            out_dir = Path(directory) / "out"
            target = out_dir / "Artist" / "Album" / "song.mp3"
            target.parent.mkdir(parents=True)
            target.write_bytes(b"content")

            def seed(version: int) -> None:
                conn = pipeline.db()
                try:
                    conn.execute("DELETE FROM source_inventory")
                    conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,metadata_state,lyrics_state,cover_state,rules_version) VALUES(?,?,?,?,'published',?,'existing_tags','lyrics_embedded','cover_embedded',?)",
                                 (str(src), src.stat().st_size, src.stat().st_mtime_ns, "dummy", str(target), version))
                    conn.commit()
                finally:
                    conn.close()

            seed(pipeline.RULES_VERSION - 1)
            self.assertIsNone(pipeline.find_cached_publish(src, out_dir))
            seed(pipeline.RULES_VERSION)
            self.assertIsNotNone(pipeline.find_cached_publish(src, out_dir))

    def test_republished_track_archives_the_previous_location(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            src = Path(directory) / "source" / "一直很安静.wav"
            src.parent.mkdir()
            src.write_bytes(b"src")
            out_dir = Path(directory) / "output"
            stale = out_dir / "Unknown Artist" / "Unknown Album" / "一直很安静.wav"
            stale.parent.mkdir(parents=True)
            stale.write_bytes(b"old copy")
            fresh = out_dir / "阿桑" / "寂寞在唱歌" / "一直很安静.wav"
            fresh.parent.mkdir(parents=True)
            fresh.write_bytes(b"new copy")

            conn = pipeline.db()
            try:
                conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path) VALUES(?,?,?,?,'published',?)",
                             (str(src), 3, 1, "hash", str(stale)))
                conn.commit()
            finally:
                conn.close()

            pipeline.archive_previous_output(src, out_dir, fresh)
            self.assertFalse(stale.exists())
            self.assertTrue(fresh.is_file())
            archived = out_dir / ".music-archive" / "Unknown Artist" / "Unknown Album" / "一直很安静.wav"
            self.assertTrue(archived.is_file())
            # Re-publishing into the same location leaves both the file and the archive alone.
            pipeline.archive_previous_output(src, out_dir, fresh)
            self.assertTrue(fresh.is_file())

    def test_normalize_track_stem(self) -> None:
        self.assertEqual(pipeline.normalize_track_stem("周杰伦 - 晴天 [FLAC]"), "周杰伦 - 晴天")
        self.assertEqual(pipeline.normalize_track_stem("周杰伦 - 晴天 (320k)"), "周杰伦 - 晴天")
        self.assertEqual(pipeline.normalize_track_stem("周杰伦 - 晴天 [24bit-96k]"), "周杰伦 - 晴天")
        self.assertEqual(pipeline.normalize_track_stem("晴天 [mqms2]"), "晴天")
        self.assertEqual(pipeline.normalize_track_stem("01. 晴天 （320kbps）"), "01. 晴天")
        self.assertEqual(pipeline.normalize_track_stem("晴天 (Live)"), "晴天 (live)")

    def test_audio_quality_score(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            flac = Path(d) / "song.flac"
            mp3_hi = Path(d) / "song.mp3"
            mp3_lo = Path(d) / "song_lo.mp3"
            flac.write_bytes(b"x" * 2000)
            mp3_hi.write_bytes(b"x" * 1000)
            mp3_lo.write_bytes(b"x" * 500)
            score_flac = pipeline.audio_quality_score(flac)
            score_hi = pipeline.audio_quality_score(mp3_hi)
            score_lo = pipeline.audio_quality_score(mp3_lo)
            self.assertGreater(score_flac, score_hi)
            self.assertGreater(score_hi, score_lo)

    def test_hierarchical_blocking_dedupe_same_recording(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d) / "state"
            src_dir = Path(d) / "source"
            state_dir.mkdir()
            src_dir.mkdir()
            flac = src_dir / "Song [FLAC].flac"
            mp3 = src_dir / "Song (320k).mp3"
            flac.write_bytes(b"flac_data" * 200)
            mp3.write_bytes(b"mp3_data" * 100)

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('r1','sample','/s','/o','running')")
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('r1',?,1000,1,210.5,'audio','candidate')", (str(flac),))
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('r1',?,500,1,210.8,'audio','candidate')", (str(mp3),))
                    conn.commit()
                finally:
                    conn.close()

                candidates = {flac, mp3}
                remaining = pipeline.acoustic_dedupe("r1", src_dir, candidates, library=None)
                self.assertEqual(remaining, {flac})
                self.assertNotIn(mp3, remaining)

                conn = pipeline.db()
                try:
                    item = conn.execute("SELECT disposition FROM items WHERE run_id='r1' AND source_path=?", (str(mp3),)).fetchone()
                    self.assertEqual(item["disposition"], "duplicate_same_recording")
                    grp = conn.execute("SELECT tool, decision, winner_source_path FROM groups WHERE run_id='r1'").fetchone()
                    self.assertEqual(grp["tool"], "hierarchical_blocking")
                    self.assertEqual(grp["decision"], "keep_best")
                    self.assertEqual(grp["winner_source_path"], str(flac))
                finally:
                    conn.close()

    def test_hierarchical_blocking_preserves_different_durations(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d) / "state"
            src_dir = Path(d) / "source"
            state_dir.mkdir()
            src_dir.mkdir()
            v1 = src_dir / "Intro.flac"
            v2 = src_dir / "Intro.mp3"
            v1.write_bytes(b"v1" * 100)
            v2.write_bytes(b"v2" * 100)

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('r2','sample','/s','/o','running')")
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('r2',?,100,1,30.0,'audio','candidate')", (str(v1),))
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('r2',?,100,1,180.0,'audio','candidate')", (str(v2),))
                    conn.commit()
                finally:
                    conn.close()

                candidates = {v1, v2}
                remaining = pipeline.acoustic_dedupe("r2", src_dir, candidates, library=None)
                self.assertEqual(remaining, {v1, v2})

    def test_hierarchical_blocking_preserves_variant_recordings(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d) / "state"
            src_dir = Path(d) / "source"
            state_dir.mkdir()
            src_dir.mkdir()
            studio = src_dir / "Track.flac"
            live = src_dir / "Track (Live).flac"
            studio.write_bytes(b"s" * 100)
            live.write_bytes(b"l" * 100)

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('r3','sample','/s','/o','running')")
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('r3',?,100,1,200.0,'audio','candidate')", (str(studio),))
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('r3',?,100,1,201.0,'audio','candidate')", (str(live),))
                    conn.commit()
                finally:
                    conn.close()

                candidates = {studio, live}
                remaining = pipeline.acoustic_dedupe("r3", src_dir, candidates, library=None)
                self.assertEqual(remaining, {studio, live})

    def test_ncm_persistent_cache_hit(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d) / "state"
            src_dir = Path(d) / "source"
            out_dir = Path(d) / "output"
            run_root = out_dir / ".music-rebuild-run-r1"
            state_dir.mkdir()
            src_dir.mkdir()
            out_dir.mkdir()
            run_root.mkdir()

            ncm_file = src_dir / "song.ncm"
            ncm_file.write_bytes(b"dummy ncm data")
            decoded_flac = run_root / "temp.flac"
            decoded_flac.write_bytes(b"decoded flac data")

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"), \
                 patch.object(pipeline, "probe", return_value=(150.0, 100, {"title": "Song", "artist": "Singer"}, False)):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('r1','full','/s','/o','running')")
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('r1',?,100,1,150.0,'ncm','candidate')", (str(ncm_file),))
                    conn.commit()
                finally:
                    conn.close()

                with patch.object(pipeline, "decode_to_directory", return_value=decoded_flac) as mock_decode:
                    res1 = pipeline.ncm_decode("r1", [ncm_file], {ncm_file}, run_root, out_dir)
                    mock_decode.assert_called_once()
                    self.assertIn(ncm_file, res1)
                    cached_path = res1[ncm_file]
                    self.assertTrue(cached_path.is_file())
                    self.assertTrue("ncm" in str(cached_path))

                with patch.object(pipeline, "decode_to_directory") as mock_decode2:
                    res2 = pipeline.ncm_decode("r1", [ncm_file], {ncm_file}, run_root, out_dir)
                    mock_decode2.assert_not_called()
                    self.assertEqual(res2[ncm_file], cached_path)

    def test_run_full_skips_published_incremental(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d) / "state"
            src_dir = Path(d) / "source"
            out_dir = Path(d) / "output"
            state_dir.mkdir()
            src_dir.mkdir()
            out_dir.mkdir()

            s1 = src_dir / "already_done.mp3"
            s2 = src_dir / "need_process.mp3"
            s1.write_bytes(b"song1" * 100)
            s2.write_bytes(b"song2" * 100)

            pub_target = out_dir / "already_done.mp3"
            pub_target.write_bytes(b"song1" * 100)

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"), \
                 patch.object(pipeline, "validate_settings", return_value=(src_dir, out_dir)), \
                 patch.object(pipeline, "sample_is_verified", return_value=True):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('prev','full',?,?, 'done')", (str(src_dir), str(out_dir)))
                    conn.execute(
                        "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,metadata_state,lyrics_state,rules_version) VALUES(?,?,?,?,'published',?,'existing_tags','lyrics_already_present',?)",
                        (str(s1), s1.stat().st_size, s1.stat().st_mtime_ns, "h1", str(pub_target), pipeline.RULES_VERSION)
                    )
                    conn.commit()
                finally:
                    conn.close()

                changed = pipeline.changed_sources(src_dir, out_dir)
                self.assertEqual(changed, [s2])

                with patch.object(pipeline, "run_batch") as mock_batch:
                    settings = {"source_dir": str(src_dir), "output_dir": str(out_dir), "sample_verified": {"run_id": "prev"}}
                    pipeline.run_full(settings, incremental=True)
                    mock_batch.assert_called_once()
                    called_paths = mock_batch.call_args[0][3]
                    self.assertEqual(called_paths, [s2])
                    self.assertNotIn(s1, called_paths)


class AppTests(unittest.TestCase):
    def test_config_keeps_only_two_directories(self) -> None:
        with patch.object(app, "authorized", return_value=True), patch.object(app.Path, "is_dir", return_value=True), patch.object(app.os, "access", return_value=True):
            self.assertTrue(app.config_valid({"source_dir": "/vol1/source", "output_dir": "/vol1/output"})[0])

    def test_config_rejects_alias_and_nested_paths(self) -> None:
        self.assertFalse(app.config_valid({"source_dir": "/nas/source", "output_dir": "/nas/output"})[0])
        with patch.object(app, "authorized", return_value=True), patch.object(app.Path, "is_dir", return_value=True), patch.object(app.os, "access", return_value=True):
            self.assertFalse(app.config_valid({"source_dir": "/vol1/source", "output_dir": "/vol1/source/output"})[0])

    def test_dashboard_explains_bounded_state_and_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(app, "STATE", Path(directory)), patch.object(app, "CONFIG", Path(directory) / "config.json"), patch.object(app, "STATUS", Path(directory) / "status.json"):
            app.write_json(app.CONFIG, {"source_dir": "/vol1/source", "output_dir": "/vol1/output"})
            app.write_json(app.STATUS, {"state": "idle", "message": "ready"})
            page = app.render_dashboard()
            self.assertIn("512 MiB", page)
            self.assertIn("结束即清理", page)

    def test_app_report_live_metrics_during_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(app, "STATE", Path(directory)):
            conn = sqlite3.connect(Path(directory) / "ledger-v6.sqlite")
            try:
                conn.execute("CREATE TABLE runs (id TEXT PRIMARY KEY, mode TEXT, source_dir TEXT, output_dir TEXT, status TEXT, phase TEXT, phase_done INTEGER, phase_total INTEGER, started_at TEXT, finished_at TEXT, summary_json TEXT);")
                conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, run_id TEXT, source_path TEXT, source_size INTEGER, source_mtime_ns INTEGER, source_sha256 TEXT, audio_sha256 TEXT, duration REAL, source_kind TEXT, disposition TEXT, output_path TEXT, metadata_state TEXT, lyrics_state TEXT, cover_state TEXT, error TEXT);")
                conn.execute("CREATE TABLE groups (id TEXT PRIMARY KEY, run_id TEXT, tool TEXT, similarity REAL, winner_source_path TEXT, decision TEXT, paths_json TEXT);")
                conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status,phase,phase_done,phase_total,summary_json) VALUES('live-run','full','/vol1/s','/vol1/o','running','publish',80,1781,'{}');")
                conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,source_kind,disposition,lyrics_state,cover_state) VALUES('live-run','a.mp3',1,1,'audio','published','lyrics_embedded','cover_embedded');")
                conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,source_kind,disposition,lyrics_state,cover_state) VALUES('live-run','b.mp3',1,1,'audio','duplicate_exact','pending','pending');")
                conn.commit()
            finally:
                conn.close()

            rep = app.report()
            self.assertEqual(rep["run_id"], "live-run")
            self.assertEqual(rep["status"], "running")
            m = rep["metrics"]
            self.assertEqual(m["scanned_total"], 2)
            self.assertEqual(m["published"], 1)
            self.assertEqual(m["exact_duplicate"], 1)
            self.assertEqual(m["lyrics_embedded"], 1)
            self.assertEqual(m["cover_embedded"], 1)

    def test_full_mode_allowed_even_when_initialized(self) -> None:
        cfg = {"source_dir": "/vol1/source", "output_dir": "/vol1/output", "initialized": True, "sample_verified": {"run_id": "s1"}}
        with patch.object(app, "read_json", return_value=cfg), \
             patch.object(app, "config_valid", return_value=(True, "")), \
             patch.object(app, "sample_ready", return_value=True), \
             patch.object(app, "take_lock", return_value=True), \
             patch.object(app.threading, "Thread"):
            code, _ = app.start_mode("full")
            self.assertEqual(code, 202)

    def test_reset_system_clears_history_and_config(self) -> None:
        with tempfile.TemporaryDirectory() as d, patch.object(app, "STATE", Path(d)), patch.object(app, "CONFIG", Path(d) / "config.json"), patch.object(app, "STATUS", Path(d) / "status.json"), patch.object(app, "LOG", Path(d) / "last-run.log"):
            conn = sqlite3.connect(Path(d) / "ledger-v6.sqlite")
            try:
                conn.execute("CREATE TABLE runs (id TEXT);")
                conn.execute("CREATE TABLE items (id INT);")
                conn.execute("CREATE TABLE groups (id TEXT);")
                conn.execute("CREATE TABLE events (id INT);")
                conn.execute("CREATE TABLE source_inventory (source_path TEXT);")
                conn.execute("CREATE TABLE knowledge_base (audio_sha256 TEXT);")
                conn.execute("INSERT INTO runs VALUES('r1');")
                conn.execute("INSERT INTO items VALUES(1);")
                conn.execute("INSERT INTO source_inventory VALUES('/vol1/a.flac');")
                conn.execute("INSERT INTO knowledge_base VALUES('sha1');")
                conn.commit()
            finally:
                conn.close()

            app.write_json(app.CONFIG, {"source_dir": "/s", "output_dir": "/o", "sample_verified": {"run_id": "s"}, "initialized": True})
            app.write_json(app.STATUS, {"state": "idle", "message": "done"})

            res = app.reset_system(clear_history=True, clear_inventory=True, clear_kb=True, clear_ncm=False)
            self.assertTrue(res["success"])

            conn = sqlite3.connect(Path(d) / "ledger-v6.sqlite")
            try:
                self.assertEqual(conn.execute("SELECT count(*) FROM runs").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT count(*) FROM items").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT count(*) FROM source_inventory").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT count(*) FROM knowledge_base").fetchone()[0], 0)
            finally:
                conn.close()

            cfg = app.read_json(app.CONFIG, {})
            self.assertNotIn("sample_verified", cfg)
            self.assertNotIn("initialized", cfg)


    def test_artist_and_title_clustering(self) -> None:
        p1 = Path("王力宏 - 花田错 (1).flac")
        p2 = Path("花田错.mp3")
        art1, title1 = pipeline.extract_artist_and_title(p1.stem)
        art2, title2 = pipeline.extract_artist_and_title(p2.stem)
        self.assertEqual(title1, "花田错")
        self.assertEqual(title2, "花田错")
        self.assertTrue(pipeline.artists_compatible(art1, art2))

    def test_artist_and_title_extraction_edge_cases(self) -> None:
        self.assertEqual(pipeline.extract_artist_and_title("周杰倫-晴天"), ("周杰倫", "晴天"))
        self.assertEqual(pipeline.extract_artist_and_title("陶喆 – 流沙"), ("陶喆", "流沙"))
        self.assertEqual(pipeline.extract_artist_and_title("01. 一直很安静"), ("", "一直很安静"))
        self.assertEqual(pipeline.extract_artist_and_title("老男孩"), ("", "老男孩"))
        # An unspaced dash in a Latin title is part of the name, not a separator.
        self.assertEqual(pipeline.extract_artist_and_title("Love-You"), ("", "love-you"))
        self.assertEqual(pipeline.extract_artist_and_title("宋冬野 - 董小姐 (Live)"), ("宋冬野", "董小姐 (live)"))

    def test_traditional_and_simplified_names_compare_equal(self) -> None:
        if pipeline.domestic_provider is None:
            self.skipTest("domestic_provider 不可导入")
        self.assertTrue(pipeline.artists_compatible("周杰倫", "周杰伦"))
        self.assertEqual(pipeline.simplify_chinese("東風破"), "东风破")
        self.assertEqual(pipeline.simplify_chinese(""), "")

    def test_traditional_and_simplified_copies_are_deduped(self) -> None:
        """A 東風破 (traditional) and 东风破 (simplified) copy are the same recording."""
        if pipeline.domestic_provider is None:
            self.skipTest("domestic_provider 不可导入")
        with tempfile.TemporaryDirectory() as d:
            state_dir, src_dir = Path(d) / "state", Path(d) / "source"
            state_dir.mkdir()
            src_dir.mkdir()
            traditional = src_dir / "周杰倫 - 東風破.flac"
            simplified = src_dir / "周杰伦 - 东风破.mp3"
            traditional.write_bytes(b"trad" * 200)
            simplified.write_bytes(b"simp" * 100)

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('rT','sample','/s','/o','running')")
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('rT',?,1000,1,240.0,'audio','candidate')", (str(traditional),))
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('rT',?,500,1,240.0,'audio','candidate')", (str(simplified),))
                    conn.commit()
                finally:
                    conn.close()

                remaining = pipeline.acoustic_dedupe("rT", src_dir, {traditional, simplified}, library=None)
                self.assertEqual(remaining, {traditional})

    def test_tag_readability_detection(self) -> None:
        self.assertTrue(pipeline.tag_is_unreadable(None))
        self.assertTrue(pipeline.tag_is_unreadable(""))
        self.assertTrue(pipeline.tag_is_unreadable("?????"))
        self.assertTrue(pipeline.tag_is_unreadable(" �� "))
        self.assertFalse(pipeline.tag_is_unreadable("一直很安静"))
        self.assertFalse(pipeline.tag_is_unreadable("What's Up?"))

    def test_strip_riff_chunk_removes_only_the_target_chunk(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            wav = Path(d) / "song.wav"
            fmt_data, audio, info, id3 = b"\x10" * 16, b"audio-bytes", b"INFOabcdef", b"ID3-payload-here"
            wav.write_bytes(riff_file((b"fmt ", fmt_data), (b"data", audio), (b"LIST", info), (b"id3 ", id3)))
            self.assertTrue(pipeline.strip_riff_chunk(wav, b"LIST"))
            payload = wav.read_bytes()
            self.assertEqual(riff_chunk_ids(payload), [b"fmt ", b"data", b"id3 "])
            self.assertEqual(payload[20:20 + len(fmt_data)], fmt_data)
            self.assertIn(b"audio-bytes", payload)
            self.assertIn(b"ID3-payload-here", payload)
            # The RIFF size field must be rewritten to match the shrunken payload.
            self.assertEqual(struct.unpack("<I", payload[4:8])[0], len(payload) - 8)
            # A second pass has nothing left to remove.
            self.assertFalse(pipeline.strip_riff_chunk(wav, b"LIST"))

    def test_repair_shadowed_wav_tags_drops_the_stale_info_chunk(self) -> None:
        """RIFF INFO 用非 UTF-8 编码遮蔽 ID3，ffmpeg/飞牛就只看到 ????? 而认不出标签。"""
        class FakeMedia:
            title = "一直很安静"
            artist = "阿桑"

        fake_mediafile = types.SimpleNamespace(MediaFile=lambda _path: FakeMedia())
        with tempfile.TemporaryDirectory() as d:
            wav = Path(d) / "song.wav"
            wav.write_bytes(riff_file((b"fmt ", b"\x10" * 16), (b"data", b"audio"), (b"LIST", b"INFO?????"), (b"id3 ", b"ID3")))
            with patch.object(pipeline, "ffprobe_media", return_value=(1.0, 12, {"title": "?????", "artist": "??"}, False)), \
                 patch.object(pipeline, "mediafile", fake_mediafile):
                self.assertTrue(pipeline.repair_shadowed_wav_tags(wav))
            self.assertEqual(riff_chunk_ids(wav.read_bytes()), [b"fmt ", b"data", b"id3 "])

            # Healthy visible tags are left alone.
            clean = Path(d) / "clean.wav"
            clean.write_bytes(riff_file((b"fmt ", b"\x10" * 16), (b"LIST", b"INFOok"), (b"id3 ", b"ID3")))
            with patch.object(pipeline, "ffprobe_media", return_value=(1.0, 12, {"title": "晴天", "artist": "周杰伦"}, False)), \
                 patch.object(pipeline, "mediafile", fake_mediafile):
                self.assertFalse(pipeline.repair_shadowed_wav_tags(clean))
            self.assertIn(b"LIST", clean.read_bytes())

            # Nothing to fall back on: leave the file untouched.
            class EmptyMedia:
                title = None
                artist = None

            blank = Path(d) / "blank.wav"
            blank.write_bytes(riff_file((b"fmt ", b"\x10" * 16), (b"LIST", b"INFO?????")))
            with patch.object(pipeline, "ffprobe_media", return_value=(1.0, 12, {}, False)), \
                 patch.object(pipeline, "mediafile", types.SimpleNamespace(MediaFile=lambda _path: EmptyMedia())):
                self.assertFalse(pipeline.repair_shadowed_wav_tags(blank))

    def test_probe_falls_back_to_embedded_tags(self) -> None:
        """ffprobe 只看得见 RIFF INFO，mutagen 才读得到我们写入的 ID3。"""
        class FakeMedia:
            title = "一直很安静"
            artist = "阿桑"
            album = "寂寞在唱歌"
            albumartist = None
            track = 3
            year = 2004
            lyrics = "[00:01.00]歌词"

        fake_mediafile = types.SimpleNamespace(MediaFile=lambda _path: FakeMedia())
        with tempfile.TemporaryDirectory() as d:
            wav = Path(d) / "song.wav"
            wav.write_bytes(b"x")
            with patch.object(pipeline, "ffprobe_media", return_value=(200.0, 1, {"title": "?????", "artist": "??"}, False)), \
                 patch.object(pipeline, "mediafile", fake_mediafile):
                _, _, tags, _ = pipeline.probe(wav)
            self.assertEqual(tags["title"], "一直很安静")
            self.assertEqual(tags["artist"], "阿桑")
            self.assertEqual(tags["album"], "寂寞在唱歌")
            self.assertEqual(tags["track"], "3")
            self.assertEqual(tags["lyrics"], "[00:01.00]歌词")

    def test_hidden_archive_is_excluded_from_library_scan(self) -> None:
        out_dir = Path("/vol2/out")
        self.assertFalse(pipeline.hidden_under(out_dir / "A" / "Song.mp3", out_dir))
        self.assertFalse(pipeline.hidden_under(Path("/elsewhere/Song.mp3"), out_dir))

    def test_duplicate_songs_with_small_duration_drift_are_merged(self) -> None:
        """Album-vs-single rips differ by a couple of seconds of silence (陶喆《爱很简单》)."""
        with tempfile.TemporaryDirectory() as d:
            state_dir, src_dir = Path(d) / "state", Path(d) / "source"
            state_dir.mkdir()
            src_dir.mkdir()
            album = src_dir / "陶喆 - 爱很简单.flac"
            single = src_dir / "陶喆 - 爱很简单 (Radio Edit).mp3"
            album.write_bytes(b"album" * 200)
            single.write_bytes(b"single" * 100)

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"), patch.object(pipeline, "SAFE_DURATION", 4.5):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('r8','sample','/s','/o','running')")
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('r8',?,1000,1,269.56,'audio','candidate')", (str(album),))
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('r8',?,500,1,271.61,'audio','candidate')", (str(single),))
                    conn.commit()
                finally:
                    conn.close()

                remaining = pipeline.acoustic_dedupe("r8", src_dir, {album, single}, library=None)
                self.assertEqual(remaining, {album})

    def test_quality_upgrade_archives_lower_quality_library_copy(self) -> None:
        """A new lossless arrival must archive, not silently duplicate, the old lossy file."""
        with tempfile.TemporaryDirectory() as d:
            state_dir, src_dir, out_dir = Path(d) / "state", Path(d) / "source", Path(d) / "output"
            state_dir.mkdir()
            src_dir.mkdir()
            out_dir.mkdir()
            library_mp3 = out_dir / "陶喆" / "黑色柳丁" / "01 - 爱很简单.mp3"
            library_mp3.parent.mkdir(parents=True)
            library_mp3.write_bytes(b"mp3" * 100)
            flac = src_dir / "陶喆 - 爱很简单.flac"
            flac.write_bytes(b"flac" * 200)

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('r9','incremental','/s','/o','running')")
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('r9',?,1000,1,269.56,'audio','candidate')", (str(flac),))
                    conn.execute(
                        "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,duration) VALUES(?,?,?,?,'published',?,?)",
                        (str(Path(d) / "old-source" / "01 - 爱很简单.mp3"), 300, 1, "oldhash", str(library_mp3), 269.56),
                    )
                    conn.commit()
                finally:
                    conn.close()

                remaining = pipeline.acoustic_dedupe("r9", src_dir, {flac}, library=out_dir)
                self.assertEqual(remaining, {flac})
                archived = out_dir / ".music-archive" / "陶喆" / "黑色柳丁" / "01 - 爱很简单.mp3"
                self.assertFalse(library_mp3.exists())
                self.assertTrue(archived.is_file())

                conn = pipeline.db()
                try:
                    row = conn.execute("SELECT disposition,output_path FROM source_inventory WHERE size_bytes=300").fetchone()
                    self.assertEqual(row["disposition"], "superseded")
                    self.assertEqual(row["output_path"], str(archived))
                    decision = conn.execute("SELECT decision FROM groups WHERE run_id='r9' AND tool='quality_upgrade'").fetchone()
                    self.assertEqual(decision["decision"], "quality_upgrade")
                finally:
                    conn.close()
                self.assertEqual(pipeline.report("r9")["metrics"]["quality_upgraded"], 1)

    def test_two_quality_upgrades_in_one_run(self) -> None:
        """Each upgrade writes to the ledger; a second writer would deadlock on SQLite."""
        with tempfile.TemporaryDirectory() as d:
            state_dir, src_dir, out_dir = Path(d) / "state", Path(d) / "source", Path(d) / "output"
            state_dir.mkdir()
            src_dir.mkdir()
            out_dir.mkdir()
            candidates: set[Path] = set()
            rows = []
            for index, title in enumerate(("爱很简单", "流沙")):
                library_mp3 = out_dir / "陶喆" / "专辑" / f"{title}.mp3"
                library_mp3.parent.mkdir(parents=True, exist_ok=True)
                library_mp3.write_bytes(b"mp3" * 100)
                flac = src_dir / f"陶喆 - {title}.flac"
                flac.write_bytes(b"flac" * 200)
                candidates.add(flac)
                rows.append((flac, library_mp3, 269.5 + index))

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('rB','incremental','/s','/o','running')")
                    for flac, library_mp3, duration in rows:
                        conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('rB',?,1000,1,?,'audio','candidate')", (str(flac), duration))
                        conn.execute(
                            "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,duration) VALUES(?,?,?,?,'published',?,?)",
                            (str(Path(d) / "old" / library_mp3.name), 300, 1, f"hash-{library_mp3.name}", str(library_mp3), duration),
                        )
                    conn.commit()
                finally:
                    conn.close()

                remaining = pipeline.acoustic_dedupe("rB", src_dir, set(candidates), library=out_dir)
                self.assertEqual(remaining, candidates)
                for _flac, library_mp3, _duration in rows:
                    self.assertFalse(library_mp3.exists())
                    self.assertTrue((out_dir / ".music-archive" / "陶喆" / "专辑" / library_mp3.name).is_file())
                self.assertEqual(pipeline.report("rB")["metrics"]["quality_upgraded"], 2)

    def test_quality_upgrade_keeps_better_existing_copy(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            state_dir, src_dir, out_dir = Path(d) / "state", Path(d) / "source", Path(d) / "output"
            state_dir.mkdir()
            src_dir.mkdir()
            out_dir.mkdir()
            library_flac = out_dir / "Singer" / "Album" / "Song.flac"
            library_flac.parent.mkdir(parents=True)
            library_flac.write_bytes(b"flac" * 400)
            mp3 = src_dir / "Song.mp3"
            mp3.write_bytes(b"mp3" * 10)

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('rA','incremental','/s','/o','running')")
                    conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('rA',?,100,1,200.0,'audio','candidate')", (str(mp3),))
                    conn.execute(
                        "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,duration) VALUES(?,?,?,?,'published',?,?)",
                        (str(Path(d) / "old-source" / "Song.flac"), 1600, 1, "flachash", str(library_flac), 200.0),
                    )
                    conn.commit()
                finally:
                    conn.close()

                remaining = pipeline.acoustic_dedupe("rA", src_dir, {mp3}, library=out_dir)
                self.assertEqual(remaining, set())
                self.assertTrue(library_flac.is_file())
                self.assertFalse((out_dir / ".music-archive").exists())
                self.assertEqual(pipeline.report("rA")["metrics"]["quality_upgraded"], 0)

    def test_destination_no_hash_suffix(self) -> None:
        with patch.object(pipeline, "probe", return_value=(200.0, 1000, {"artist": "Artist", "album": "Album", "title": "Song", "track": "1"}, True)):
            res = pipeline.destination(Path("/out"), Path("source.flac"), "dummy_digest")
            self.assertEqual(res, Path("/out/Artist/Album/1 - Song.flac"))

    def test_knowledge_base_put_and_get(self) -> None:
        with tempfile.TemporaryDirectory() as d, patch.object(pipeline, "STATE", Path(d)), patch.object(pipeline, "LEDGER", Path(d) / "ledger-v6.sqlite"):
            with pipeline.db() as conn:
                pipeline.kb_put(conn, "sha123", "Jay Chou", "Fantasy", "Simple Love", "02", 2001, "[00:00.00]Lyrics", True)
                hit = pipeline.kb_get(conn, "sha123")
                self.assertIsNotNone(hit)
                self.assertEqual(hit["artist"], "Jay Chou")
                self.assertEqual(hit["album"], "Fantasy")
                self.assertEqual(hit["title"], "Simple Love")
                self.assertEqual(hit["lyrics"], "[00:00.00]Lyrics")
                self.assertEqual(hit["has_cover"], 1)

                hit_by_art_tit = pipeline.kb_get(conn, "unknown_sha", "Jay Chou", "Simple Love")
                self.assertIsNotNone(hit_by_art_tit)
                self.assertEqual(hit_by_art_tit["lyrics"], "[00:00.00]Lyrics")

    def test_clean_track_number(self) -> None:
        self.assertEqual(pipeline.clean_track_number("1/10"), "1")
        self.assertEqual(pipeline.clean_track_number("01"), "01")
        self.assertEqual(pipeline.clean_track_number("12/12"), "12")
        self.assertEqual(pipeline.clean_track_number(""), "")

    def test_run_full_defaults_to_all_paths(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d) / "state"
            src_dir = Path(d) / "source"
            out_dir = Path(d) / "output"
            state_dir.mkdir()
            src_dir.mkdir()
            out_dir.mkdir()

            s1 = src_dir / "done.mp3"
            s2 = src_dir / "new.mp3"
            s1.write_bytes(b"1" * 100)
            s2.write_bytes(b"2" * 100)

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"), \
                 patch.object(pipeline, "validate_settings", return_value=(src_dir, out_dir)), \
                 patch.object(pipeline, "sample_is_verified", return_value=True):
                with patch.object(pipeline, "run_batch") as mock_batch:
                    settings = {"source_dir": str(src_dir), "output_dir": str(out_dir), "sample_verified": {"run_id": "s"}}
                    pipeline.run_full(settings)
                    mock_batch.assert_called_once()
                    called_paths = mock_batch.call_args[0][3]
                    self.assertIn(s1, called_paths)
                    self.assertIn(s2, called_paths)

    def test_clean_smb_trailing_dots_and_spaces(self) -> None:
        self.assertEqual(pipeline.clean("Song. ", "default"), "Song")
        self.assertEqual(pipeline.clean("Artist... ", "default"), "Artist")
        self.assertEqual(pipeline.clean("Normal Song", "default"), "Normal Song")
        self.assertEqual(pipeline.clean("   ...   ", "default"), "default")

    def test_destination_multi_disc(self) -> None:
        fake_audio = Path("/fake/audio.flac")
        with patch.object(pipeline, "probe", return_value=(200.0, 1000, {"artist": "Artist", "album": "Double Album", "title": "Track 1", "disc": "2", "track": "01"}, True)):
            dest = pipeline.destination(Path("/out"), fake_audio, "hash1")
            self.assertEqual(str(dest).replace("\\", "/"), "/out/Artist/Double Album/CD2/01 - Track 1.flac")

        with patch.object(pipeline, "probe", return_value=(200.0, 1000, {"artist": "Artist", "album": "Single Album", "title": "Track 1", "disc": "1", "track": "01"}, True)):
            dest = pipeline.destination(Path("/out"), fake_audio, "hash1")
            self.assertEqual(str(dest).replace("\\", "/"), "/out/Artist/Single Album/01 - Track 1.flac")

    def test_validate_settings_allows_proxy(self) -> None:
        cfg = {"source_dir": "/vol2/src", "output_dir": "/vol2/out", "proxy": "http://127.0.0.1:7890"}
        with patch.object(Path, "is_dir", return_value=True), patch.object(os, "access", return_value=True):
            s, o = pipeline.validate_settings(cfg)
            self.assertEqual(os.environ.get("HTTP_PROXY"), "http://127.0.0.1:7890")
            self.assertEqual(os.environ.get("HTTPS_PROXY"), "http://127.0.0.1:7890")

    def test_validate_settings_allows_offline_and_admin_password(self) -> None:
        cfg = {
            "source_dir": "/vol2/src",
            "output_dir": "/vol2/out",
            "offline_mode": False,
            "admin_password": "secret",
            "sample_verified": {"run_id": "r1"},
            "initialized": True,
        }
        with patch.object(Path, "is_dir", return_value=True), patch.object(os, "access", return_value=True):
            s, o = pipeline.validate_settings(cfg)
            self.assertEqual(str(s).replace("\\", "/"), "/vol2/src")
            self.assertEqual(str(o).replace("\\", "/"), "/vol2/out")

    def test_scan_dupsonic_chunking(self) -> None:
        many_paths = [Path(f"/fake/song_{i}.flac") for i in range(350)]
        with patch.object(pipeline, "command") as mock_cmd, patch.object(pipeline, "assert_state_budget"):
            pipeline.scan_dupsonic(many_paths)
            # 350 paths with chunk_size 150 -> 3 calls (150, 150, 50)
            self.assertEqual(mock_cmd.call_count, 3)

    def test_sqlite_wal_mode(self) -> None:
        with tempfile.TemporaryDirectory() as d, patch.object(pipeline, "STATE", Path(d)), patch.object(pipeline, "LEDGER", Path(d) / "ledger-v6.sqlite"):
            with pipeline.db() as conn:
                mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
                self.assertEqual(mode.lower(), "wal")

    def test_clean_windows_reserved_device_names(self) -> None:
        self.assertEqual(pipeline.clean("CON", "fallback"), "CON_")
        self.assertEqual(pipeline.clean("AUX", "fallback"), "AUX_")
        self.assertEqual(pipeline.clean("prn.flac", "fallback"), "prn_.flac")
        self.assertEqual(pipeline.clean("com1.mp3", "fallback"), "com1_.mp3")
        self.assertEqual(pipeline.clean("NUL", "fallback"), "NUL_")
        self.assertEqual(pipeline.clean("Normal Album", "fallback"), "Normal Album")

    def test_domestic_artist_matching_and_variants(self) -> None:
        try:
            import domestic_provider
        except ImportError:
            domestic_provider = load("domestic_provider", "domestic_provider.py")
        self.assertFalse(domestic_provider.artist_matches(["Lucky小爱"], "周杰伦"))
        self.assertTrue(domestic_provider.artist_matches(["周杰伦"], "周杰伦"))
        self.assertTrue(domestic_provider.artist_matches(["周杰伦", "费玉清"], "周杰伦"))
        self.assertTrue(domestic_provider.is_unwanted_variant("晴天(深情版)", "晴天"))
        self.assertTrue(domestic_provider.is_unwanted_variant("晴天 钢琴版", "晴天"))
        self.assertTrue(domestic_provider.is_unwanted_variant("晴天 (伴奏)", "晴天"))
        self.assertTrue(domestic_provider.is_unwanted_variant("Hey Jude (Instrumental)", "Hey Jude"))
        self.assertTrue(domestic_provider.is_unwanted_variant("Hey Jude (Cover)", "Hey Jude"))
        self.assertTrue(domestic_provider.is_unwanted_variant("Hey Jude [Inst]", "Hey Jude"))
        self.assertTrue(domestic_provider.is_unwanted_variant("Hey Jude", "Hey Jude", album_name="Bossa Beatles Instrumental"))
        self.assertTrue(domestic_provider.is_unwanted_variant("晴天", "晴天", album_name="周杰伦 伴奏专辑"))
        self.assertFalse(domestic_provider.is_unwanted_variant("晴天", "晴天"))
        self.assertFalse(domestic_provider.is_unwanted_variant("Hey Jude", "Hey Jude"))
        # Artist cover filtering
        self.assertFalse(domestic_provider.artist_matches(["周杰伦 (Cover: 小明)"], "周杰伦"))
        self.assertFalse(domestic_provider.artist_matches(["小明 (翻唱周杰伦)"], "周杰伦"))

    def test_instrumental_and_original_not_deduped_together(self) -> None:
        """原唱与伴奏（如晴天 与 晴天(伴奏)）即使时长相仿，也绝不能被当成同录音重复判定。"""
        p1 = Path("/fake/周杰伦 - 晴天.flac")
        p2 = Path("/fake/周杰伦 - 晴天 (伴奏).mp3")
        self.assertTrue(pipeline.has_incompatible_variants([p1, p2]))
        p3 = Path("/fake/Hey Jude.flac")
        p4 = Path("/fake/Hey Jude (Instrumental).mp3")
        self.assertTrue(pipeline.has_incompatible_variants([p3, p4]))
        p5 = Path("/fake/Hey Jude (Cover).mp3")
        self.assertTrue(pipeline.has_incompatible_variants([p3, p5]))
        p_clean1 = Path("/fake/Hey Jude (Radio Edit).mp3")
        p_clean2 = Path("/fake/Hey Jude (Radio-Edit).flac")
        self.assertFalse(pipeline.has_incompatible_variants([p_clean1, p_clean2]))

    def test_confidence_scoring_distinguishes_covers_and_instrumentals(self) -> None:
        """测试置信度引擎对正歌、翻唱、伴奏及不同时长的打分与门禁。"""
        try:
            import domestic_provider
        except ImportError:
            domestic_provider = load("domestic_provider", "domestic_provider.py")

        # 1. 真实正歌高置信度匹配（时长吻合，歌名歌手一致） -> 得分 >= 75
        score, _ = domestic_provider.compute_match_confidence(
            target_title="晴天",
            target_artist="周杰伦",
            target_duration=269.0,
            cand_title="晴天",
            cand_artists=["周杰伦"],
            cand_album="叶惠美",
            cand_duration=269.5,
        )
        self.assertGreaterEqual(score, 75.0)

        # 2. 伴奏或纯音乐（即便歌手相同，也因命中变体词重罚不及格） -> 得分 < 75
        score, _ = domestic_provider.compute_match_confidence(
            target_title="晴天",
            target_artist="周杰伦",
            target_duration=269.0,
            cand_title="晴天 (伴奏)",
            cand_artists=["周杰伦"],
            cand_album="叶惠美",
            cand_duration=269.0,
        )
        self.assertLess(score, 75.0)

        # 3. 翻唱（歌手不同，且时长有偏差） -> 得分严重不及格
        score, _ = domestic_provider.compute_match_confidence(
            target_title="晴天",
            target_artist="周杰伦",
            target_duration=269.0,
            cand_title="晴天",
            cand_artists=["张三 (Cover: 周杰伦)"],
            cand_album="网络翻唱合辑",
            cand_duration=210.0,
        )
        self.assertLess(score, 50.0)

        # 4. 时长严重不符（原唱 269s，候选短视频片段 60s） -> 一票否决
        score, _ = domestic_provider.compute_match_confidence(
            target_title="晴天",
            target_artist="周杰伦",
            target_duration=269.0,
            cand_title="晴天",
            cand_artists=["周杰伦"],
            cand_album="短视频截取",
            cand_duration=60.0,
        )
        self.assertLess(score, 50.0)

    def test_traditional_chinese_is_normalised_for_domestic_search(self) -> None:
        """国内源按简体收录，繁体查询（周杰倫/東風破）必须归一到简体。"""
        try:
            import domestic_provider
        except ImportError:
            domestic_provider = load("domestic_provider", "domestic_provider.py")
        self.assertEqual(domestic_provider.to_simplified("周杰倫 東風破"), "周杰伦 东风破")
        self.assertEqual(domestic_provider.to_simplified("鄧麗君 - 月亮代表我的心"), "邓丽君 - 月亮代表我的心")
        self.assertEqual(domestic_provider.to_simplified("張學友 吻別"), "张学友 吻别")
        self.assertEqual(domestic_provider.to_simplified("愛很簡單"), "爱很简单")
        self.assertEqual(domestic_provider.to_simplified(""), "")
        # Artist matching must survive the traditional form of the same name.
        self.assertTrue(domestic_provider.artist_matches(["周杰倫"], "周杰伦"))

    def test_qq_smartbox_detail_fields_are_normalised(self) -> None:
        """fcg_play_single_song 用新字段名(mid/name/album.mid)作答，下游读的却是
        songmid/songname/albummid —— 不映射就等于没有歌词、没有封面。"""
        try:
            import domestic_provider
        except ImportError:
            domestic_provider = load("domestic_provider", "domestic_provider.py")
        smartbox = {"code": 0, "data": {"song": {"itemlist": [{"mid": "003uEbEr0jcW7c", "name": "东风破", "singer": "周杰伦"}]}}}
        detail = {"code": 0, "data": [{
            "mid": "003uEbEr0jcW7c", "name": "东风破", "title": "东风破", "interval": 315,
            "album": {"mid": "000MkMni19ClKG", "name": "叶惠美"},
            "singer": [{"name": "周杰伦"}],
        }]}
        with patch.object(domestic_provider, "_http_get_json", side_effect=[smartbox, detail]):
            song = domestic_provider.search_qq_music("东风破", "周杰伦")
        self.assertEqual(song["songmid"], "003uEbEr0jcW7c")
        self.assertEqual(song["songname"], "东风破")
        self.assertEqual(song["albummid"], "000MkMni19ClKG")
        self.assertEqual(song["albumname"], "叶惠美")
        self.assertEqual([s["name"] for s in song["singer"]], ["周杰伦"])

        lyric_payload = {"lyric": base64.b64encode("[00:01.00]一盏离愁".encode("utf-8")).decode("ascii")}
        with patch.object(domestic_provider, "_http_get_json", side_effect=[lyric_payload]):
            cover, lyric = domestic_provider.get_qq_details(song)
        self.assertEqual(cover, "https://y.gtimg.cn/music/photo_new/T002R800x800M000000MkMni19ClKG.jpg")
        self.assertEqual(lyric, "[00:01.00]一盏离愁")

    def test_qq_smartbox_survives_a_failed_detail_lookup(self) -> None:
        try:
            import domestic_provider
        except ImportError:
            domestic_provider = load("domestic_provider", "domestic_provider.py")
        smartbox = {"code": 0, "data": {"song": {"itemlist": [{"mid": "003uEbEr0jcW7c", "name": "东风破", "singer": "周杰伦"}]}}}
        with patch.object(domestic_provider, "_http_get_json", side_effect=[smartbox, None]):
            song = domestic_provider.search_qq_music("东风破", "周杰伦")
        self.assertEqual(song["songmid"], "003uEbEr0jcW7c")
        self.assertEqual(song["songname"], "东风破")
        self.assertEqual(song["albummid"], "")
        self.assertEqual(song["songname"], "东风破")

    def test_title_match_preference_without_artist(self) -> None:
        """无歌手可过滤时，歌名对得上的候选要胜过排在第一位的无关结果。"""
        try:
            import domestic_provider
        except ImportError:
            domestic_provider = load("domestic_provider", "domestic_provider.py")
        self.assertTrue(domestic_provider.title_matches("一直很安静", "一直很安静"))
        self.assertTrue(domestic_provider.title_matches("一直很安静 (Live)", "一直很安静"))
        self.assertFalse(domestic_provider.title_matches("Warrior", "一直很安静"))

        self.assertEqual(domestic_provider._prefer_title_match({"name": "Warrior"}, {"name": "一直很安静"}, "一直很安静")["name"], "一直很安静")
        self.assertEqual(domestic_provider._prefer_title_match({"name": "一直很安静"}, {"name": "一直很安静 (Live)"}, "一直很安静")["name"], "一直很安静")

        ranked = {"result": {"songs": [
            {"id": 1, "name": "Warrior", "artists": [{"name": "Artemiss"}]},
            {"id": 2, "name": "一直很安静", "artists": [{"name": "阿桑"}]},
        ]}}
        with patch.object(domestic_provider, "_http_get_json", return_value=ranked):
            self.assertEqual(domestic_provider.search_netease("一直很安静")["id"], 2)
        # No title match anywhere: keep the old behaviour and take the top hit.
        other = {"result": {"songs": [{"id": 7, "name": "Something Else", "artists": []}]}}
        with patch.object(domestic_provider, "_http_get_json", return_value=other):
            self.assertEqual(domestic_provider.search_netease("一直很安静")["id"], 7)

        # A known artist still wins over the title heuristic.
        with patch.object(domestic_provider, "_http_get_json", return_value=ranked):
            self.assertEqual(domestic_provider.search_netease("一直很安静", "Artemiss")["id"], 1)

    def test_proxy_setting_preserved_in_api_config(self) -> None:
        with tempfile.TemporaryDirectory() as d, patch.object(app, "STATE", Path(d)), patch.object(app, "CONFIG", Path(d) / "config.json"), patch.object(app, "STATUS", Path(d) / "status.json"):
            cfg = {"source_dir": "/vol2/src", "output_dir": "/vol2/out", "proxy": "http://192.168.100.1:7890"}
            with patch.object(app, "config_valid", return_value=(True, "")):
                # Simulate /api/config POST handler
                new = {
                    "source_dir": str(cfg.get("source_dir", "")).strip(),
                    "output_dir": str(cfg.get("output_dir", "")).strip(),
                }
                proxy_val = str(cfg.get("proxy", "")).strip()
                if proxy_val:
                    new["proxy"] = proxy_val
                app.write_json(app.CONFIG, new)
                saved = app.read_json(app.CONFIG, {})
                self.assertEqual(saved.get("proxy"), "http://192.168.100.1:7890")

    def test_clean_lrc(self) -> None:
        raw = """[ti:Song Title]
[ar:Artist Name]
[al:Album Name]
[by:NetEase Lrc Builder]
[re:Desktop player]
[ve:1.0.0]
[offset:0]
[length:03:45]
[00:10.00]First line of lyrics
[00:15.50]Second line of lyrics
"""
        cleaned = pipeline.clean_lrc(raw)
        self.assertIn("[ti:Song Title]", cleaned)
        self.assertIn("[ar:Artist Name]", cleaned)
        self.assertIn("[00:10.00]First line of lyrics", cleaned)
        self.assertNotIn("[by:NetEase Lrc Builder]", cleaned)
        self.assertNotIn("[offset:0]", cleaned)
        self.assertNotIn("[re:Desktop player]", cleaned)
        self.assertNotIn("[ve:1.0.0]", cleaned)
        self.assertNotIn("[length:03:45]", cleaned)

    def test_find_and_publish_cue(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src_audio = root / "album.flac"
            src_cue = root / "album.cue"
            src_audio.write_bytes(b"FLAC_DATA")
            src_cue.write_text("PERFORMER \"Artist\"\nTITLE \"Album\"", encoding="utf-8")

            # Check find_sibling_cue
            self.assertEqual(pipeline.find_sibling_cue(src_audio), src_cue)

            # Check publishing alongside audio
            out_dir = root / "out"
            out_dir.mkdir()
            target_audio = out_dir / "album.flac"
            target_audio.write_bytes(b"FLAC_DATA")

            pipeline.publish_cue(target_audio, src_audio)
            target_cue = out_dir / "album.cue"
            self.assertTrue(target_cue.is_file())
            self.assertIn("PERFORMER \"Artist\"", target_cue.read_text(encoding="utf-8"))

    def test_is_offline_mode(self) -> None:
        # Default should be False
        with patch.dict(os.environ, {}, clear=True), patch.object(pipeline, "cfg", return_value={}):
            self.assertFalse(pipeline.is_offline_mode())

        # Env MUSIC_OFFLINE_MODE=1
        with patch.dict(os.environ, {"MUSIC_OFFLINE_MODE": "1"}, clear=True):
            self.assertTrue(pipeline.is_offline_mode())

        # Config file offline_mode=True
        with patch.dict(os.environ, {}, clear=True), patch.object(pipeline, "cfg", return_value={"offline_mode": True}):
            self.assertTrue(pipeline.is_offline_mode())

    def test_truncate_utf8_bytes(self) -> None:
        # Chinese char is 3 bytes. 10 chars = 30 bytes.
        chinese = "一二三四五六七八九十"
        self.assertEqual(len(chinese.encode("utf-8")), 30)
        # Truncate at 10 bytes -> should fit exactly 3 chars (9 bytes), dropping 4th without error
        truncated = pipeline.truncate_utf8_bytes(chinese, 10)
        self.assertEqual(truncated, "一二三")
        self.assertLessEqual(len(truncated.encode("utf-8")), 10)

        # Truncate at 29 bytes -> fits 9 chars (27 bytes)
        self.assertEqual(pipeline.truncate_utf8_bytes(chinese, 29), "一二三四五六七八九")

        # clean() with 300-char string should not exceed max_bytes
        huge_str = "超长音乐名称" * 50
        cleaned = pipeline.clean(huge_str, "fallback", max_bytes=180)
        self.assertLessEqual(len(cleaned.encode("utf-8")), 180)

    def test_destination_multi_disc(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            src = root / "song.mp3"
            src.write_bytes(b"dummy")

            # Single disc
            with patch.object(pipeline, "probe", return_value=("mp3", 0, {"artist": "周杰伦", "album": "范特西", "title": "爱在西元前", "disc": "1", "disctotal": "1"}, {})):
                out = pipeline.destination(root / "out", src, "dummy")
                self.assertNotIn("CD", str(out))
                self.assertIn("范特西", str(out))

            # Multi-disc CD 2
            with patch.object(pipeline, "probe", return_value=("mp3", 0, {"artist": "周杰伦", "album": "经典合辑", "title": "以父之名", "disc": "2", "disctotal": "2"}, {})):
                out = pipeline.destination(root / "out", src, "dummy")
                self.assertIn("CD2", str(out))

            # Multi-disc CD 1 of 2
            with patch.object(pipeline, "probe", return_value=("mp3", 0, {"artist": "周杰伦", "album": "经典合辑", "title": "可爱女人", "disc": "1", "disctotal": "2"}, {})):
                out = pipeline.destination(root / "out", src, "dummy")
                self.assertIn("CD1", str(out))

    def test_validate_settings_allows_arbitrary_future_keys(self) -> None:
        cfg = {
            "source_dir": "/vol1/src",
            "output_dir": "/vol1/out",
            "some_future_key": 123,
            "custom_feature_flag": True,
        }
        with patch.object(Path, "is_dir", return_value=True), patch.object(os, "access", return_value=True):
            s, o = pipeline.validate_settings(cfg)
            self.assertEqual(str(s).replace("\\", "/"), "/vol1/src")
            self.assertEqual(str(o).replace("\\", "/"), "/vol1/out")

    def test_get_run_root_uses_output_directory_and_never_touches_state(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            out = root / "output"
            out.mkdir()
            run_root = pipeline.get_run_root("test-run-123", out)
            self.assertTrue(str(run_root).startswith(str(out)))
            self.assertFalse(str(run_root).startswith(str(pipeline.STATE)))
            pipeline.cleanup_run_root(run_root)
            self.assertFalse(run_root.exists())

    def test_assert_state_budget_auto_cleans_legacy_runs(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d)
            with patch.object(pipeline, "STATE", state_dir):
                legacy_runs = state_dir / "runs"
                legacy_runs.mkdir(parents=True, exist_ok=True)
                (legacy_runs / "dummy.flac").write_bytes(b"flac dummy")
                # Calling assert_state_budget should auto-clean legacy_runs and not raise
                pipeline.assert_state_budget()
                self.assertFalse(legacy_runs.exists())

    def test_merge_bilingual_lrc(self) -> None:
        import domestic_provider
        orig = "[00:10.00]Hello world\n[00:20.00]Goodbye world"
        trans = "[00:10.00]你好世界\n[00:20.00]再见世界"
        merged = domestic_provider.merge_bilingual_lrc(orig, trans)
        self.assertIn("[00:10.00]Hello world", merged)
        self.assertIn("[00:10.00]你好世界", merged)
        self.assertIn("[00:20.00]Goodbye world", merged)
        self.assertIn("[00:20.00]再见世界", merged)

        # None or empty fallback
        self.assertEqual(domestic_provider.merge_bilingual_lrc(orig, None), orig)
        self.assertEqual(domestic_provider.merge_bilingual_lrc(None, trans), trans)

    def test_reset_system_returns_ok_and_cleans_state(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            state_dir = root / "state"
            state_dir.mkdir(parents=True)
            status_file = state_dir / "status.json"
            config_file = state_dir / "config.json"
            log_file = state_dir / "pipeline.log"
            db_file = state_dir / "ledger-v6.sqlite"

            # Create dummy db
            conn = sqlite3.connect(db_file)
            conn.execute("CREATE TABLE runs (id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TABLE source_inventory (path TEXT)")
            conn.execute("CREATE TABLE knowledge_base (key TEXT)")
            conn.execute("INSERT INTO runs VALUES (1)")
            conn.execute("INSERT INTO source_inventory VALUES ('song.mp3')")
            conn.execute("INSERT INTO knowledge_base VALUES ('artist:song')")
            conn.commit()
            conn.close()

            config_file.write_text(json.dumps({"sample_verified": True, "initialized": True, "source_dir": "/s", "output_dir": "/o"}), encoding="utf-8")
            status_file.write_text(json.dumps({"state": "idle", "message": "done"}), encoding="utf-8")
            log_file.write_text("sample log", encoding="utf-8")

            with patch.object(app, "STATE", state_dir), \
                 patch.object(app, "STATUS", status_file), \
                 patch.object(app, "CONFIG", config_file), \
                 patch.object(app, "LOG", log_file):
                res = app.reset_system(clear_history=True, clear_inventory=True, clear_kb=True)
                self.assertTrue(res.get("ok"))
                self.assertTrue(res.get("success"))
                self.assertEqual(res.get("cleared_history"), True)
                self.assertEqual(res.get("cleared_inventory"), True)
                self.assertEqual(res.get("cleared_knowledge_base"), True)

                # Check DB was cleared
                conn = sqlite3.connect(db_file)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM source_inventory").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM knowledge_base").fetchone()[0], 0)
                conn.close()

                # Check config flags cleared
                cfg = json.loads(config_file.read_text(encoding="utf-8"))
                self.assertNotIn("sample_verified", cfg)
                self.assertNotIn("initialized", cfg)
                self.assertEqual(cfg.get("source_dir"), "/s")

                # Check log cleared
                self.assertEqual(log_file.read_text(encoding="utf-8"), "")

    def test_scheduler_compute_next_run_and_trigger(self) -> None:
        from datetime import datetime, timedelta

        # Test compute_next_run logic
        base_dt = datetime(2026, 9, 16, 14, 0, 0)
        
        # 1. Hourly
        next_hourly = app.compute_next_run("hourly", from_dt=base_dt)
        self.assertEqual(next_hourly, datetime(2026, 9, 16, 15, 0, 0))

        # 2. Intervals
        self.assertEqual(app.compute_next_run("interval_6h", from_dt=base_dt), datetime(2026, 9, 16, 20, 0, 0))
        self.assertEqual(app.compute_next_run("interval_12h", from_dt=base_dt), datetime(2026, 9, 17, 2, 0, 0))
        self.assertEqual(app.compute_next_run("interval_24h", from_dt=base_dt), datetime(2026, 9, 17, 14, 0, 0))

        # 3. Daily
        # If now is 14:00 and target is 03:00, target should be next day 03:00
        next_daily = app.compute_next_run("daily", custom_time="03:00", from_dt=base_dt)
        self.assertEqual(next_daily, datetime(2026, 9, 17, 3, 0, 0))

        # If now is 02:00 and target is 03:00, target should be today 03:00
        early_dt = datetime(2026, 9, 16, 2, 0, 0)
        next_daily_early = app.compute_next_run("daily", custom_time="03:00", from_dt=early_dt)
        self.assertEqual(next_daily_early, datetime(2026, 9, 16, 3, 0, 0))

        # Test check_and_trigger_schedule
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            cfg_file = temp_path / "config.json"
            status_file = temp_path / "status.json"

            with patch.object(app, "CONFIG", cfg_file), patch.object(app, "STATUS", status_file):
                # Case 1: Disabled
                cfg_file.write_text(json.dumps({"schedule": {"enabled": False}}), encoding="utf-8")
                status_file.write_text(json.dumps({"state": "idle"}), encoding="utf-8")
                self.assertFalse(app.check_and_trigger_schedule(base_dt))

                # Case 2: Enabled but not initialized -> Should return False (guard against uninitialized run)
                cfg_file.write_text(json.dumps({
                    "initialized": False,
                    "schedule": {"enabled": True, "rule": "interval_6h", "next_run": "2026-09-16 13:00:00"}
                }), encoding="utf-8")
                self.assertFalse(app.check_and_trigger_schedule(base_dt))

                # Case 3: Enabled and initialized, but state is running -> Should return False
                cfg_file.write_text(json.dumps({
                    "initialized": True,
                    "schedule": {"enabled": True, "rule": "interval_6h", "next_run": "2026-09-16 13:00:00"}
                }), encoding="utf-8")
                status_file.write_text(json.dumps({"state": "running"}), encoding="utf-8")
                self.assertFalse(app.check_and_trigger_schedule(base_dt))

                # Case 4: Enabled and initialized, state is idle, but next_run is in future -> Should return False
                status_file.write_text(json.dumps({"state": "idle"}), encoding="utf-8")
                cfg_file.write_text(json.dumps({
                    "initialized": True,
                    "schedule": {"enabled": True, "rule": "interval_6h", "next_run": "2026-09-16 15:00:00"}
                }), encoding="utf-8")
                self.assertFalse(app.check_and_trigger_schedule(base_dt))

                # Case 5: Enabled, initialized, state idle, and next_run is due -> Triggers start_mode
                cfg_file.write_text(json.dumps({
                    "initialized": True,
                    "schedule": {"enabled": True, "rule": "interval_6h", "next_run": "2026-09-16 13:00:00"}
                }), encoding="utf-8")
                with patch.object(app, "start_mode", return_value=(202, "{}")) as mock_start:
                    self.assertTrue(app.check_and_trigger_schedule(base_dt))
                    mock_start.assert_called_once_with("incremental", lang="zh")

                    # Verify updated config next_run and last_run
                    updated_cfg = json.loads(cfg_file.read_text(encoding="utf-8"))
                    self.assertEqual(updated_cfg["schedule"]["last_run"], "2026-09-16 14:00:00")
                    self.assertEqual(updated_cfg["schedule"]["next_run"], "2026-09-16 20:00:00")

    def test_incremental_exact_dedupe_uses_index(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory) / "state"), patch.object(pipeline, "LEDGER", Path(directory) / "state" / "ledger-v6.sqlite"):
            src_dir, out_dir = Path(directory) / "source", Path(directory) / "output"
            src_dir.mkdir()
            out_dir.mkdir()
            (Path(directory) / "state").mkdir()
            pub_song = out_dir / "Artist" / "Album" / "Song.mp3"
            pub_song.parent.mkdir(parents=True)
            pub_song.write_bytes(b"exact same music audio content")

            incoming_dup = src_dir / "Incoming_Dup.mp3"
            incoming_dup.write_bytes(b"exact same music audio content")

            conn = pipeline.db()
            try:
                conn.execute(
                    "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path) VALUES(?,?,?,?,'published',?)",
                    ("previous/source.mp3", len(b"exact same music audio content"), 12345, pipeline.sha256(pub_song), str(pub_song))
                )
                conn.commit()
            finally:
                conn.close()

            # Calling exact_dedupe in incremental mode should detect duplicate via SQLite index
            candidates = {incoming_dup}
            with patch("subprocess.run") as mock_run:
                remaining = pipeline.exact_dedupe("incremental-20260917-123456-abc123", src_dir, candidates, out_dir)
                # Should not invoke jdupes at all!
                mock_run.assert_not_called()
                self.assertEqual(len(remaining), 0)

    def test_circuit_breaker_stops_stubborn_retries(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            src_dir, out_dir = Path(directory) / "source", Path(directory) / "output"
            src_dir.mkdir()
            out_dir.mkdir()
            stubborn = src_dir / "stubborn.mp3"
            stubborn.write_bytes(b"unrecognized noise")

            conn = pipeline.db()
            try:
                # Track has failed twice already
                conn.execute(
                    "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,retry_count,unresolvable) VALUES(?,?,?,?,'failed',2,1)",
                    (str(stubborn), stubborn.stat().st_size, stubborn.stat().st_mtime_ns, "hash")
                )
                conn.commit()
            finally:
                conn.close()

            buckets = pipeline.classify_sources(src_dir, out_dir)
            self.assertIn(stubborn, buckets["unresolvable"])
            self.assertNotIn(stubborn, buckets["retry"])
            self.assertEqual(pipeline.flatten_buckets(buckets), [])

    def test_fallback_publish_one(self) -> None:
        with tempfile.TemporaryDirectory() as directory, patch.object(pipeline, "STATE", Path(directory)), patch.object(pipeline, "LEDGER", Path(directory) / "ledger-v6.sqlite"):
            src_dir, out_dir = Path(directory) / "source", Path(directory) / "output"
            src_dir.mkdir()
            out_dir.mkdir()
            song = src_dir / "MyVoiceMemo.mp3"
            song.write_bytes(b"audio recording")

            target = pipeline.fallback_publish_one(song, out_dir)
            self.assertIsNotNone(target)
            self.assertTrue(target.is_file())
            self.assertIn("未知艺术家", str(target))

            conn = pipeline.db()
            try:
                row = conn.execute("SELECT disposition, unresolvable FROM source_inventory WHERE source_path=?", (str(song),)).fetchone()
                self.assertEqual(row["disposition"], "published")
                self.assertEqual(row["unresolvable"], 0)
            finally:
                conn.close()

    def test_chromaprint_packing_and_similarity(self) -> None:
        raw_fp = [12345678, 87654321, 11223344, 99887766, 55443322]
        packed = pipeline.pack_fingerprint(raw_fp)
        self.assertIsInstance(packed, str)
        self.assertTrue(len(packed) > 0)
        unpacked = pipeline.unpack_fingerprint(packed)
        self.assertEqual(unpacked, raw_fp)

        # Self-similarity should be 1.0
        self.assertAlmostEqual(pipeline.chromaprint_similarity(raw_fp, raw_fp), 1.0, places=4)

        # Dissimilar fingerprints
        diff_fp = [0, 0, 0, 0, 0]
        self.assertLess(pipeline.chromaprint_similarity(raw_fp, diff_fp), 0.7)

        # Empty fingerprint similarity
        self.assertEqual(pipeline.chromaprint_similarity([], raw_fp), 0.0)

    def test_strip_riff_chunk_protects_markers(self) -> None:
        import struct
        with tempfile.TemporaryDirectory() as d:
            wav_file = Path(d) / "test.wav"
            # Construct a minimal RIFF WAV with two LIST chunks:
            # 1. LIST-adtl (marker chunk, 8 bytes payload: b"adtl1234") -> should NOT be stripped
            # 2. LIST-INFO (metadata chunk, 8 bytes payload: b"INFO1234") -> SHOULD be stripped
            chunk1 = b"LIST" + struct.pack("<I", 8) + b"adtl1234"
            chunk2 = b"LIST" + struct.pack("<I", 8) + b"INFO1234"
            data_chunk = b"data" + struct.pack("<I", 4) + b"\x00\x00\x00\x00"
            riff_body = b"WAVE" + chunk1 + chunk2 + data_chunk
            header = b"RIFF" + struct.pack("<I", len(riff_body))
            wav_file.write_bytes(header + riff_body)

            # Run strip_riff_chunk looking for b"LIST"
            modified = pipeline.strip_riff_chunk(wav_file, b"LIST")
            self.assertTrue(modified)
            content = wav_file.read_bytes()
            # LIST-adtl must remain intact!
            self.assertIn(b"adtl1234", content)
            # LIST-INFO must be gone!
            self.assertNotIn(b"INFO1234", content)

    def test_app_get_local_now(self) -> None:
        import app
        with patch.dict(os.environ, {"TZ": "Asia/Shanghai"}):
            dt = app.get_local_now()
            self.assertIsNotNone(dt)
            self.assertIsNone(dt.tzinfo)

    def test_circuit_breaker_two_failures_becomes_unresolvable(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            src_dir = Path(d) / "source"
            out_dir = Path(d) / "output"
            state_dir = Path(d) / "state"
            src_dir.mkdir()
            out_dir.mkdir()
            state_dir.mkdir()
            bad_song = src_dir / "bad.mp3"
            bad_song.write_bytes(b"corrupt")

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"):
                conn = pipeline.db()
                try:
                    st = bad_song.stat()
                    conn.execute(
                        "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,retry_count,unresolvable,rules_version) VALUES(?,?,?,?,'failed',2,0,?)",
                        (str(bad_song), st.st_size, st.st_mtime_ns, "badhash", pipeline.RULES_VERSION)
                    )
                    conn.commit()
                finally:
                    conn.close()

                buckets = pipeline.classify_sources(src_dir, out_dir)
                self.assertIn(bad_song, buckets["unresolvable"])
                self.assertNotIn(bad_song, buckets["retry"])


    def test_frontend_backend_metrics_contract(self) -> None:
        """Automated contract test ensuring all m.<field> references in dashboard.html exist in backend metrics dictionaries."""
        import re
        html_path = Path(__file__).parent / "dashboard.html"
        self.assertTrue(html_path.is_file())
        html_content = html_path.read_text(encoding="utf-8")
        
        # Extract all m.<identifier> usages in dashboard.html
        frontend_keys = set(re.findall(r"\bm\.([a-zA-Z0-9_]+)", html_content))
        self.assertGreater(len(frontend_keys), 10)

        with tempfile.TemporaryDirectory() as d, patch.object(pipeline, "STATE", Path(d)), patch.object(pipeline, "LEDGER", Path(d) / "ledger-v6.sqlite"), patch.object(app, "STATE", Path(d)):
            with pipeline.db() as conn:
                conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status,started_at,finished_at,phase,phase_done,phase_total) VALUES('contract_run','full','/s','/o','done',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'done',0,0)")
                conn.commit()

            pipe_rep = pipeline.report("contract_run")
            pipe_metrics = pipe_rep.get("metrics", {})

            with patch("app.state_size", return_value=0):
                app_rep = app.report("contract_run")
                app_metrics = app_rep.get("metrics", {})

            for k in frontend_keys:
                self.assertIn(
                    k, pipe_metrics,
                    f"Dead field detected! Frontend dashboard.html references 'm.{k}', but pipeline.report()['metrics'] does not provide it!"
                )
                self.assertIn(
                    k, app_metrics,
                    f"Dead field detected! Frontend dashboard.html references 'm.{k}', but app.report()['metrics'] does not provide it!"
                )

    def test_fallback_published_and_quality_upgrade_metrics(self) -> None:
        """Verify report() correctly tallies quality_upgraded from groups decision and fallback_published."""
        with tempfile.TemporaryDirectory() as d, patch.object(pipeline, "STATE", Path(d)), patch.object(pipeline, "LEDGER", Path(d) / "ledger-v6.sqlite"), patch.object(app, "STATE", Path(d)):
            with pipeline.db() as conn:
                conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status,started_at,finished_at,phase,phase_done,phase_total) VALUES('r_test','incremental','/s','/o','done',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'done',0,0)")
                # Normal published
                conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition,output_path,metadata_state) VALUES('r_test','/s/normal.flac',100,1,100.0,'audio','published','/o/周杰伦/范特西/01 - 爱在西元前.flac','metadata_matched')")
                # Fallback published (unknown artist)
                conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition,output_path,metadata_state) VALUES('r_test','/s/rare.mp3',100,1,100.0,'audio','published','/o/未知艺术家/未知专辑/rare.mp3','metadata_not_found')")
                # Quality upgrade group
                conn.execute("INSERT INTO groups(id,run_id,tool,similarity,winner_source_path,decision,paths_json) VALUES('grp1','r_test','quality_upgrade',1.0,'/s/better.flac','quality_upgrade','{}')")
                conn.commit()

            pipe_rep = pipeline.report("r_test")
            self.assertEqual(pipe_rep["metrics"]["published"], 2)
            self.assertEqual(pipe_rep["metrics"]["fallback_published"], 1)
            self.assertEqual(pipe_rep["metrics"]["quality_upgraded"], 1)

            with patch("app.state_size", return_value=0):
                app_rep = app.report("r_test")
                self.assertEqual(app_rep["metrics"]["published"], 2)
                self.assertEqual(app_rep["metrics"]["fallback_published"], 1)
                self.assertEqual(app_rep["metrics"]["quality_upgraded"], 1)

    def test_unresolved_and_matched_target_logic(self) -> None:
        """Verify matched_target is populated for duplicate files and /api/unresolved returns both sets."""
        with tempfile.TemporaryDirectory() as d, patch.object(pipeline, "STATE", Path(d)), patch.object(pipeline, "LEDGER", Path(d) / "ledger-v6.sqlite"), patch.object(app, "STATE", Path(d)):
            with pipeline.db() as conn:
                conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status,started_at,finished_at,phase,phase_done,phase_total) VALUES('r_dup','incremental','/s','/o','done',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'done',0,0)")
                # Normal target in output
                conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition,output_path,audio_sha256) VALUES('r_dup','/s/first.flac',100,1,100.0,'audio','published','/o/Artist/Album/01.flac','hash_match')")
                # Dedupe skipped file
                conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition,output_path,audio_sha256) VALUES('r_dup','/s/dup.flac',100,1,100.0,'audio','duplicate_exact',NULL,'hash_match')")
                # Source inventory entries
                conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,unresolvable,retry_count,retry_reason) VALUES('/s/isolated.mp3',100,1,'h1','failed','',1,2,'Max retries')")
                conn.execute("INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,unresolvable,retry_count,metadata_state) VALUES('/s/unknown.flac',100,1,'h2','published','/o/未知艺术家/未知专辑/unknown.flac',0,0,'metadata_not_found')")
                conn.commit()

            db_conn = app.connection()
            try:
                # 1. Test unresolved query
                unresolved_rows = db_conn.execute(
                    "SELECT source_path, disposition, retry_reason FROM source_inventory WHERE unresolvable=1 OR (disposition='failed' AND retry_count >= 2)"
                ).fetchall()
                self.assertEqual(len(unresolved_rows), 1)
                self.assertEqual(unresolved_rows[0]["source_path"], "/s/isolated.mp3")

                fallback_rows = db_conn.execute(
                    "SELECT source_path, output_path FROM source_inventory WHERE disposition='published' AND (output_path LIKE '%未知艺术家%' OR output_path LIKE '%Unknown Artist%' OR metadata_state='metadata_not_found')"
                ).fetchall()
                self.assertEqual(len(fallback_rows), 1)
                self.assertEqual(fallback_rows[0]["source_path"], "/s/unknown.flac")

                # 2. Test matched_target enrichment logic for duplicate items
                items_rows = db_conn.execute(
                    "SELECT id, run_id, source_path, disposition, output_path, audio_sha256 FROM items WHERE run_id='r_dup' ORDER BY id ASC"
                ).fetchall()
                items_list = [dict(r) for r in items_rows]
                for item in items_list:
                    if item.get("disposition") in ("duplicate_exact", "exact_dedupe", "duplicate_same_recording", "acoustic_dedupe") and not item.get("output_path"):
                        target_row = db_conn.execute(
                            "SELECT output_path FROM items WHERE audio_sha256=? AND output_path IS NOT NULL ORDER BY id DESC LIMIT 1",
                            (item.get("audio_sha256"),)
                        ).fetchone()
                        if target_row and target_row["output_path"]:
                            item["matched_target"] = target_row["output_path"]

                dup_item = next(it for it in items_list if it["source_path"] == "/s/dup.flac")
                self.assertEqual(dup_item["matched_target"], "/o/Artist/Album/01.flac")
            finally:
                db_conn.close()


class TestFrontendStaticContracts(unittest.TestCase):
    """前端单文件 dashboard.html 的纯 Python 静态契约安全测试。
    在不依赖 Node.js/Jest/Playwright 的前提下，通过正则/JSON 解析
    实现对 DOM ID 完整性、国际化多语言键完整性、严禁裸读 I18N 等契约的断言，
    从源头杜绝因属性拼写错误或 DOM ID 不匹配导致的前端运行时卡死。
    """

    @classmethod
    def setUpClass(cls):
        html_path = Path(__file__).resolve().parent / "dashboard.html"
        if not html_path.exists():
            raise unittest.SkipTest(f"{html_path} not found")
        cls.html_content = html_path.read_text(encoding="utf-8")

    def test_frontend_dom_ids_integrity(self):
        """断言 JS 代码中通过 getElementById 引用的所有 ID 必须在 HTML 中存在。"""
        # 1. 提取所有 HTML 中的 id
        declared_ids = set(re.findall(r'\bid=["\']([a-zA-Z0-9_\-]+)["\']', self.html_content))
        # 2. 提取所有 JS 中 getElementById("...") 引用的 ID
        referenced_ids = set(re.findall(r'document\.getElementById\(["\']([a-zA-Z0-9_\-]+)["\']\)', self.html_content))

        self.assertGreater(len(referenced_ids), 50, "未能提取到足够的前端 DOM ID 引用")
        missing_ids = referenced_ids - declared_ids
        self.assertEqual(missing_ids, set(), f"JS 引用的 DOM ID 在 HTML 中不存在: {missing_ids}")

    def test_frontend_i18n_keys_integrity(self):
        """断言 JS 中所有 t('...') 调用的多语言 key 必须在中英两套语言包中 100% 存在。"""
        # 1. 提取 const I18N = { ... }; 字典定义
        match = re.search(r'const I18N = (\{.*?\n    \});\n\n    let currentLang', self.html_content, re.DOTALL)
        self.assertIsNotNone(match, "未能从 dashboard.html 中定位到 const I18N 定义")
        js_code = match.group(1)

        # 2. 转换为合法 JSON
        # 去除单行注释
        s = re.sub(r'//[^\n]*', '', js_code)
        # 将反引号模板字符串转义为标准双引号字符串
        def repl_backtick(m):
            content = m.group(1).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '')
            return f'"{content}"'
        s = re.sub(r'`(.*?)`', repl_backtick, s, flags=re.DOTALL)
        # 给未加引号的 key 加上双引号
        s = re.sub(r'([{,]\s*)([a-zA-Z0-9_\-]+)\s*:', r'\1"\2":', s)
        # 移除末尾多余逗号
        s = re.sub(r',\s*([}\]])', r'\1', s)

        i18n_data = json.loads(s)
        self.assertIn("zh", i18n_data)
        self.assertIn("en", i18n_data)
        zh = i18n_data["zh"]
        en = i18n_data["en"]

        # 3. 提取所有 t("...") 或 t('...') 调用
        t_calls = sorted(set(re.findall(r'\bt\(["\']([a-zA-Z0-9_\.]+)["\']', self.html_content)))
        self.assertGreater(len(t_calls), 50, "未能提取到足够的前端 t() 国际化键调用")

        def check_path(tree, path):
            if path.endswith('.'):
                clean_path = path.rstrip('.')
                curr = tree
                for part in clean_path.split('.'):
                    if not isinstance(curr, dict) or part not in curr:
                        return False
                    curr = curr[part]
                return isinstance(curr, dict) and len(curr) > 0
            else:
                curr = tree
                for part in path.split('.'):
                    if not isinstance(curr, dict) or part not in curr:
                        return False
                    curr = curr[part]
                return True

        missing_zh = [p for p in t_calls if not check_path(zh, p)]
        missing_en = [p for p in t_calls if not check_path(en, p)]

        self.assertEqual(missing_zh, [], f"以下 t() 键在 zh 字典中缺失: {missing_zh}")
        self.assertEqual(missing_en, [], f"以下 t() 键在 en 字典中缺失: {missing_en}")

    def test_frontend_no_raw_i18n_access(self):
        """断言代码中不存在随意裸读 I18N[currentLang] 的行为，必须统一使用 t() 安全函数。"""
        # 只允许在 t() 内部和 setLanguage() 内部出现最多 2 处
        raw_accesses = [line.strip() for line in self.html_content.splitlines() if "I18N[" in line]
        self.assertLessEqual(len(raw_accesses), 2, f"发现违规裸读 I18N[...]: {raw_accesses}")



class RefactorRegressionTests(unittest.TestCase):
    def test_exact_dedupe_hash_cache_penetration(self):
        """验证 exact_dedupe：大小唯一者不查哈希，缓存命中者不重复读盘。"""
        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d) / "state"
            src_dir = Path(d) / "source"
            state_dir.mkdir()
            src_dir.mkdir()

            f1 = src_dir / "song1.mp3"
            f2 = src_dir / "song2.mp3"
            f3 = src_dir / "unique.mp3"
            f1.write_bytes(b"content_same" * 100)
            f2.write_bytes(b"content_same" * 100)
            f3.write_bytes(b"unique_size_12345")

            hash_cache = {
                f1: "hash_same",
                f2: "hash_same",
            }

            with patch.object(pipeline, "STATE", state_dir), \
                 patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"), \
                 patch.object(pipeline, "sha256", wraps=pipeline.sha256) as mock_sha:
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('r_test','full','/s','/o','running')")
                    for p in (f1, f2, f3):
                        conn.execute("INSERT INTO items(run_id,source_path,source_size,source_mtime_ns,duration,source_kind,disposition) VALUES('r_test',?,?,1,100,'audio','candidate')",
                                     (str(p), p.stat().st_size))
                    conn.commit()
                finally:
                    conn.close()

                remaining = pipeline.exact_dedupe("r_test", src_dir, {f1, f2, f3}, library=None, hash_cache=hash_cache)
                # f3 是唯一大小保留，f1/f2 中保留一个，另一个被去重
                self.assertEqual(len(remaining), 2)
                self.assertIn(f3, remaining)
                # 关键断言：因为 f3 大小独一无二直接跳过，f1/f2 在 hash_cache 中命中，sha256 调用次数必须为 0！
                self.assertEqual(mock_sha.call_count, 0)

    def test_fallback_output_cond_coverage(self):
        """验证 pipeline 与 app 的 FALLBACK_OUTPUT_COND 一致性并精准覆盖所有未知/未分类情形。"""
        self.assertEqual(pipeline.FALLBACK_OUTPUT_COND, app.FALLBACK_OUTPUT_COND)

        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d) / "state"
            state_dir.mkdir()
            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"):
                conn = pipeline.db()
                try:
                    test_cases = [
                        ("/out/未知艺术家/Album/s1.mp3", "published", "cloud_matched", True),
                        ("/out/Unknown Artist/Album/s2.mp3", "published", "cloud_matched", True),
                        ("/out/Artist/未知专辑/s3.mp3", "published", "cloud_matched", True),
                        ("/out/Artist/未分类专辑/s4.mp3", "published", "cloud_matched", True),
                        ("/out/Artist/Unknown Album/s5.mp3", "published", "cloud_matched", True),
                        ("/out/Artist/Album/s6.mp3", "published", "fallback_tags", True),
                        ("/out/Artist/Album/s7.mp3", "published", "cloud_matched", False),
                    ]
                    for idx, (path, disp, meta, _) in enumerate(test_cases):
                        conn.execute(
                            "INSERT INTO source_inventory(source_path,size_bytes,mtime_ns,sha256,disposition,output_path,metadata_state) "
                            "VALUES(?,?,1,'h',?,?,?)",
                            (f"/src/{idx}.mp3", 1000 + idx, disp, path, meta)
                        )
                    conn.commit()

                    query = f"SELECT output_path FROM source_inventory WHERE disposition = 'published' AND ({pipeline.FALLBACK_OUTPUT_COND})"
                    matched = [r["output_path"] for r in conn.execute(query).fetchall()]
                    expected = [tc[0] for tc in test_cases if tc[3]]
                    self.assertEqual(sorted(matched), sorted(expected))
                finally:
                    conn.close()

    def test_corrupted_file_persisted_to_source_inventory(self):
        """损坏的音频文件在校验阶段必须被沉淀至 source_inventory(unresolvable=1, disposition='failed')。"""
        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d) / "state"
            src_dir = Path(d) / "source"
            state_dir.mkdir()
            src_dir.mkdir()

            bad_file = src_dir / "corrupted.mp3"
            bad_file.write_bytes(b"invalid garbage header not audio 1234567890")

            with patch.object(pipeline, "STATE", state_dir), patch.object(pipeline, "LEDGER", state_dir / "ledger-v6.sqlite"):
                conn = pipeline.db()
                try:
                    conn.execute("INSERT INTO runs(id,mode,source_dir,output_dir,status) VALUES('r_bad','incremental','/s','/o','running')")
                    conn.commit()
                finally:
                    conn.close()

                # analyse_regular 遇到损坏文件
                res = pipeline.analyse_regular("r_bad", [bad_file])
                self.assertIn(bad_file, res)
                self.assertIn("error", res[bad_file])

                # 断言：已持久化进 source_inventory
                conn = pipeline.db()
                try:
                    row = conn.execute("SELECT * FROM source_inventory WHERE source_path = ?", (str(bad_file),)).fetchone()
                    self.assertIsNotNone(row)
                    self.assertEqual(row["disposition"], "failed")
                    self.assertEqual(row["unresolvable"], 1)
                finally:
                    conn.close()

    def test_log_rotation_and_scheduler_backoff(self):
        """测试 start_mode 日志轮转以及调度触发受阻时的 10 分钟退避机制。"""
        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d) / "state"
            state_dir.mkdir()
            log_file = state_dir / "last-run.log"
            log_file.write_text("old log line 1\nold log line 2\n", encoding="utf-8")
            status_file = state_dir / "status.json"
            status_file.write_text(json.dumps({"state": "idle"}), encoding="utf-8")
            cfg_file = state_dir / "config.json"
            cfg_data = {
                "source_dir": "/vol1/music",
                "output_dir": "/vol1/output",
                "initialized": True,
                "schedule": {
                    "enabled": True,
                    "rule": "daily",
                    "time": "03:00",
                    "next_run": "2026-09-20 03:00:00"
                }
            }
            cfg_file.write_text(json.dumps(cfg_data), encoding="utf-8")

            with patch.object(app, "LOG", log_file), \
                 patch.object(app, "CONFIG", cfg_file), \
                 patch.object(app, "STATUS", status_file), \
                 patch.object(app, "LOCK", state_dir / "run.lock"), \
                 patch.object(app, "config_valid", return_value=(True, "")), \
                 patch.object(app, "sample_ready", return_value=True), \
                 patch.object(app, "take_lock", return_value=True):
                # 1. 模拟 start_mode 触发，验证日志轮转
                with patch.object(app, "run_pipeline"):
                    code, _ = app.start_mode("incremental")
                    self.assertEqual(code, 202)
                    rotated = log_file.with_name("last-run.1.log")
                    self.assertTrue(rotated.exists())
                    self.assertIn("old log line 1", rotated.read_text(encoding="utf-8"))
                    self.assertIn("正在初始化并启动", log_file.read_text(encoding="utf-8"))

                # 2. 模拟调度器触发失败（先将 status 恢复为 idle，再模拟 start_mode 返回 409 占用），断言 next_run 顺延 10 分钟
                status_file.write_text(json.dumps({"state": "idle"}), encoding="utf-8")
                with patch.object(app, "start_mode", return_value=(409, "busy")):
                    test_now = app.datetime(2026, 9, 20, 3, 0, 0)
                    with patch.object(app, "get_local_now", return_value=test_now):
                        triggered = app.check_and_trigger_schedule()
                        self.assertFalse(triggered)
                        saved_cfg = json.loads(cfg_file.read_text(encoding="utf-8"))
                        self.assertEqual(saved_cfg["schedule"]["next_run"], "2026-09-20 03:10:00")

    def test_static_ast_no_undefined_variables(self):
        """【体系加固】静态 AST 符号分析门禁：强制扫描 app.py 与 pipeline.py，杜绝遗漏 import 或未定义全局变量逃逸入库。"""
        import ast, builtins
        project_root = Path(__file__).parent

        for target_filename in ("app.py", "pipeline.py"):
            target_path = project_root / target_filename
            with open(target_path, "r", encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=str(target_path))

            defined = set(dir(builtins))
            undefined_uses: list[tuple[str, int]] = []

            class ScopeVisitor(ast.NodeVisitor):
                def __init__(self):
                    self.scopes = [defined.copy()]

                def visit_Import(self, node):
                    for alias in node.names:
                        name = alias.asname or alias.name.split(".")[0]
                        self.scopes[-1].add(name)

                def visit_ImportFrom(self, node):
                    for alias in node.names:
                        name = alias.asname or alias.name
                        self.scopes[-1].add(name)

                def visit_FunctionDef(self, node):
                    self.scopes[-1].add(node.name)
                    self.scopes.append(set(self.scopes[-1]))
                    for arg in node.args.args:
                        self.scopes[-1].add(arg.arg)
                    self.generic_visit(node)
                    self.scopes.pop()

                def visit_AsyncFunctionDef(self, node):
                    self.visit_FunctionDef(node)

                def visit_ClassDef(self, node):
                    self.scopes[-1].add(node.name)
                    self.scopes.append(set(self.scopes[-1]))
                    self.generic_visit(node)
                    self.scopes.pop()

                def visit_Name(self, node):
                    if isinstance(node.ctx, ast.Store):
                        self.scopes[-1].add(node.id)
                    elif isinstance(node.ctx, ast.Load):
                        if not any(node.id in s for s in self.scopes):
                            undefined_uses.append((node.id, node.lineno))

            v = ScopeVisitor()
            v.visit(tree)

            # 排除循环推导式/内置宏/已知晚绑定符号
            known_benign = {
                "__file__", "DEFAULT_SOURCE", "DEFAULT_OUTPUT", "accessible_paths",
                "normalize_track_stem", "classify_sources", "flatten_buckets",
                "classify_counts", "find_cached_publish", "recorded_source_for",
                "clean", "state_size", "exc"
            }
            # 推导式中内部使用的常规短变量
            comprehension_vars = {"k", "v", "p", "r", "x", "s", "i", "h", "c", "w", "a", "item", "row", "path", "part", "value", "key", "word", "label", "root", "parent", "side", "stream", "char", "details", "run"}
            critical_undefined = [
                (name, line) for name, line in undefined_uses
                if name not in known_benign and name not in comprehension_vars
            ]
            self.assertEqual(critical_undefined, [], f"{target_filename} 存在未导入或未定义的全局符号，将引发运行时 NameError: {critical_undefined}")

    def test_run_pipeline_real_subprocess_execution(self):
        """【体系加固】真实子进程端到端冒烟测试：不使用 mock，真实拉起子进程并验证日志写入与异常捕获。"""
        with tempfile.TemporaryDirectory() as d:
            state_dir = Path(d) / "state"
            state_dir.mkdir()
            log_file = state_dir / "last-run.log"
            status_file = state_dir / "status.json"
            status_file.write_text(json.dumps({"state": "running"}), encoding="utf-8")
            config_file = state_dir / "config.json"
            config_file.write_text(json.dumps({}), encoding="utf-8")

            with patch.object(app, "STATE", state_dir), \
                 patch.object(app, "LOG", log_file), \
                 patch.object(app, "STATUS", status_file), \
                 patch.object(app, "CONFIG", config_file), \
                 patch.object(app, "LOCK", state_dir / "pipeline.lock"):
                # 真实执行 run_pipeline，传入一个不存在的无效模式，验证：
                # 1. 真实子进程能够被正常 Popen 拉起并执行（不因缺少 sys 崩溃）；
                # 2. 输出正常被追加到 log_file 中；
                # 3. 失败时 status.json 正常被更新为 failed。
                app.run_pipeline("invalid_smoke_test_mode")

                self.assertTrue(log_file.exists(), "子进程未能创建或写入日志文件")
                saved_status = json.loads(status_file.read_text(encoding="utf-8"))
                self.assertEqual(saved_status["state"], "failed", "子进程执行失败后状态未正常标记为 failed")


if __name__ == "__main__":
    unittest.main()




