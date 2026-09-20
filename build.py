#!/usr/bin/env python3
"""Build an fnOS .fpk package from the music-mate source directory."""
import io
import os
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PKG_DIR = ROOT / "package"
APP_DIR = PKG_DIR / "app"
MANIFEST = PKG_DIR / "manifest"


def get_manifest_info() -> dict[str, str]:
    info = {}
    for line in MANIFEST.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        info[key.strip()] = val.strip()
    return info


import gzip

FIXED_MTIME = 1726400000  # Reproducible build timestamp (2024-09-15 11:33:20 UTC)


def make_app_tgz(app_dir: Path) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0.0) as gz:
        with tarfile.open(fileobj=gz, mode="w:") as tar:
            for path in sorted(app_dir.rglob("*")):
                rel = path.relative_to(app_dir).as_posix()
                tarinfo = tar.gettarinfo(str(path), arcname=rel)
                tarinfo.mtime = FIXED_MTIME
                if path.is_file():
                    tarinfo.mode = 0o644
                    with path.open("rb") as f:
                        tar.addfile(tarinfo, f)
                elif path.is_dir():
                    tarinfo.mode = 0o755
                    tar.addfile(tarinfo)
            config_dir = PKG_DIR / "config"
            if config_dir.is_dir():
                ti = tar.gettarinfo(str(config_dir), arcname="config")
                ti.mode = 0o755
                ti.mtime = FIXED_MTIME
                tar.addfile(ti)
                for path in sorted(config_dir.rglob("*")):
                    rel = ("config" / path.relative_to(config_dir)).as_posix()
                    ti = tar.gettarinfo(str(path), arcname=rel)
                    ti.mtime = FIXED_MTIME
                    if path.is_file():
                        ti.mode = 0o644
                        with path.open("rb") as f:
                            tar.addfile(ti, f)
                    elif path.is_dir():
                        ti.mode = 0o755
                        tar.addfile(ti)
    return buffer.getvalue()


def build_fpk(output_path: Path | None = None) -> Path:
    meta = get_manifest_info()
    appname = meta.get("appname", "fn-music-companion")
    version = meta.get("version", "1.0.0")
    if output_path is None:
        output_path = ROOT / f"{appname}-v{version}.fpk"

    app_tgz_bytes = make_app_tgz(APP_DIR)

    # Calculate checksum from app_tgz_bytes
    import hashlib
    checksum = hashlib.md5(app_tgz_bytes).hexdigest()

    with gzip.GzipFile(filename=output_path.name, fileobj=open(output_path, "wb"), mode="wb", mtime=0.0) as gz:
        with tarfile.open(fileobj=gz, mode="w:") as fpk:
            # 1. Add app.tgz
            tinfo = tarfile.TarInfo(name="app.tgz")
            tinfo.size = len(app_tgz_bytes)
            tinfo.mode = 0o644
            tinfo.mtime = FIXED_MTIME
            fpk.addfile(tinfo, io.BytesIO(app_tgz_bytes))

            # 2. Add other items: cmd, config, wizard, manifest, ICONs
            items = ["cmd", "config", "ICON.PNG", "ICON_256.PNG", "manifest", "wizard"]
            for item in items:
                p = PKG_DIR / item
                if not p.exists():
                    continue
                if item == "manifest":
                    manifest_text = p.read_text(encoding="utf-8")
                    lines = [l for l in manifest_text.splitlines() if not l.strip().startswith("checksum")]
                    lines.append(f"checksum              = {checksum}\n")
                    new_manifest_bytes = "\n".join(lines).encode("utf-8")
                    ti = tarfile.TarInfo(name="manifest")
                    ti.size = len(new_manifest_bytes)
                    ti.mode = 0o644
                    ti.mtime = FIXED_MTIME
                    fpk.addfile(ti, io.BytesIO(new_manifest_bytes))
                    continue
                if p.is_file():
                    ti = fpk.gettarinfo(str(p), arcname=item)
                    ti.mode = 0o755 if "cmd" in item or item == "manifest" else 0o644
                    ti.mtime = FIXED_MTIME
                    with p.open("rb") as f:
                        fpk.addfile(ti, f)
                elif p.is_dir():
                    ti = fpk.gettarinfo(str(p), arcname=item)
                    ti.mode = 0o755
                    ti.mtime = FIXED_MTIME
                    fpk.addfile(ti)
                    for sub in sorted(p.rglob("*")):
                        rel = sub.relative_to(PKG_DIR).as_posix()
                        sti = fpk.gettarinfo(str(sub), arcname=rel)
                        sti.mtime = FIXED_MTIME
                        if sub.is_file():
                            sti.mode = 0o755 if "cmd" in rel else 0o644
                            with sub.open("rb") as sf:
                                fpk.addfile(sti, sf)
                        elif sub.is_dir():
                            sti.mode = 0o755
                            fpk.addfile(sti)

    print(f"Package built successfully: {output_path.name} ({output_path.stat().st_size} bytes)")
    import shutil
    root_fpk = ROOT / f"{appname}.fpk"
    if output_path != root_fpk:
        shutil.copyfile(output_path, root_fpk)
        print(f"Synced to: {root_fpk.name} ({root_fpk.stat().st_size} bytes)")
    legacy_fpk = ROOT / "fn-music-rebuild.fpk"
    if legacy_fpk.exists():
        legacy_fpk.unlink(missing_ok=True)
    return output_path


if __name__ == "__main__":
    build_fpk()
