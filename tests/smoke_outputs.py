#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from PIL import Image


IMAGE_EXTS = {".png", ".jpg", ".jpeg", ".webp"}
MIN_DIMENSION = 100
MAX_DIMENSION_DEFAULT = 900
MAX_DIMENSION_SLACK = 10


def _parse_int(raw: Optional[str], default: int) -> int:
    try:
        return int(str(raw).strip()) if raw is not None and str(raw).strip() else default
    except (TypeError, ValueError):
        return default


def _parse_utc(raw: Optional[str]) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _run_id_from_init_utc(raw: Optional[str]) -> Optional[str]:
    dt = _parse_utc(raw)
    if dt is None:
        return None
    return dt.strftime("%Y%m%d%H")


def _collect_images(run_dir: Path) -> List[Path]:
    files: List[Path] = []
    for path in run_dir.rglob("*"):
        if path.is_file() and path.suffix.lower() in IMAGE_EXTS:
            files.append(path)
    files.sort()
    return files


def _newest_image_mtime(images: List[Path]) -> float:
    return max((p.stat().st_mtime for p in images), default=0.0)


def _load_manifest_runs(manifest_path: Path) -> List[dict]:
    if not manifest_path.exists():
        return []
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict) and isinstance(payload.get("runs"), list):
        return [r for r in payload["runs"] if isinstance(r, dict)]
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    return []


def _pick_run_dir(runs_root: Path, run_images: Dict[str, List[Path]]) -> Tuple[Path, List[Path], str]:
    candidates = {name: runs_root / name for name in run_images.keys()}
    if not candidates:
        raise RuntimeError("No run directories with images were found under public/runs.")

    run_id_env = str(os.environ.get("RUN_ID") or "").strip()
    init_run_id = _run_id_from_init_utc(os.environ.get("RUN_INIT_UTC"))
    for run_id in (run_id_env, init_run_id):
        if run_id and run_id in candidates:
            return candidates[run_id], run_images[run_id], "env"

    manifest_runs = _load_manifest_runs(Path("public") / "runs_manifest.json")
    best_name: Optional[str] = None
    best_score: Optional[Tuple[datetime, datetime, str]] = None
    for item in manifest_runs:
        run_name = str(item.get("id") or "").strip()
        if not run_name or run_name not in candidates:
            continue
        init_utc = _parse_utc(item.get("init_utc")) or datetime.min.replace(tzinfo=timezone.utc)
        generated_utc = _parse_utc(item.get("generated_utc")) or datetime.min.replace(tzinfo=timezone.utc)
        score = (init_utc, generated_utc, run_name)
        if best_score is None or score > best_score:
            best_score = score
            best_name = run_name
    if best_name is not None:
        return candidates[best_name], run_images[best_name], "manifest"

    newest_name = max(
        run_images.keys(),
        key=lambda name: (_newest_image_mtime(run_images[name]), name),
    )
    return candidates[newest_name], run_images[newest_name], "image_mtime"


def _validate_images(images: Iterable[Path], max_dimension: int) -> Tuple[int, int, int]:
    unreadable: List[str] = []
    too_small: List[str] = []
    too_large: List[str] = []
    max_w = 0
    max_h = 0
    max_side_seen = 0
    max_allowed = int(max_dimension) + MAX_DIMENSION_SLACK

    count = 0
    for img_path in images:
        count += 1
        try:
            with Image.open(img_path) as im:
                im.load()
                width, height = im.size
        except Exception as exc:  # pragma: no cover - this is the smoke gate path
            unreadable.append(f"{img_path}: {exc}")
            continue

        max_w = max(max_w, int(width))
        max_h = max(max_h, int(height))
        max_side_seen = max(max_side_seen, int(max(width, height)))

        if min(width, height) < MIN_DIMENSION:
            too_small.append(f"{img_path} ({width}x{height})")
        if max(width, height) > max_allowed:
            too_large.append(f"{img_path} ({width}x{height}) > {max_allowed}")

    if count == 0:
        raise RuntimeError("No images found in selected run directory.")

    errors: List[str] = []
    if unreadable:
        errors.append("Unreadable images:\n" + "\n".join(unreadable[:10]))
    if too_small:
        errors.append("Images below minimum dimension:\n" + "\n".join(too_small[:10]))
    if too_large:
        errors.append("Images above max dimension policy:\n" + "\n".join(too_large[:10]))
    if errors:
        raise RuntimeError("\n\n".join(errors))

    return count, max_w, max_h


def main() -> int:
    runs_root = Path("public") / "runs"
    if not runs_root.exists():
        raise RuntimeError("public/runs does not exist.")

    run_images: Dict[str, List[Path]] = {}
    for run_dir in sorted([p for p in runs_root.iterdir() if p.is_dir()]):
        imgs = _collect_images(run_dir)
        if imgs:
            run_images[run_dir.name] = imgs

    chosen_dir, images, picked_by = _pick_run_dir(runs_root, run_images)
    max_dimension = _parse_int(os.environ.get("WN2_MAX_DIMENSION"), MAX_DIMENSION_DEFAULT)
    count, max_w, max_h = _validate_images(images, max_dimension=max_dimension)

    rel_dir = chosen_dir.as_posix()
    print(
        f"SMOKE OK: run_dir={rel_dir} picked_by={picked_by} "
        f"images={count} max_observed={max_w}x{max_h} limit={max_dimension + MAX_DIMENSION_SLACK}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"SMOKE FAIL: {exc}", file=sys.stderr)
        raise SystemExit(1)
