from __future__ import annotations

import io
import os
import shlex
import tempfile
import zipfile
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from ..logging import get_logger

logger = get_logger(__name__)

__all__ = [
    "ZipTooLargeError",
    "deduplicate_target",
    "default_upload_name",
    "default_upload_path",
    "deny_reason",
    "file_usage",
    "format_bytes",
    "format_image_prompt_annotation",
    "image_upload_path",
    "is_image_document",
    "normalize_relative_path",
    "parse_file_command",
    "parse_file_prompt",
    "resolve_path_within_root",
    "split_command_args",
    "write_bytes_atomic",
    "zip_directory",
]

_IMAGE_EXTENSIONS = frozenset(
    {
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".bmp",
        ".heic",
        ".heif",
        ".tif",
        ".tiff",
    }
)


def is_image_document(
    *,
    mime_type: str | None,
    file_name: str | None,
    file_path: str | None = None,
    raw: Mapping[str, Any] | None = None,
) -> bool:
    """Return True for Telegram photos and image/* documents."""
    if isinstance(mime_type, str) and mime_type.lower().startswith("image/"):
        return True
    for candidate in (file_name, file_path):
        if not candidate:
            continue
        if Path(candidate).suffix.lower() in _IMAGE_EXTENSIONS:
            return True
    # Telegram photos are PhotoSize objects: no name/mime, but have width/height.
    return bool(
        raw
        and file_name is None
        and mime_type is None
        and "width" in raw
        and "height" in raw
    )


def image_upload_path(
    uploads_dir: str,
    image_subdir: str,
    filename: str | None,
    file_path: str | None,
) -> Path:
    """Unique path under uploads_dir/image_subdir for agent-visible images."""
    name = default_upload_name(filename, file_path)
    suffix = Path(name).suffix.lower()
    if suffix not in _IMAGE_EXTENSIONS:
        # Telegram photo downloads often end in .jpg
        if file_path and Path(file_path).suffix.lower() in _IMAGE_EXTENSIONS:
            suffix = Path(file_path).suffix.lower()
        else:
            suffix = ".jpg"
        stem = Path(name).stem or "photo"
        name = f"{stem}{suffix}"
    unique = f"{Path(name).stem}_{uuid4().hex[:8]}{Path(name).suffix}"
    return Path(uploads_dir) / image_subdir / unique


def format_image_prompt_annotation(rel_paths: Sequence[str]) -> str:
    paths = [p for p in rel_paths if p]
    if not paths:
        return ""
    if len(paths) == 1:
        return (
            f"[image]\n"
            f"- {paths[0]}\n\n"
            f"Read the image file above and answer based on what you see."
        )
    body = "\n".join(f"- {path}" for path in paths)
    return (
        f"[images]\n{body}\n\n"
        f"Read the image files above and answer based on what you see."
    )


def split_command_args(text: str) -> tuple[str, ...]:
    if not text.strip():
        return ()
    try:
        return tuple(shlex.split(text))
    except ValueError:
        return tuple(text.split())


def file_usage() -> str:
    return "usage: `/file put <path>` or `/file get <path>`"


def parse_file_command(args_text: str) -> tuple[str | None, str, str | None]:
    tokens = split_command_args(args_text)
    if not tokens:
        return None, "", file_usage()
    command = tokens[0].lower()
    rest = " ".join(tokens[1:]).strip()
    if command not in {"put", "get"}:
        return None, rest, file_usage()
    return command, rest, None


def parse_file_prompt(
    prompt: str, *, allow_empty: bool
) -> tuple[str | None, bool, str | None]:
    tokens = split_command_args(prompt)
    force = False
    parts: list[str] = []
    for token in tokens:
        if token == "--force":
            force = True
            continue
        if token.startswith("--"):
            return None, force, f"unknown flag: {token}"
        parts.append(token)
    path = " ".join(parts).strip()
    if not path and not allow_empty:
        return None, force, "missing path"
    return (path or None), force, None


def normalize_relative_path(value: str) -> Path | None:
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.startswith("~"):
        return None
    path = Path(cleaned)
    if path.is_absolute():
        return None
    parts = [part for part in path.parts if part not in {"", "."}]
    if not parts:
        return None
    if ".." in parts:
        return None
    if ".git" in parts:
        return None
    return Path(*parts)


def resolve_path_within_root(root: Path, rel_path: Path) -> Path | None:
    root_resolved = root.resolve(strict=False)
    target = (root / rel_path).resolve(strict=False)
    if not target.is_relative_to(root_resolved):
        return None
    return target


def deny_reason(rel_path: Path, deny_globs: Sequence[str]) -> str | None:
    if ".git" in rel_path.parts:
        return ".git/**"
    posix = PurePosixPath(rel_path.as_posix())
    for pattern in deny_globs:
        if posix.match(pattern):
            return pattern
    return None


def format_bytes(value: int) -> str:
    size = max(0.0, float(value))
    units = ("b", "kb", "mb", "gb", "tb")
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "b":
                return f"{int(size)} b"
            if size < 10:
                return f"{size:.1f} {unit}"
            return f"{size:.0f} {unit}"
        size /= 1024
    return f"{int(size)} B"


def default_upload_name(filename: str | None, file_path: str | None) -> str:
    name = Path(filename or "").name
    if not name and file_path:
        name = Path(file_path).name
    if not name:
        name = "upload.bin"
    return name


def default_upload_path(
    uploads_dir: str, filename: str | None, file_path: str | None
) -> Path:
    return Path(uploads_dir) / default_upload_name(filename, file_path)


def deduplicate_target(target: Path) -> Path:
    """Return *target* if it doesn't exist, otherwise append ``_1``, ``_2``, … before the extension."""
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    parent = target.parent
    for i in range(1, 1000):
        candidate = parent / f"{stem}_{i}{suffix}"
        if not candidate.exists():
            logger.info(
                "file.deduplicate",
                original=str(target),
                renamed=str(candidate),
            )
            return candidate
    raise FileExistsError(f"no available deduplicated path for {target}")


def write_bytes_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb", delete=False, dir=path.parent, prefix=".untether-upload-"
    ) as handle:
        handle.write(payload)
        temp_name = handle.name
    Path(temp_name).replace(path)


class ZipTooLargeError(Exception):
    pass


def zip_directory(
    root: Path,
    rel_path: Path,
    deny_globs: Sequence[str],
    *,
    max_bytes: int | None = None,
) -> bytes:
    target = root / rel_path
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for dirpath, _, filenames in os.walk(target, followlinks=False):
            dir_path = Path(dirpath)
            for filename in filenames:
                item = dir_path / filename
                if item.is_symlink():
                    continue
                if not item.is_file():
                    continue
                rel_item = rel_path / item.relative_to(target)
                if deny_reason(rel_item, deny_globs) is not None:
                    continue
                archive.write(item, arcname=rel_item.as_posix())
                if max_bytes is not None and buffer.tell() > max_bytes:
                    logger.debug(
                        "file.zip_too_large",
                        max_bytes=max_bytes,
                        actual_bytes=buffer.tell(),
                        path=str(rel_path),
                    )
                    raise ZipTooLargeError()
    payload = buffer.getvalue()
    if max_bytes is not None and len(payload) > max_bytes:
        logger.debug(
            "file.zip_too_large",
            max_bytes=max_bytes,
            actual_bytes=len(payload),
            path=str(rel_path),
        )
        raise ZipTooLargeError()
    return payload
