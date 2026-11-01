"""Host-independent lexical project identity for native persistence adapters."""

from pathlib import PurePosixPath, PureWindowsPath


def _project_path_key(value: object) -> tuple[str, ...] | None:
    """Compare absolute paths lexically, without consulting this host's filesystem."""
    if (
        not isinstance(value, str)
        or not value
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        return None
    windows = PureWindowsPath(value)
    if windows.drive:
        # Only drive-rooted paths: reject UNC/device namespaces and drive-relative paths.
        if (
            len(windows.drive) != 2
            or windows.drive[1] != ":"
            or not windows.is_absolute()
        ):
            return None
        parts = windows.parts
        if (
            ".." in parts
            or not windows.drive[0].isascii()
            or not windows.drive[0].isalpha()
        ):
            return None
        return ("windows", windows.drive.lower(), *parts[1:])
    if not value.startswith("/") or value.startswith("//") or "\\" in value:
        return None
    path = PurePosixPath(value)
    if ".." in path.parts:
        return None
    return ("posix", *path.parts)
