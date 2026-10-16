"""Path confinement and visibility rules for the exposed workspace.

Every remote path goes through :meth:`PathPolicy.resolve` before it touches
the filesystem. Remote paths are always relative to the workspace root.
"""
from dataclasses import dataclass, field
from pathlib import Path

import pathspec

from synq_expose.config import ExposeConfig


class PolicyError(PermissionError):
    """A remote path is outside the workspace, hidden, or read-only."""


@dataclass
class PathPolicy:
    root: Path
    exclude: pathspec.PathSpec = field(default_factory=lambda: pathspec.PathSpec([]))
    readonly: pathspec.PathSpec = field(default_factory=lambda: pathspec.PathSpec([]))
    allow_write: bool = False

    @classmethod
    def from_config(cls, config: ExposeConfig) -> "PathPolicy":
        return cls(
            root=config.workspace.resolve(),
            exclude=pathspec.GitIgnoreSpec.from_lines(config.exclude),
            readonly=pathspec.GitIgnoreSpec.from_lines(config.readonly_paths),
            allow_write=config.allow_write,
        )

    def resolve(self, remote_path: str, *, write: bool = False, nofollow: bool = False,
                is_dir: bool | None = None) -> Path:
        """Map a workspace-relative path to an absolute host path, or raise PolicyError.

        ``..`` segments and symlinks along the way are refused: either could
        lead outside the workspace, or to an excluded or read-only path under
        a name that isn't.

        ``nofollow`` allows the last segment to be a symlink, for calls that
        act on the link itself (lstat, unlink). ``is_dir`` says whether the
        target is a directory for pattern matching, when it doesn't exist yet.
        """
        parts = [part for part in remote_path.split("/") if part not in ("", ".")]
        if ".." in parts:
            raise PolicyError(f"{remote_path!r}: '..' isn't allowed")
        lexical = self.root.joinpath(*parts)
        checked = lexical.parent if nofollow and parts else lexical
        if checked.resolve() != checked:
            raise PolicyError(f"{remote_path!r} goes through a symlink")
        if not self.is_visible(lexical, is_dir):
            raise PolicyError(f"{remote_path!r} is not exposed")
        if write and not self.is_writable(lexical, is_dir):
            raise PolicyError("workspace is read-only" if not self.allow_write else f"{remote_path!r} is read-only")
        return lexical

    def _match_key(self, path: Path, is_dir: bool | None) -> str | None:
        """``path`` relative to the root as gitignore patterns see it (directories end in ``/``), or None for the root."""
        if path == self.root:
            return None
        if is_dir is None:
            is_dir = path.is_dir() and not path.is_symlink()
        rel = path.relative_to(self.root).as_posix()
        return rel + "/" if is_dir else rel

    def is_visible(self, path: Path, is_dir: bool | None = None) -> bool:
        """Whether a host path under the root is exposed, and should show up in listings/globs."""
        key = self._match_key(path, is_dir)
        return key is None or not self.exclude.match_file(key)

    def is_writable(self, path: Path, is_dir: bool | None = None) -> bool:
        """Whether a host path under the root may be changed. Doesn't check visibility."""
        key = self._match_key(path, is_dir)
        return self.allow_write and (key is None or not self.readonly.match_file(key))
