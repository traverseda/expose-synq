"""Path confinement and visibility rules for the exposed workspace.

Every remote path goes through :meth:`PathPolicy.resolve` before it touches
the filesystem. Remote paths are always relative to the workspace root.
"""
from dataclasses import dataclass, field
from pathlib import Path

import pathspec

from synq_expose.config import ExposeConfig


# Characters with meaning in zenoh key expressions and selectors.
KEY_RESERVED = frozenset("*$#?")


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
        patterns = list(config.exclude)
        if config.gitignore:
            # TODO: nested .gitignore files, and re-reading when they change.
            gitignore = config.workspace / ".gitignore"
            if gitignore.is_file():
                patterns += gitignore.read_text().splitlines()
        return cls(
            root=config.workspace.resolve(),
            exclude=pathspec.GitIgnoreSpec.from_lines(patterns),
            readonly=pathspec.GitIgnoreSpec.from_lines(config.readonly_paths),
            allow_write=config.allow_write,
        )

    def resolve(self, remote_path: str, *, write: bool = False) -> Path:
        """Map a workspace-relative path to an absolute host path, or raise PolicyError.

        Paths are meant to be carried in zenoh keys and checked by zenoh ACLs
        (see fs.py), so a path must name exactly the file its key names. That
        rules out ``.``/``..``/empty segments, wildcard characters, and any
        symlink along the way, each of which could lead outside what an ACL
        rule on that key allows.
        """
        stripped = remote_path.strip("/")
        parts = stripped.split("/") if stripped not in ("", ".") else []
        for part in parts:
            if part in ("", ".", "..") or any(c in part for c in KEY_RESERVED):
                raise PolicyError(f"{remote_path!r}: invalid path segment {part!r}")
        lexical = self.root.joinpath(*parts)
        if lexical.resolve() != lexical:
            raise PolicyError(f"{remote_path!r} goes through a symlink")
        target = lexical
        rel = target.relative_to(self.root).as_posix()
        if rel != "." and self.exclude.match_file(rel):
            raise PolicyError(f"{remote_path!r} is not exposed")
        if write:
            if not self.allow_write:
                raise PolicyError("workspace is read-only")
            if rel != "." and self.readonly.match_file(rel):
                raise PolicyError(f"{remote_path!r} is read-only")
        return target

    def is_visible(self, path: Path) -> bool:
        """Whether a host path under the root should show up in listings/globs."""
        rel = path.relative_to(self.root).as_posix()
        return not self.exclude.match_file(rel)
