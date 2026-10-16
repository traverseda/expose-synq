"""The single chokepoint every remote call passes through: confirmation and audit.

Who may call at all is zenoh's job (TLS, auth, access_control in the zenoh config).
"""
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from synq_expose.config import ExposeConfig


@dataclass
class Guard:
    confirm: bool = False
    audit_log: Path | None = None

    @classmethod
    def from_config(cls, config: ExposeConfig) -> "Guard":
        return cls(confirm=config.confirm, audit_log=config.audit_log)

    def check(self, operation: str, **details: Any) -> None:
        """Allow one remote operation, or raise. Call first thing in every remote method."""
        if self.confirm and operation in CONFIRM_OPERATIONS:
            # TODO: RPCs arrive on zenoh callback threads; route prompts through
            # one queue so concurrent calls don't fight over the terminal.
            raise NotImplementedError("--confirm prompting")
        self.audit(operation, allowed=True, **details)

    def audit(self, operation: str, **fields: Any) -> None:
        if self.audit_log is None:
            return
        record = {"ts": time.time(), "op": operation, **fields}
        with self.audit_log.open("a") as f:
            f.write(json.dumps(record, default=str) + "\n")


# Operations that change something or run something; --confirm prompts for these.
CONFIRM_OPERATIONS = frozenset({
    "open_write", "edit_file", "rename", "chmod", "mkdir", "delete",
    "spawn",
})
