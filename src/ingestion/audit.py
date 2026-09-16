import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class AuditLogger:

    def __init__(
        self,
        audit_base_path: str = "data/audit",
    ):
        self.audit_base_path = Path(
            audit_base_path
        )

        self.audit_base_path.mkdir(
            parents=True,
            exist_ok=True,
        )

    def write(
        self,
        pipeline_run_id: str,
        status: str,
        details: dict[str, Any],
    ) -> None:

        audit_record = {
            "pipeline_run_id": pipeline_run_id,
            "status": status,
            "event_timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            **details,
        }

        audit_file = (
            self.audit_base_path
            / f"{pipeline_run_id}_{status.lower()}.json"
        )

        with audit_file.open(
            "w",
            encoding="utf-8",
        ) as file:

            json.dump(
                audit_record,
                file,
                indent=2,
            )