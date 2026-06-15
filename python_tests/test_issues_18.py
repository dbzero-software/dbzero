# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import os
import subprocess
import sys
import textwrap

import pytest


SHUTDOWN_LIFETIME_SKIP_REASON = (
    "Known shutdown-lifetime crash in subprocesses that leave singleton-backed durable "
    "collections with nested immutable memo references alive at interpreter teardown; "
    "currently observed on Python 3.12/3.13 and macOS builds only."
)


@pytest.mark.skip(reason=SHUTDOWN_LIFETIME_SKIP_REASON)
def test_unhandled_exception_with_nested_durable_list_does_not_segfault(tmp_path):
    """Regression for SIGSEGV during interpreter shutdown after an exception."""
    model_path = tmp_path / "repro_model.py"
    model_path.write_text(
        textwrap.dedent(
            """
            from __future__ import annotations

            from dataclasses import dataclass, field

            import dbzero as db0

            DATA_PREFIX = "/issue-18/data"


            @db0.memo(prefix=DATA_PREFIX, immutable=True)
            @dataclass
            class Metadata:
                title: str
                subtitle: str
                parent: LegalExcerptRecord | int
                url: str
                source: str


            @db0.memo(prefix=DATA_PREFIX, immutable=True)
            @dataclass
            class LegalExcerptRecord:
                id: int
                content: str
                metadata: Metadata

                @classmethod
                def from_schema_data(cls, data, parent=None):
                    metadata = Metadata.from_schema_data(data["metadata"], parent=parent)
                    return cls(int(data["id"]), data["content"], metadata)


            @classmethod
            def metadata_from_schema_data(cls, data, parent=None):
                parent_id = int(data["parentId"])
                return cls(
                    data["title"],
                    data["subtitle"],
                    parent_id if parent is None else parent,
                    data["url"],
                    data["source"],
                )


            Metadata.from_schema_data = metadata_from_schema_data


            @db0.memo(prefix=DATA_PREFIX, singleton=True)
            @dataclass
            class Root:
                records: list[LegalExcerptRecord] = field(default_factory=list)
                records_by_id: dict[str, LegalExcerptRecord] = field(default_factory=dict)
                record_positions_by_id: dict[str, int] = field(default_factory=dict)
                unresolved_child_ids_by_parent_id: dict[str, list[str]] = field(default_factory=dict)

                def add_record_from_schema_data(self, data):
                    parent_id = int(data["metadata"]["parentId"])
                    existing_parent = self.get_record(parent_id)
                    parent = existing_parent if existing_parent is not None else parent_id
                    record = LegalExcerptRecord.from_schema_data(data, parent=parent)
                    self.store_record(record)
                    return record

                def store_record(self, record):
                    record_key = str(record.id)
                    self._remove_unresolved_child(record_key)
                    if isinstance(record.metadata.parent, int):
                        self._add_unresolved_child(record_key, record.metadata.parent)

                    position = self.record_positions_by_id.get(record_key)
                    if position is None:
                        self.record_positions_by_id[record_key] = len(self.records)
                        self.records.append(record)
                    else:
                        self.records[position] = record
                    self.records_by_id[record_key] = record

                def get_record(self, record_id):
                    return self.records_by_id.get(str(record_id))

                def _add_unresolved_child(self, child_key, parent_id):
                    parent_key = str(parent_id)
                    child_ids = self.unresolved_child_ids_by_parent_id.setdefault(parent_key, [])
                    if child_key not in child_ids:
                        child_ids.append(child_key)

                def _remove_unresolved_child(self, child_key):
                    for _parent_key, child_ids in self.unresolved_child_ids_by_parent_id.items():
                        child_ids.discard(child_key)
            """
        ),
        encoding="utf-8",
    )

    script = textwrap.dedent(
        f"""
        import sys
        import tempfile

        import dbzero as db0

        sys.path.insert(0, {str(tmp_path)!r})

        from repro_model import DATA_PREFIX, Root

        def record_data(record_id):
            return {{
                "id": str(record_id),
                "content": "Legal text excerpt body.",
                "metadata": {{
                    "title": "Legal act title",
                    "subtitle": "Legal act subtitle",
                    "parentId": "14",
                    "url": "data/html/DU_1919_364.html",
                    "source": "Art. 1.",
                }},
            }}

        db0.init(tempfile.mkdtemp() + "/dbzero", prefix=DATA_PREFIX, autocommit=True)
        root = Root()
        root.add_record_from_schema_data(record_data(19458))
        root.add_record_from_schema_data(record_data(19459))
        """
    )

    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        env=env,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 1, (
        f"subprocess exited with {result.returncode}; expected ordinary AttributeError exit\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    assert "AttributeError" in result.stderr
