# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2026 DBZero Software sp. z o.o.

"""Regression coverage for set difference against memo-backed set fields."""

from __future__ import annotations

import subprocess
import sys
import textwrap

def test_python_set_difference_with_memo_set_field_does_not_segfault(tmp_path):
    script = textwrap.dedent(
        f"""
        from dataclasses import dataclass, field

        import dbzero as db0


        @db0.memo(prefix="/issue-18")
        @dataclass(eq=False)
        class Contact:
            tags: set[str] = field(default_factory=set)


        db0.init({str(tmp_path)!r}, prefix="/issue-18", autocommit=True)
        contact = Contact({{"lead", "technical"}})
        removed = {{"lead", "technical"}} - contact.tags
        assert removed == set()
        db0.close()
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, (
        f"set difference repro exited with {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
