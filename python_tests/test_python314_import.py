# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

import subprocess
import sys

import pytest


@pytest.mark.skipif(sys.version_info < (3, 14), reason="Python 3.14 regression")
def test_registers_abstract_memo_class_without_crashing():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from abc import ABC, abstractmethod\n"
            "import dbzero as db0\n"
            "@db0.memo\n"
            "class AbstractMemo(ABC):\n"
            "    @abstractmethod\n"
            "    def value(self):\n"
            "        pass\n",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
