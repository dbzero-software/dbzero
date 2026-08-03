# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

import os
import subprocess
import sys
import textwrap


def test_repeated_memo_graph_creation_with_tagged_shared_lookup_does_not_segfault(tmp_path):
    """Regression for repeated memo graph construction with tagged shared lookup."""
    script = textwrap.dedent(
        f"""
        from dataclasses import dataclass
        from decimal import Decimal
        from typing import Optional
        import os

        import dbzero as db0

        db_path = {str(tmp_path / "db0")!r}
        os.mkdir(db_path)
        db0.init(db_path)
        db0.open("p")

        JobStatus = db0.enum("Issue16JobStatus", ["READY", "DONE"])

        @db0.memo
        @dataclass
        class Pricing:
            input_price_per_M: Optional[Decimal] = None
            input_price_per_cached_M: Optional[Decimal] = None
            output_price_per_M: Optional[Decimal] = None

        @db0.memo(no_default_tags=True)
        @dataclass
        class Usage:
            pricing: Pricing
            context_bytes: int = 0
            total_bytes_sent: int = 0
            total_bytes_received: int = 0
            total_input_tokens: int = 0
            total_cached_tokens: int = 0
            total_output_tokens: int = 0
            total_reported_cost: Optional[float] = None

        @db0.memo
        class RuntimeState:
            def __init__(self):
                self.local_state = {{}}
                self.console = []
                self.exceptions = None
                self.exit_status = None

        @db0.memo
        class Agent:
            def __init__(self, role, metadata, tools):
                self.role = role
                self._metadata = metadata
                self._tools = tools

        @db0.memo
        @dataclass
        class JobDef:
            agent: Agent
            metadata: dict = None
            job_params: dict = None
            warmup_code: object = None

            def __post_init__(self):
                if self.agent is not None:
                    db0.tags(self).add(self.agent)
                if self.metadata is None:
                    self.metadata = self.agent._metadata

        @db0.memo
        class Job:
            def __init__(self, job_def, job_status=JobStatus.READY):
                self.job_def = job_def
                self.parent_job = None
                if self.job_def.agent is not None:
                    db0.tags(self).add(self.job_def.agent)
                self.__job_status = None
                self.set_status(job_status)
                self.runtime_state = RuntimeState()
                self.chat_log = []
                self.awaited_result = None
                self.next_instr_num = None
                self.warmup_block_num = None
                self.error = None
                self.created_at = None
                self.error_handlers = []
                self.__last_difficulty = None
                self.usage = Usage(pricing=self.current_pricing())
                self.error = None
                self.num_completions = None
                self.__ext_ref = None
                self.__pending_chat_log = []

            def set_status(self, new_status):
                if self.__job_status is not None:
                    db0.tags(self).remove(self.__job_status)
                db0.tags(self).add(new_status)
                self.__job_status = new_status

            def current_pricing(self):
                existing = next(iter(db0.find(Pricing, "UNKNOWN", "test-model")), None)
                if existing is not None:
                    return existing
                pricing = Pricing()
                db0.tags(pricing).add(["UNKNOWN", "test-model", "USAGE"])
                return pricing

        def make_job():
            agent = Agent("test", {{"MODEL": "test-model"}}, [])
            job_def = JobDef(agent)
            return Job(job_def)

        make_job()
        make_job()
        db0.close()
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

    assert result.returncode == 0, (
        f"subprocess exited with {result.returncode}\\n"
        f"stdout:\\n{result.stdout}\\n"
        f"stderr:\\n{result.stderr}"
    )
