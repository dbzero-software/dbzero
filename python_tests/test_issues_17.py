# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import os
import subprocess
import sys
import textwrap


def test_statek_future_error_warmup_keeps_job_status_readable(tmp_path):
    """Focused repro for the statek FutureError warmup failure."""
    script = textwrap.dedent(
        f"""
        import asyncio
        import os
        from dataclasses import dataclass

        import dbzero as db0

        from statek.agents.agent import Agent
        from statek.exceptions import FutureError
        from statek.executors.job import Job, JobDef, JobStatus
        from statek.executors.utils import run_job_step
        from statek.future import FutureResult
        from statek.prompt_config import make_system_prompt

        db_path = {str(tmp_path / "db0")!r}
        os.mkdir(db_path)
        db0.init(db_path, read_write=True)
        db0.open("test_prefix", "rw")

        @db0.memo
        @dataclass
        class MemoObject:
            value: int = 0

        def check_condition_false(_):
            return False

        def fetch_result_not_ready(future_result):
            raise FutureError(future_result=future_result)

        future = FutureResult(deps=MemoObject(value=0), state_num=0)
        future.set_complement_functions(
            complement=fetch_result_not_ready,
            condition=check_condition_false,
        )

        agent = Agent(
            role="test",
            _system_prompt=make_system_prompt("Test"),
            _metadata={{"MODEL": "test-model"}},
            _tools=[],
        )
        job_def = JobDef(
            agent=agent,
            metadata={{"MODEL": "test-model"}},
            warmup_code=[
                "counter = counter + 1\\n"
                "before_flag = True\\n"
                "result = future_val\\n"
                "after_flag = True",
                'exit("ok")',
            ],
        )
        job = Job(
            job_def=job_def,
            model_family="test",
            model="test-model",
            job_status=JobStatus.READY,
        )
        job.py_env.local_state["counter"] = 0
        job.py_env.local_state["before_flag"] = False
        job.py_env.local_state["after_flag"] = False
        job.py_env.local_state["future_val"] = future

        result = asyncio.run(run_job_step(job))
        assert result is False
        assert job.status == JobStatus.WARMING_UP
        assert job.awaited_result is future
        assert job.next_instr_num == 2

        db0.close()
        """
    )

    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONPATH"] = "/src/statek" + os.pathsep + env.get("PYTHONPATH", "")

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        env=env,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, (
        f"subprocess exited with {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
