# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import gc
import tracemalloc

import pytest
import dbzero as db0


def _retained_bytes(before: tracemalloc.Snapshot, after: tracemalloc.Snapshot) -> int:
    return sum(
        stat.size_diff
        for stat in after.compare_to(before, "lineno")
        if stat.size_diff > 0
    )


@pytest.mark.stress_test
def test_filter_find_pending_tasks_mock_jobs_does_not_leak_memory(db0_small_lang_cache_small_cache):
    JobStatus = db0.enum(
        "FilterLeakJobStatus",
        ["READY", "WARMING_UP", "STARTED", "DONE"],
    )

    @db0.memo
    class Job:
        def __init__(self, name, status):
            self.name = name
            self.status = None
            self.set_status(status)

        def set_status(self, status):
            if self.status is not None:
                db0.tags(self).remove(self.status)
            db0.tags(self).add(status)
            self.status = status

    active_statuses = [JobStatus.READY, JobStatus.WARMING_UP, JobStatus.STARTED]
    all_statuses = [*active_statuses, JobStatus.DONE]
    [Job(f"job-{i}", all_statuses[i % len(all_statuses)]) for i in range(128)]
    jobs = list(db0.find(Job))
    pending_tasks = {
        job: object()
        for index, job in enumerate(jobs)
        if job.status in active_statuses and index % 3 == 0
    }
    expected = [
        job for job in jobs
        if job.status in active_statuses and job not in pending_tasks
    ]

    def run_query():
        ready_or_started_jobs = db0.filter(
            lambda found_job: found_job not in pending_tasks,
            db0.find(Job, [JobStatus.READY, JobStatus.WARMING_UP, JobStatus.STARTED]),
        )
        return list(ready_or_started_jobs)

    for _ in range(50):
        assert run_query() == expected

    gc.collect()
    tracemalloc.start(25)
    iterations = 200000
    try:
        before = tracemalloc.take_snapshot()
        for iteration in range(iterations):
            assert run_query() == expected
            if iteration % 50 == 49:
                gc.collect()
            if iteration % 1000 == 999:
                print(f"Completed iteration {iteration + 1}/{iterations}...")
        gc.collect()
        after = tracemalloc.take_snapshot()
    finally:
        tracemalloc.stop()

    retained_bytes = _retained_bytes(before, after)
    print(f"Retained {retained_bytes / (1024 * 1024):.3f} MB across 500 iterations of db0.filter(lambda found_job: found_job not in pending_tasks, "
          f"db0.find(Job, [JobStatus.READY, JobStatus.WARMING_UP, JobStatus.STARTED]))\n Memory growth by iteration: {retained_bytes / iterations:.3f} bytes/iteration")
    assert retained_bytes <= 512 * 1024, (
        "Repeated db0.filter(lambda found_job: found_job not in pending_tasks, "
        "db0.find(Job, [JobStatus.READY, JobStatus.WARMING_UP, JobStatus.STARTED])) "
        f"retained too much Python memory: {retained_bytes} bytes"
    )
