#!/usr/bin/env python3
"""Benchmark regular db0 memo reads with read_only machinery compiled in.

This benchmark intentionally performs only plain db0 read operations outside a
read_only block. It answers whether the read_only mutation-check machinery
slows existing read-heavy code when read_only is not active.

Observed on this workspace:
- CPU: 11th Gen Intel(R) Core(TM) i9-11950H @ 2.60GHz (2.61 GHz)
- Python: 3.11.13
- Build: release, default async-safe read_only implementation, mutation-only atomic API guard
- Command:
  PYTHONPATH=/src/dev/dbzero python3 benchmarks/read_only_reads.py --target-seconds 30
- Current result:
  iterations=56095010
  elapsed_seconds=30.795000
  reads_per_second=1821562.283
  nanoseconds_per_read=548.979
- Previous recorded result:
  iterations=53910152
  elapsed_seconds=29.781574
  reads_per_second=1810184.778
  nanoseconds_per_read=552.430
"""

import argparse
import gc
import tempfile
import time

import dbzero as db0


@db0.memo
class ReadBenchmarkMemo:
    pass


def run_reads(obj, iterations):
    total = 0
    for _ in range(iterations):
        total += obj.value
    return total


def measure(obj, iterations):
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        start = time.perf_counter()
        total = run_reads(obj, iterations)
        elapsed = time.perf_counter() - start
    finally:
        if gc_was_enabled:
            gc.enable()
    return elapsed, total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int)
    parser.add_argument("--target-seconds", type=float, default=30.0)
    parser.add_argument("--calibration-seconds", type=float, default=2.0)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as root:
        db0.init(root)
        db0.open("read-only-overhead-benchmark")
        obj = ReadBenchmarkMemo()
        obj.value = 1
        object_id = db0.uuid(obj)
        db0.commit()
        obj = db0.fetch(object_id)

        run_reads(obj, 10000)
        if args.iterations is None:
            calibration_iterations = 10000
            elapsed = 0.0
            while elapsed < args.calibration_seconds:
                calibration_iterations *= 2
                elapsed, _ = measure(obj, calibration_iterations)
            iterations = max(1, int(calibration_iterations * args.target_seconds / elapsed))
        else:
            iterations = args.iterations

        elapsed, total = measure(obj, iterations)
        print(f"build_flags={db0.build_flags()}")
        print(f"iterations={iterations}")
        print(f"elapsed_seconds={elapsed:.6f}")
        print(f"reads_per_second={iterations / elapsed:.3f}")
        print(f"nanoseconds_per_read={elapsed * 1_000_000_000 / iterations:.3f}")
        print(f"checksum={total}")


if __name__ == "__main__":
    main()
