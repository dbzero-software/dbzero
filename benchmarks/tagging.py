#!/usr/bin/env python3
"""Benchmark db0 tag assignment throughput.

The benchmark creates memo objects in a preparation step, commits them, and
pre-seeds a regular anchor tag before timing starts. The measured loop applies
fresh simple tags to a fixed batch of existing objects and commits after every
batch, so the reported throughput includes flushing pending tag index updates.

Scenarios:
- fresh-tags: creates a new tag key for every measured batch; this measures tag
  key creation, inverted-list creation, assignment, and flush cost together.
- precreated-tags: creates and flushes tag keys before timing starts, then uses
  those existing keys once during timing; this focuses on assignment and flush
  cost without string-pool/tag-key creation.

Observed on this workspace:
- CPU: 11th Gen Intel(R) Core(TM) i9-11950H @ 2.60GHz
- Python: 3.11.13
- Build: release
- Commands:
  python3 benchmarks/tagging.py --scenario fresh-tags --target-seconds 30
  python3 benchmarks/tagging.py --scenario precreated-tags --target-seconds 30
- Current result, fresh-tags:
  object_count=10000
  batch_size=1000
  tags_per_batch=1
  batches=1442
  elapsed_seconds=30.012787
  tag_assignments=1442000
  tag_assignments_per_second=48046.187
- Current result, precreated-tags:
  object_count=10000
  batch_size=1000
  tags_per_batch=1
  precreated_tag_count=5000
  batches=1334
  elapsed_seconds=30.006962
  tag_assignments=1334000
  tag_assignments_per_second=44456.349
"""

import argparse
import gc
import os
import platform
import tempfile
import time

import dbzero as db0


@db0.memo
class TagBenchmarkMemo:
    def __init__(self, value):
        self.value = value


def read_cpu_model():
    try:
        with open("/proc/cpuinfo", "r", encoding="utf-8") as cpuinfo:
            for line in cpuinfo:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown"


def prepare_objects(object_count, batch_size):
    objects = [TagBenchmarkMemo(i) for i in range(object_count)]
    db0.commit()
    target_objects = objects[:batch_size]
    db0.tags(*target_objects).add("tagging-benchmark-anchor")
    db0.commit()
    return objects, target_objects


def prepare_precreated_tags(tag_count):
    tag_owner = TagBenchmarkMemo(-1)
    db0.commit()
    tags = [
        f"precreated-tag-{index}"
        for index in range(tag_count)
    ]
    db0.tags(tag_owner).add(tags)
    db0.commit()
    return tag_owner, tags


def apply_tag_batch(target_objects, tags):
    db0.tags(*target_objects).add(tags)
    db0.commit()
    return len(target_objects) * len(tags)


def fresh_tag_batch(batch_index, tags_per_batch):
    return [
        f"fresh-tag-{batch_index}-{index}"
        for index in range(tags_per_batch)
    ]


def precreated_tag_batch(tags, batch_index, tags_per_batch):
    start = batch_index * tags_per_batch
    end = start + tags_per_batch
    if end > len(tags):
        return None
    return tags[start:end]


def measure(target_objects, target_seconds, tags_per_batch, tag_batch_factory):
    total_assignments = 0
    batches = 0
    exhausted_tags = False
    gc_was_enabled = gc.isenabled()
    gc.disable()
    try:
        start = time.perf_counter()
        deadline = start + target_seconds
        while True:
            tags = tag_batch_factory(batches)
            if tags is None:
                exhausted_tags = True
                break
            total_assignments += apply_tag_batch(target_objects, tags)
            batches += 1
            if time.perf_counter() >= deadline:
                break
        elapsed = time.perf_counter() - start
    finally:
        if gc_was_enabled:
            gc.enable()
    return elapsed, total_assignments, batches, exhausted_tags


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scenario",
        choices=("fresh-tags", "precreated-tags"),
        default="fresh-tags",
    )
    parser.add_argument("--target-seconds", type=float, default=30.0)
    parser.add_argument("--object-count", type=int, default=10000)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--tags-per-batch", type=int, default=1)
    parser.add_argument(
        "--precreated-tag-count",
        type=int,
        default=5000,
        help="number of tag keys to create before timing in the precreated-tags scenario",
    )
    args = parser.parse_args()

    if args.object_count < args.batch_size:
        parser.error("--object-count must be greater than or equal to --batch-size")
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    if args.tags_per_batch <= 0:
        parser.error("--tags-per-batch must be positive")
    if args.precreated_tag_count <= 0:
        parser.error("--precreated-tag-count must be positive")

    with tempfile.TemporaryDirectory() as root:
        db0.init(root)
        db0.open("tagging-throughput-benchmark")
        objects, target_objects = prepare_objects(args.object_count, args.batch_size)
        precreated_tag_owner = None
        precreated_tags = None
        if args.scenario == "precreated-tags":
            if args.precreated_tag_count < args.tags_per_batch:
                parser.error("--precreated-tag-count must cover at least one measured batch")
            precreated_tag_owner, precreated_tags = prepare_precreated_tags(
                args.precreated_tag_count
            )

        if args.scenario == "fresh-tags":
            tag_batch_factory = lambda batch: fresh_tag_batch(
                batch,
                args.tags_per_batch,
            )
        else:
            tag_batch_factory = lambda batch: precreated_tag_batch(
                precreated_tags,
                batch,
                args.tags_per_batch,
            )

        elapsed, assignments, batches, exhausted_tags = measure(
            target_objects,
            args.target_seconds,
            args.tags_per_batch,
            tag_batch_factory,
        )

        print(f"cpu={read_cpu_model()}")
        print(f"python={platform.python_version()}")
        print(f"build_flags={db0.build_flags()}")
        print(f"pid={os.getpid()}")
        print(f"scenario={args.scenario}")
        print(f"object_count={len(objects)}")
        print(f"batch_size={args.batch_size}")
        print(f"tags_per_batch={args.tags_per_batch}")
        print(f"precreated_tag_count={len(precreated_tags) if precreated_tags is not None else 0}")
        print(f"precreated_tag_owner_kept={precreated_tag_owner is not None}")
        print(f"batches={batches}")
        print(f"elapsed_seconds={elapsed:.6f}")
        print(f"tag_assignments={assignments}")
        print(f"tag_assignments_per_second={assignments / elapsed if elapsed else 0:.3f}")
        print(f"seconds_per_batch={elapsed / batches if batches else 0:.6f}")
        print(f"exhausted_tags={exhausted_tags}")


if __name__ == "__main__":
    main()
