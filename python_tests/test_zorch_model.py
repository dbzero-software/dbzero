# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from dbzero import memo, no
from datetime import datetime
from itertools import islice


@memo(singleton=True)
class Zorch:
    def __init__(self, prefix = None):
        db0.set_prefix(self, prefix)
        # set of keys to prevent task duplication        
        self.keys = set()
        # this list of root-level tasks
        self.tasks = []
        # task queues by processor type
        self.task_queues = {}
        # index related with the "last_update" property of TaskRunLog (does not store nulls)
        self.ix_last_update = db0.index()
        self.ix_completed_at = db0.index()
        
            
@memo
class Task:
    def __init__(self, type, processor_type, data = None, key = None, parent = None,
                 requirements = None, scheduled_at = None, deadline = None, prefix = None):
        db0.set_prefix(self, prefix)
        # optional task key (None is allowed)
        self.key = key
        # task type e.g. 'etl'
        self.type = type
        # task specific data dict
        self.data = data
        # task size in task type specific units - e.g. bytes
        self.task_size = 0
        # task creation date and time
        self.created_at = datetime.now()
        # optional deadline (affects task priority)
        self.deadline = deadline
        # optional task execution scheduled date and time
        self.scheduled_at = scheduled_at
        # task status code
        self.status = 0
        # task associated processor type
        self.processor_type = processor_type
        self.runs = []
        self.parent = parent
        self.root = parent.root if parent is not None else None
        self.child_tasks = []
        self.requirements = requirements        
        self.max_retry = None
        self.root = parent.root if parent is not None else self
    

@memo
class TaskRunLog:
    def __init__(self, task, processor_id):
        self.task = task
        self.processor_id = processor_id
        self.started_at = datetime.now()
        self.last_update = self.started_at
        self.progress = 0
        self.error_message = None
        self.result = None
        self.error_code = None        
    
    
@memo
class TaskQueue:
    def __init__(self, prefix = None):
        db0.set_prefix(self, prefix)
        # index corresponding to the "scheduled_at" property
        self.ix_scheduled_at = db0.index()
        # index related with the "deadline" property
        self.ix_deadline = db0.index()
        # index related with the "created_at" property
        self.ix_created_at = db0.index()
    
    def test_schedule(self):
        return self.ix_scheduled_at.select(None, datetime.now(), null_first=True)

    def __grab_from(self, query, limit):        
        tasks = list(islice(query, limit))        
        db0.tags(*tasks).remove("ready")        
        return len(tasks)
            
    def test_grab(self, limit):        
        return self.__grab_from(db0.find(Task, "ready"), limit)

    def test_grab_sorted(self, limit):
        return self.__grab_from(self.ix_created_at.sort(db0.find(Task, "ready")), limit)


@memo
class TaskRequirements:
    def __init__(self, memory = 4096, vcpu_milli = 1000):
        self.memory = memory
        self.vcpu_milli = vcpu_milli

    
def test_create_zorch(db0_fixture):
    zorch = Zorch()
    assert len(zorch.tasks) == 0


def test_create_minimal_task(db0_fixture):
    task = Task("etl", "etl")
    assert task.type == "etl"
    assert task.processor_type == "etl" 


def test_create_task_log(db0_fixture):
    task = Task("etl", "etl")
    task.runs.append(TaskRunLog(task, "etl_oooooo"))
    assert len(task.runs) == 1


def test_create_task_queue(db0_fixture):
    task_queue = TaskQueue()
    assert task_queue is not None


def test_create_task_with_requirements(db0_fixture):
    task = Task("etl", "etl", requirements = TaskRequirements(1024, 500))
    assert task.requirements.memory == 1024
    assert task.requirements.vcpu_milli == 500


def test_create_10k_tasks(db0_fixture):
    tasks = [Task("etl", "etl") for i in range(10000)]
    assert len(tasks) == 10000


def test_push_tasks_into_zorch_model(db0_fixture):
    zorch = Zorch()
    task_count = 100    
    for i in range(task_count):
        key = f"some task key_{i}"
        # 1. check for dupicates
        # assert key not in zorch.keys
        zorch.keys.add(key)
        # 2. find existing or create new task queue
        processor_type = "etl"
        task_queue = zorch.task_queues.get(processor_type, None)
        if task_queue is None:
            task_queue = TaskQueue()
            zorch.task_queues[processor_type] = task_queue

        # 3. create new task
        task = Task("etl", "etl", key = key)
        
        # 4. add task to the queue
        task_queue.ix_scheduled_at.add(i, task)
        task_queue.ix_deadline.add(i, task)
        task_queue.ix_created_at.add(i, task)

        # 5. mark task as ready / root
        db0.tags(task).add("ready", "root")
    
    db0.commit()    


def test_atomic_push_tasks_find_runnable_pods(db0_fixture):    
    zorch = Zorch()
    with db0.atomic():
        tq = TaskQueue()
        zorch.task_queues["q"] = tq
        task = Task("etl", "etl", key = "1")
        tq.ix_scheduled_at.add(None, task)

    zorch = Zorch()
    assert len(list(zorch.task_queues["q"].test_schedule())) == 1


def test_atomic_grab_tasks_issue_1(db0_fixture):
    prefix = "zorch-test-prefix"
    zorch = Zorch(prefix=prefix)
    for _ in range(2):
        with db0.atomic():
            for _ in range(2):
                tq = zorch.task_queues.get("q", None)
                if tq is None:
                    tq = TaskQueue(prefix=prefix)
                    zorch.task_queues["q"] = tq
                
                task = Task("etl", "etl", prefix=prefix)
                db0.tags(task).add("ready")
    
    # open to change current prefix (to define the find's scope)
    db0.open(prefix)
    assert zorch.task_queues["q"].test_grab(2) == 2
    assert zorch.task_queues["q"].test_grab(2) == 2
    assert zorch.task_queues["q"].test_grab(2) == 0


def test_atomic_grab_tasks_issue_2(db0_fixture):
    prefix = "zorch-test-prefix"    
    for _ in range(6):
        zorch = Zorch(prefix=prefix)
        with db0.atomic():
            for _ in range(6):
                tq = zorch.task_queues.get("etl", None)
                if tq is None:
                    tq = TaskQueue(prefix=prefix)
                    zorch.task_queues["etl"] = tq
                
                task = Task("etl", "etl", prefix=prefix)
                tq.ix_created_at.add(datetime.now(), task)
                db0.tags(task).add("ready")
    
    # open to change current prefix (to define the find's scope)
    db0.open(prefix)
    
    def __grab_sorted(limit):
        zorch = Zorch(prefix=prefix)
        return zorch.task_queues["etl"].test_grab_sorted(limit)
    
    assert __grab_sorted(2) == 2
    assert __grab_sorted(2) == 2
    assert __grab_sorted(2) == 2    
