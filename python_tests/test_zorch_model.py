import pytest
import dbzero_ce as db0
from dbzero_ce import memo, no
from datetime import datetime


@memo(singleton=True)
class Zorch:
    def __init__(self):
        # set of keys to prevent task duplication
        # FIXME: can be replaced with a bloom filter in future versions
        self.keys = set()
        # this list of root-level tasks
        self.tasks = []
        # task queues by processor type
        self.task_queues = {}
    

@memo
class Task:
    def __init__(self, type, processor_type, data = None, key = None, parent = None,
                 requirements = None, scheduled_at = None, deadline = None):
        # optional task key (None is allowed)
        self.key = key
        # task type e.g. 'etl'
        self.type = type
        # task specific data dict
        self.data = data
        # task size in task type specific units - e.g. bytes
        self.task_size = 0
        # task creation date and time
        # FIXME: implement
        # self.created_at = datetime.now()
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
        self.paused = False
        self.max_retry = None
    

@memo
class TaskRunLog:
    def __init__(self, task, processor_id):
        self.task = task
        self.processor_id = processor_id
        # FIXME: implement
        # self.started_at = datetime.now()
        # FIXME: implement
        # self.last_update = self.started_at
        self.progress = 0
        self.error_message = None
        self.result = None
        self.error_code = None
        self.kill = False
    
    
@memo
class TaskQueue:
    def __init__(self):
        # index corresponding to the "scheduled_at" property
        self.ix_scheduled_at = db0.index()
        # index related with the "deadline" property
        self.ix_deadline = db0.index()
        # index related with the "created_at" property
        self.ix_created_at = db0.index()


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
    start = datetime.now()
    tasks = [Task("etl", "etl") for i in range(10000)]
    # print duration in milliseconds
    print("Duration: ", (datetime.now() - start).microseconds / 1000)


def test_push_tasks_into_zorch_model(db0_fixture):
    zorch = Zorch()
    task_count = 100
    start = datetime.now()    
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
    end = datetime.now()
    # print duration in milliseconds
    print("Duration: ", (end - start).total_seconds())
    # push tasks / sec
    print("Push tasks / sec: ", task_count / (end - start).total_seconds())


def test_grab_tasks_query(db0_fixture):
    task_count = 100
    available_memory = 4096
    available_vcpu_milli = 1000
    task_queue = TaskQueue()
    ix_scheduled_at = task_queue.ix_scheduled_at
    ix_deadline = task_queue.ix_deadline
    ix_created_at = task_queue.ix_created_at
    root = Task("root-etl", "etl", key = "My new ETL root task")
    
    def get_resource_tags(memory=None, vcpu_milli=None):
        return (
            f"mem-{memory}" if memory is not None else "mem-default",
            f"vcpu-{vcpu_milli}" if vcpu_milli is not None else "vcpu-default"
        )
    
    for i in range(task_count):
        task = Task("etl", "etl", scheduled_at=i)
        
        ix_scheduled_at.add(i, task)
        ix_deadline.add(0, task)
        ix_created_at.add(i, task)
        
        db0.tags(task).add("ready", root, get_resource_tags())
        if i % 3 == 0:
            db0.tags(task).add("paused")
    
    tags = ("ready", get_resource_tags(available_memory, available_vcpu_milli))
    query = ix_deadline.sort(
        ix_created_at.sort(db0.find(
            Task, ix_scheduled_at.range(30, 100), tags, no("paused"))
        )
    )

    assert len(list(query)) == 69
