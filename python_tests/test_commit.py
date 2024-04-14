import dbzero_ce as db0
from itertools import islice
from datetime import datetime
from .memo_test_types import MemoTestClass


@db0.memo
class MemoTask:
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


def test_create_instances_in_multiple_transactions(db0_fixture):
    group_size = 10
    for x in range(10):
        for i in range(group_size):
            obj = MemoTestClass(i)
            db0.tags(obj).add("tag1")
        
        db0.commit()        
        assert len(list(db0.find("tag1"))) == group_size * (x + 1)


def test_append_list_in_multiple_transactions(db0_fixture):
    # prepare instances first
    tasks = db0.list()
    db0.commit()
    for _ in range(10):
        for _ in range(5):
            tasks.append(MemoTask("etl", "processor1"))
        db0.commit()
    
    assert len(tasks) == 50


def test_create_types_in_multiple_transactions(db0_fixture):
    MemoTestClass(123)    
    db0.commit()
    # type MemoTask created in a new transaction
    MemoTask("etl", "processor1")


def test_append_list_member_in_multiple_transactions(db0_fixture):
    root = MemoTestClass(db0.list())
    db0.commit()
    for _ in range(10):
        for _ in range(5):            
            root.value.append(MemoTask("etl", "processor1"))
        db0.commit()
            
    assert len(root.value) == 50

    
def test_untag_instances_in_multiple_transactions(db0_fixture):
    # prepare instances first
    count = 10
    for _ in range(count):
        task = MemoTask("etl", "processor1")
        db0.tags(task).add("ready")
    
    db0.commit()
    repeats = 0
    while count > 0:
        tasks = list(islice(db0.find("ready"), 2))
        for task in tasks:
            task.runs.append(MemoTestClass(1))
            
        count -= len(tasks)
        repeats += 1
        db0.tags(*tasks).remove("ready")
        db0.tags(*tasks).add("running")
        db0.commit()
    
    assert repeats == 5