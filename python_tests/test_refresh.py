import pytest
import random
import multiprocessing
import time
import dbzero_ce as db0
from .conftest import DB0_DIR
from .memo_test_types import DynamicDataClass, DynamicDataSingleton, MemoTestClass, MemoTestSingleton


@db0.memo(singleton=True)
class MemoClassX:
    def __init__(self, value1, value2):
        self.value1 = value1
        self.value2 = value2
    

@db0.memo
class RefreshTestClass:
    def __init__(self, value1, value2):
        self.value1 = value1
        self.value2 = value2        


def test_objects_are_removed_from_gc0_registry_when_deleted(db0_fixture):
    # first crete objects
    object_1 = RefreshTestClass(0, "text")
    id_1 = db0.uuid(object_1)
    object_2 = RefreshTestClass(0, "text")
    id_2 = db0.uuid(object_2)
    root = MemoTestSingleton(object_1, object_2)
    prefix_name = db0.get_prefix_of(object_1).name
    db0.commit()
    db0.close()
    
    # open as read/write (otherwise GC0 not initialized)
    db0.init(DB0_DIR)
    db0.open(prefix_name, "rw")    
    object_1 = db0.fetch(id_1)
    reg_size_1 = db0.get_prefix_stats()["gc0"]["size"]
    # size can be >1 because type also might be registered
    assert reg_size_1 > 0
    object_2 = db0.fetch(id_2)
    assert db0.get_prefix_stats()["gc0"]["size"] > reg_size_1
    del object_1
    db0.clear_cache()
    assert db0.get_prefix_stats()["gc0"]["size"] == reg_size_1
    del object_2
    db0.clear_cache()
    assert db0.get_prefix_stats()["gc0"]["size"] < reg_size_1


def test_refresh_can_fetch_object_changes_done_by_other_process(db0_fixture):
    # create a singleton
    object_1 = MemoClassX(0, "text")
    prefix_name = db0.get_prefix_of(object_1).name
    max_value = 25
    
    # start a child process that will change the singleton
    def change_singleton_process():
        db0.init(DB0_DIR)
        db0.open(prefix_name, "rw")
        object_x = MemoClassX()
        for i in range(1, max_value + 1):
            object_x.value1 = i
            time.sleep(0.05)
            db0.commit()
        del object_x
        db0.close()
    
    # close db0 and open as read-only
    db0.commit()
    db0.close()
    
    p = multiprocessing.Process(target=change_singleton_process)
    p.start()
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")
    object_2 = MemoClassX()
    # refresh db0 to retrieve changes
    while object_2.value1 < max_value:
        db0.refresh()
        time.sleep(0.1)
    
    p.terminate()
    p.join()


def test_refresh_can_handle_objects_deleted_by_other_process(db0_fixture):
    # create singleton so that it's not dropped
    object_1 = MemoTestSingleton(123)
    object_id = db0.uuid(object_1)
    prefix_name = db0.get_prefix_of(object_1).name
    
    # drop object from a separate transaction / process
    def update_process():
        time.sleep(0.25)
        db0.init(DB0_DIR)
        db0.open(prefix_name, "rw")
        object_x = db0.fetch(object_id)
        # drop the singleton
        db0.delete(object_x)
        # must also remove python object, otherwise the instance will not be removed immediately
        del object_x
        db0.commit()
        db0.close()
    
    # close db0 and open as read-only
    db0.commit()
    db0.close()
    
    p = multiprocessing.Process(target=update_process)
    p.start()
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")
    object_1 = db0.fetch(object_id)
    max_repeat = 10
    while max_repeat > 0:
        db0.refresh() 
        max_repeat -= 1
        try :
            a = object_1.value
        except Exception:
            break
        time.sleep(0.1)

    p.join()
    assert max_repeat > 0        


def test_auto_refresh(db0_fixture):
    # create singleton with a list type member
    object_1 = MemoClassX(123, "some text")
    object_id = db0.uuid(object_1)
    prefix_name = db0.get_prefix_of(object_1).name
    
    # update object from a separate process
    def update_process():
        time.sleep(0.25)
        db0.init(DB0_DIR)
        db0.open(prefix_name, "rw")
        object_x = MemoClassX()
        object_x.value1 = 124        
        db0.commit()        
        db0.close()        

    # close db0 and open as read-only
    db0.commit()
    db0.close()
    
    p = multiprocessing.Process(target=update_process)
    p.start()
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")    
    object_1 = MemoClassX()
    assert object_1.value1 == 123
    max_repeat = 10
    while max_repeat > 0:
        max_repeat -= 1
        # exit when modified value is detected
        if object_1.value1 == 124:
            break        
        time.sleep(0.1)
    
    p.terminate()
    p.join()    
    assert max_repeat > 0        


def test_refresh_can_detect_kv_index_updates(db0_fixture):
    # create singleton with a list type member
    object_1 = RefreshTestClass(123, "some text")
    object_id = db0.uuid(object_1)
    root = MemoTestSingleton(object_1)
    prefix_name = db0.get_prefix_of(object_1).name
    
    # add dynamic (kv-index) field from a separate process
    def update_process():
        time.sleep(0.25)
        db0.init(DB0_DIR)
        db0.open(prefix_name, "rw")
        object_x = db0.fetch(object_id)
        object_x.new_field = 123
        db0.commit()
        db0.close()
        
    # close db0 and open as read-only
    db0.commit()
    db0.close()
    
    p = multiprocessing.Process(target=update_process)
    p.start()
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")    
    object_1 = db0.fetch(object_id)
    max_repeat = 10
    while max_repeat > 0:
        db0.refresh()
        max_repeat -= 1
        try :
            if object_1.new_field == 123:
                # a new field was detected
                break            
        except Exception as e:
            pass
        time.sleep(0.1)

    p.terminate()
    p.join()
    assert max_repeat > 0


def test_refresh_can_detect_updates_in_posvt_fields(db0_fixture):
    object_1 = RefreshTestClass(123, "some text")
    object_id = db0.uuid(object_1)
    root = MemoTestSingleton(object_1)
    prefix_name = db0.get_prefix_of(object_1).name
    
    # update posvt field from a separate process
    def update_process():
        time.sleep(0.25)
        db0.init(DB0_DIR)
        db0.open(prefix_name, "rw")
        object_x = db0.fetch(object_id)
        object_x.value1 = 999
        db0.commit()
        db0.close()
    
    # close db0 and open as read-only
    db0.commit()
    db0.close()
    
    p = multiprocessing.Process(target=update_process)
    p.start()
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")    
    object_1 = db0.fetch(object_id)
    max_repeat = 10
    while max_repeat > 0:
        db0.refresh()
        max_repeat -= 1        
        if object_1.value1 == 999:            
            break            
        time.sleep(0.1)

    p.terminate()
    p.join()
    assert max_repeat > 0
    
    
def test_refresh_can_detect_updates_in_indexvt_fields(db0_fixture):
    object_1 = DynamicDataClass([0, 1, 2, 11, 33, 119])
    object_id = db0.uuid(object_1)
    root = MemoTestSingleton(object_1)
    prefix_name = db0.get_prefix_of(object_1).name
    
    # update index-vt field from a separate process
    def update_process():
        time.sleep(0.25)
        db0.init(DB0_DIR)
        db0.open(prefix_name, "rw")
        object_x = db0.fetch(object_id)
        object_x.field_119 = 94124
        db0.commit()
        db0.close()
    
    # close db0 and open as read-only
    db0.commit()
    db0.close()
    
    p = multiprocessing.Process(target=update_process)
    p.start()
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")    
    object_1 = db0.fetch(object_id)
    max_repeat = 10
    while max_repeat > 0:
        db0.refresh()
        max_repeat -= 1        
        if object_1.field_119 == 94124:
            break            
        time.sleep(0.1)

    p.terminate()
    p.join()
    assert max_repeat > 0


def test_refresh_can_detect_updates_in_kvstore_fields(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    object_1 = DynamicDataSingleton(5)
    object_1.kv_field = 123
    root = MemoTestSingleton(object_1)        
    # update kv-store field from a separate process
    def update_process():
        time.sleep(0.25)
        db0.init(DB0_DIR)
        db0.open(prefix_name, "rw")
        object_x = DynamicDataSingleton()
        object_x.kv_field = 94124
        db0.commit()
        db0.close()
    
    # close db0 and open as read-only
    db0.commit()
    db0.close()
    
    p = multiprocessing.Process(target=update_process)
    p.start()
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")    
    object_1 = DynamicDataSingleton()
    max_repeat = 10
    while max_repeat > 0:
        db0.refresh()
        max_repeat -= 1
        if object_1.kv_field == 94124:
            break      
        time.sleep(0.1)

    p.terminate()
    p.join()
    assert max_repeat > 0


def test_objects_created_by_different_process_are_not_dropped(db0_fixture):
    some_instance = DynamicDataSingleton(5)
    object_x = MemoTestClass(123123)
    prefix_name = db0.get_current_prefix().name
    
    def create_process(result_queue):
        db0.init(DB0_DIR)
        db0.open(prefix_name, "rw")
        object_x = MemoTestClass(123123)
        top_object = MemoTestSingleton(object_x)
        result_queue.put(db0.uuid(object_x))
        db0.commit()
        db0.close()
    
    db0.commit()
    db0.close()
    
    result_queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=create_process, args = (result_queue,))
    p.start()
    p.join()    
    id = result_queue.get()
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")    
    object_1 = db0.fetch(id)
    db0.close()
    
    # open again to make sure it was not dropped
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")
    object_1 = db0.fetch(id)
    assert object_1.value == 123123
    
    
@pytest.mark.stress_test
def test_refresh_query_while_adding_new_objects(db0_fixture):
    px_name = db0.get_current_prefix().name
    
    def rand_string(str_len):
        import random
        import string    
        return ''.join(random.choice(string.ascii_letters) for i in range(str_len))
        
    def create_process(num_iterations, num_objects, str_len):
        db0.init(DB0_DIR)
        db0.open(px_name, "rw")
        for _ in range(num_iterations):          
            for index in range(num_objects):
                obj = MemoTestClass(rand_string(str_len))
                db0.tags(obj).add("tag1")
                if index % 3 == 0:
                    db0.tags(obj).add("tag2")            
            db0.commit()
        db0.close()
    
    db0.commit()
    db0.close()
    
    num_iterations = 1
    num_objects = 1000
    str_len = 4096
    p = multiprocessing.Process(target=create_process, args = (num_iterations, num_objects, str_len))
    p.start()
    
    try:
        db0.init(DB0_DIR)
        db0.open(px_name, "r")
        while True:
            db0.refresh()
            time.sleep(0.1)
            query_len = len(list(db0.find(MemoTestClass, "tag1")))        
            print(f"Query length: {query_len}")
            if query_len == num_iterations * num_objects:
                break
    finally:
        p.terminate()
        p.join()
        db0.close()


def test_wait_for_updates(db0_fixture):
    prefix = db0.get_current_prefix().name
    db0.commit()
    db0.close()

    def writer_process(prefix, writer_sem, reader_sem):
        db0.init(DB0_DIR)
        db0.open(prefix, "rw")
        reader_sem.release()
        while True:
            if not writer_sem.acquire(timeout=3.0):
                return # Safeguard
            time.sleep(0.1)
            _obj = MemoTestClass(123)
            db0.commit()

    writer_sem = multiprocessing.Semaphore(0)
    reader_sem = multiprocessing.Semaphore(0)
    def make_trasaction(n):
        for _ in range(n):
            writer_sem.release()

    p = multiprocessing.Process(target=writer_process, args=(prefix, writer_sem, reader_sem))
    p.start()
    reader_sem.acquire()

    db0.init(DB0_DIR)
    db0.open(prefix, "r")

    # Start waiting before transactions complete
    current_num = db0.get_state_num(prefix)
    make_trasaction(5)
    assert db0.wait(prefix, current_num + 5, 1000)

    # Start waiting after transactions complete
    current_num = db0.get_state_num(prefix)
    make_trasaction(2)
    time.sleep(0.5)
    assert db0.wait(prefix, current_num + 2, 1000)

    current_num = db0.get_state_num(prefix)
    # Wait current state
    assert db0.wait(prefix, current_num, 1000)
    # Wait past state
    assert db0.wait(prefix, current_num - 2, 1000)
    # Wait timeout
    assert db0.wait(prefix, current_num + 1, 1000) == False

    p.terminate()
    p.join()


@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 * 1024 * 1024}], indirect=True)
def test_refresh_issue1(db0_slab_size):
    """
    Issue: process blocked on refresh attempt
    Reason: 
    """    
    px_name = db0.get_current_prefix().name
    expected_values = ["first string", "second string"]
        
    rand_ints = [350, 480, 343, 475, 871, 493, 550, 723, 342, 236, 110, 585, 633, 54, 797, 478, 850, 716, 1021, 
                 136, 248, 879, 151, 249, 15, 717, 773, 625, 738, 731, 955, 280, 208, 730, 754, 982, 281, 221, 
                 549, 501, 282, 307, 551, 472, 509, 761, 78, 735, 744, 450, 388, 645, 577, 706, 417, 78, 849, 
                 873, 904, 534, 945, 985, 431, 725, 826, 49, 64, 766, 32, 460, 971, 766, 390, 990, 899, 835, 
                 16, 570, 190, 573, 54, 642, 840, 817, 924, 793, 634, 889, 835, 250, 676, 1006, 819, 322, 
                 373, 278, 895, 767, 380, 442]                 
    
    index = 0
    root = MemoTestSingleton([])
    for _ in range(10000):
        str_len = rand_ints[index]
        root.value.append(''.join("A" for i in range(str_len)))
        index += 1
        if index == len(rand_ints):
            index = 0
    db0.close()
    
    def make_small_update():
        time.sleep(0.25)
        db0.init(DB0_DIR)
        db0.open(px_name, "rw")
        note = MemoTestClass(expected_values[0])
        db0.tags(note).add("tag")
        db0.commit()                
        time.sleep(0.25)
        if 'D' in db0.build_flags():            
            db0.dbg_start_logs()
        note.value = expected_values[1]
        db0.close()
    
    p = multiprocessing.Process(target=make_small_update)
    p.start()
    
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    
    for i in range(2):
        state_num = db0.get_state_num(px_name)    
        # refresh until 2 transactions are detected
        max_repeat = 5
        if i == 1 and 'D' in db0.build_flags():
            db0.dbg_start_logs()
        
        while db0.get_state_num(px_name) == state_num:
            assert max_repeat > 0
            db0.refresh()
            time.sleep(0.1)
            max_repeat -= 1
        assert next(iter(db0.find(MemoTestClass))).value == expected_values[i]
        max_repeat -= 1
    
    p.join()
    
        