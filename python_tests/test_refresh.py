import pytest
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
    
    num_iterations = 10
    num_objects = 1000
    str_len = 128
    # str_len = 4096
    p = multiprocessing.Process(target=create_process, args = (num_iterations, num_objects, str_len))
    p.start()
    
    db0.init(DB0_DIR)
    db0.open(px_name, "r")    
    while True:
        db0.refresh()
        time.sleep(0.1)
        query_len = len(list(db0.find(MemoTestClass, "tag1")))        
        print(f"Query length: {query_len}")
        if query_len == num_iterations * num_objects:
            break        
    db0.close()
    p.join()
    