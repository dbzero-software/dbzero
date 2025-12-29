# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
import multiprocessing
import inspect
from dbzero.reflection_api import CallableType
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton, MemoTestClassPropertiesAndImmutables, MemoTestClassWithMethods
from datetime import datetime


def test_get_prefixes(db0_fixture):
    assert len(list(db0.get_prefixes())) == 1
    db0.open("my-new_prefix")
    assert len(list(db0.get_prefixes())) == 2
    
    
def test_get_prefixes_with_nested_dirs(db0_fixture):
    assert len(list(db0.get_prefixes())) == 1    
    db0.open("dir_1/my-new_prefix")    
    assert len(list(db0.get_prefixes())) == 2
    db0.open("dir_1/subdir/my-new_prefix")
    assert len(list(db0.get_prefixes())) == 3
    db0.open("dir_2/subdir1/subdir2/my-new_prefix")
    assert len(list(db0.get_prefixes())) == 4

    
def test_get_memo_classes_from_default_prefix(db0_fixture):
    _ = MemoTestClass(123)
    assert len(list(db0.get_memo_classes())) > 0

def subprocess_get_memo_classes(result_queue, prefix):

    db0.init(DB0_DIR)
    db0.open(prefix.name)
    result_queue.put(list(db0.get_memo_classes()))

def test_get_memo_classes_from_separate_process(db0_fixture):
    prefix = db0.get_current_prefix()
    _ = MemoTestClass(123)
    db0.commit()
    db0.close()
    
    # run from a subprocess
    result_queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=subprocess_get_memo_classes, 
                                args = (result_queue, prefix))
    p.start()
    p.join()
    
    # validate the result
    memo_classes = result_queue.get()    
    assert len(memo_classes) > 0


def test_get_memo_classes_by_prefix_name(db0_fixture):
    prefix = db0.get_current_prefix()
    _ = MemoTestClass(123)
    assert len(list(db0.get_memo_classes(prefix.name))) > 0


def test_get_memo_classes_by_current_prefix(db0_fixture):
    prefix = db0.get_current_prefix()
    _ = MemoTestClass(123)
    assert len(list(db0.get_memo_classes(prefix))) > 0


def test_get_memo_classes_by_prefix_uuid_only(db0_fixture):
    _ = MemoTestClass(123)
    count = sum([len(list(db0.get_memo_classes(db0.PrefixMetaData(None, uuid)))) for _, uuid in db0.get_prefixes()])
    assert count > 0


def test_get_memo_classes_by_prefix_metadata(db0_fixture):
    _ = MemoTestClass(123)
    count = sum([len(list(db0.get_memo_classes(px) for px in db0.get_prefixes()))])
    assert count > 0


def test_get_memo_classes_raises_when_mismatched_name_and_uuid(db0_fixture):
    _ = MemoTestClass(123)
    with pytest.raises(Exception):
        _ = [list(db0.get_memo_classes(prefix_name=name, prefix_uuid=123)) for name, _ in db0.get_prefixes()]
    
        
def test_get_memo_classes_returns_singletons(db0_fixture):
    root = MemoTestSingleton(123)
    _ = MemoTestClass(123)
    singletons = [obj for obj in db0.get_memo_classes() if obj.is_singleton]
    assert len(singletons) == 1
    # try accessing the singleton by UUID
    obj = singletons[0].get_instance()
    assert obj == root
    
   
def test_memo_class_get_attributes(db0_fixture):
    _ = MemoTestClass(123)
    memo_info = [obj for obj in db0.get_memo_classes() if not obj.is_singleton][0]
    assert len(list(memo_info.get_attributes())) > 0


class TestClassPropertiesAndImmutables:
    __test__ = False
    
    def __init__(self, value):
        self.__value = value
        self.some_param = 5

    @db0.immutable
    def immutable_func(self):
        return self.value

    @property
    def value(self):
        return self.__value

    def normal_method(self):
        pass

def test_memo_class_get_properties(db0_fixture):
    obj = MemoTestClassPropertiesAndImmutables(123)
    result = list(db0.get_properties(obj))
    assert len(result) == 3
    assert ("some_param", False) in result
    assert ("value", False) in result
    assert ("immutable_func", True) in result

def test_class_get_properties():
    obj = TestClassPropertiesAndImmutables(123)
    result = list(db0.get_properties(obj))
    assert len(result) == 3
    assert ("some_param", False) in result
    assert ("value", False) in result
    assert ("immutable_func", True) in result

class CallablesTestClass:
    # query
    @db0.immutable
    def immutable_query(self, from_date: datetime = None):
        pass
    
    # action
    def action(self, from_date: datetime = None):
        pass

    # mutator
    def mutator(self):
        pass

    # property
    @db0.immutable
    def immutable_property(self):
        pass

    @db0.immutable
    @db0.complete_with('complete_two_stage_action')
    def begin_two_stage_action(self, param):
        pass

    def complete_two_stage_action(self, hash, param):
        pass


def test_class_get_callables():
    obj = CallablesTestClass()
    result = list(db0.get_callables(obj))
    assert len(result) == 4
    assert ("immutable_query", CallableType.QUERY) in result
    assert ("action", CallableType.ACTION) in result
    assert ("mutator", CallableType.MUTATOR) in result
    assert ("begin_two_stage_action", CallableType.QUERY) in result

    result = list(db0.get_callables(obj, True))
    assert len(result) == 5
    assert ("immutable_query", CallableType.QUERY) in result
    assert ("action", CallableType.ACTION) in result
    assert ("mutator", CallableType.MUTATOR) in result
    assert ("immutable_property", CallableType.PROPERTY) in result
    assert ("begin_two_stage_action", CallableType.QUERY) in result

@db0.memo
class CallablesMemoTestClass:
    # query
    @db0.immutable
    def immutable_query(self, from_date: datetime = None):
        pass
    
    # action
    def action(self, from_date: datetime = None):
        pass

    # mutator
    def mutator(self):
        pass

    # property
    @db0.immutable
    def immutable_property(self):
        pass


def test_memo_class_get_callables(db0_fixture):
    obj = CallablesMemoTestClass()
    result = list(db0.get_callables(obj))
    assert len(result) == 3
    assert ("immutable_query", CallableType.QUERY) in result
    assert ("action", CallableType.ACTION) in result
    assert ("mutator", CallableType.MUTATOR) in result

    result = list(db0.get_callables(obj, True))
    assert len(result) == 4
    assert ("immutable_query", CallableType.QUERY) in result
    assert ("action", CallableType.ACTION) in result
    assert ("mutator", CallableType.MUTATOR) in result
    assert ("immutable_property", CallableType.PROPERTY) in result

def test_discover_tagged_objects(db0_fixture):
    obj = MemoTestClass(123)
    db0.tags(obj).add("tag1", "tag2")
    # using reflection API, identify memo classes
    memo_type = [obj for obj in db0.get_memo_classes()][0].get_class()
    # find all objects of this type
    assert len(list(db0.find(memo_type))) == 1
    
    
def test_get_instance_count_for_class(db0_fixture):
    _ = MemoTestClass(123)
    memo_info = [obj for obj in db0.get_memo_classes() if not obj.is_singleton][0]
    assert memo_info.get_instance_count() > 0


def test_memo_class_get_attribute_values(db0_fixture):
    def values_of(obj, attr_names):
        return [getattr(obj, attr_name) for attr_name in attr_names]
    
    db0.tags(MemoTestClass(datetime.now()), MemoTestClass(123)).add("tag1")
    memo_info = [obj for obj in db0.get_memo_classes() if not obj.is_singleton][0]
    attr_names = [attr.name for attr in memo_info.get_attributes()]
    for obj in db0.find(MemoTestClass):
        assert len(values_of(obj, attr_names)) == len(attr_names)
        
    
def test_get_attributes_by_type(db0_fixture):
    obj = MemoTestClass(123)    
    assert len(list(db0.get_attributes(type(obj)))) > 0


def test_get_memo_class_by_uuid(db0_fixture):
    _ = MemoTestClass(123)
    meta_1 = list(db0.get_memo_classes())[0]
    meta_2 = db0.get_memo_class(meta_1.class_uuid)
    assert meta_1 == meta_2
    
    
def test_get_methods(db0_fixture):
    obj = MemoTestClassWithMethods(123)    
    methods = list(db0.get_methods(obj))
    assert len(methods) == 4
    
    
def test_get_all_instances_of_known_type_from_snapshot(db0_fixture, memo_tags):
    # commit to make data available to snapshot
    db0.commit()
    with db0.snapshot() as snap:
        total_len = 0
        for memo_class in db0.get_memo_classes():
            uuids = [db0.uuid(obj) for obj in memo_class.all(snap)]
            total_len += len(uuids)
        assert total_len > 0


def test_get_all_instances_of_unknown_type_from_snapshot(db0_fixture, memo_tags):
    db0.commit()
    with db0.snapshot() as snap:
        total_len = 0
        for memo_class in db0.get_memo_classes():
            uuids = [db0.uuid(obj) for obj in memo_class.all(snapshot=snap, as_memo_base=True)]
            total_len += len(uuids)
        assert total_len > 0

    
def test_import_model(db0_fixture):
    db0.import_model("datetime")


def test_get_mutable_prefixes(db0_fixture):
    def names(prefixes):
        return [prefix.name for prefix in prefixes]

    prefix = db0.get_current_prefix().name
    assert names(db0.get_mutable_prefixes()) == [prefix]

    db0.open('prefix1')
    db0.open('prefix2')
    result = names(db0.get_mutable_prefixes())
    assert set(result) == {'prefix2', 'prefix1', prefix}
    assert len(result) == len(set(result))

    db0.close()
    db0.init(DB0_DIR)
    db0.open('prefix1', 'r')
    db0.open('prefix2', 'r')
    db0.open('prefix3', 'rw')
    db0.open('prefix4', 'rw')
    result = names(db0.get_mutable_prefixes())
    assert set(names(db0.get_mutable_prefixes())) == {'prefix4', 'prefix3'}
    assert len(result) == len(set(result))


def test_method_info_class(db0_fixture):
    obj = MemoTestClassWithMethods(123)
    metaclass = db0.get_memo_class(obj)

    method = db0.MethodInfo('many_args_method', inspect.signature(obj.many_args_method), metaclass)
    assert method.name == 'many_args_method'
    assert method.metaclass is metaclass
    params = method.get_params()
    assert all(isinstance(param, db0.MethodParam) for param in params)
    assert all(param.method is method for param in params)
    assert [param.name for param in params] == ['param1', 'param2', 'param3']
    assert method.has_args
    assert method.has_kwargs

    method = db0.MethodInfo('get_value', inspect.signature(obj.get_value), metaclass)
    assert [param.name for param in method.get_params()] == []
    assert not method.has_args
    assert not method.has_kwargs

    method = db0.MethodInfo('get_value_plus', inspect.signature(obj.get_value_plus), metaclass)
    assert [param.name for param in method.get_params()] == ['other']
    assert not method.has_args
    assert not method.has_kwargs


def test_query_class(db0_fixture):
    def query_function(param1, /, param2, *args, param3, **kwargs):
        pass

    query = db0.Query('query_function', query_function)
    assert query.name == 'query_function'
    assert query.function_object is query_function
    assert query.has_params is True
    assert query.has_kwargs is True
    params = query.get_params()
    assert all(isinstance(param, db0.QueryParam) for param in params)
    assert all(param.query is query for param in params)
    assert [param.name for param in params] == ['param1', 'param2', 'param3']


def test_discover_schema(db0_fixture):
    _ = MemoTestClass(123)
    for meta_class in db0.get_memo_classes():        
        assert meta_class.get_schema() is not None


def test_get_memo_class_of_instance(db0_fixture):
    obj = MemoTestClass(123)
    memo_class = db0.get_memo_class(obj)    
    assert memo_class is not None
    assert db0.get_prefix_of(obj) == db0.get_prefix_of(memo_class.get_class())
