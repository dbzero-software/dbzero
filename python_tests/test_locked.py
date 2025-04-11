import pytest
import random
import asyncio
from .memo_test_types import MemoTestClass, MemoScopedClass
import dbzero_ce as db0


def test_locked_section_can_be_entered(db0_fixture):
    with db0.locked():
        obj = MemoTestClass(951)
        obj_uuid = db0.uuid(obj)        
    # make sure object exists outside of locked section
    assert db0.fetch(obj_uuid) == obj
    

def test_locked_section_reporting_updated_current_prefix(db0_fixture):
    px_name = db0.get_current_prefix().name
    with db0.locked() as lock:
        obj = MemoTestClass(951)
    # collect the mutation log after exiting the locked section
    mutation_log = lock.get_mutation_log()
    assert px_name in [name for name, _ in mutation_log]
    
    
def test_locked_section_no_mutations(db0_fixture):
    obj = MemoTestClass(951)
    with db0.locked() as lock:
        # read/only operation
        obj_2 = db0.fetch(db0.uuid(obj))
    
    assert len(lock.get_mutation_log()) == 0
    
    
def test_locked_section_non_default_prefix_mutations(db0_fixture):
    obj_1 = MemoTestClass(951)
    px_name = db0.get_current_prefix()
    db0.open("some-new-prefix", "rw")
    obj_2 = MemoTestClass(952)
    obj_3 = MemoTestClass(953)
    # switch back to px_name as the default prefix
    db0.open(px_name.name)
    with db0.locked() as lock:        
        # update non-default prefix bound object
        obj_2.value = 123123
        obj_3.value = 91237
        
    mutation_log = lock.get_mutation_log()
    assert px_name not in [name for name, _ in mutation_log]
    assert "some-new-prefix" in [name for name, _ in mutation_log]    
    
    
def test_prefix_opened_inside_locked_section(db0_fixture):
    obj_1 = MemoTestClass(951)
    with db0.locked() as lock:
        db0.open("some-new-prefix", "rw")
        obj_2 = MemoTestClass(952)
    
    mutation_log = lock.get_mutation_log()
    assert len(mutation_log) == 1
    assert "some-new-prefix" in [name for name, _ in mutation_log]    
    
    
def test_mutated_prefix_closed_inside_locked_section(db0_fixture):
    px_name = db0.get_current_prefix().name
    obj_1 = MemoTestClass(951)
    db0.open("some-new-prefix", "rw")
    with db0.locked() as lock:
        obj_1.value = 77777
        db0.close(px_name)
    
    mutation_log = lock.get_mutation_log()
    assert len(mutation_log) == 1
    assert px_name in [name for name, _ in mutation_log]


async def test_await_prefix_state(db0_fixture):
    px_name = db0.get_current_prefix().name
    for i in range(10):
        current_state_num = db0.get_state_num(px_name)
        obj = MemoTestClass(i)
        await db0.await_prefix_state(px_name, current_state_num)
        assert obj.value == i
        assert db0.get_state_num(px_name, True) == current_state_num


async def test_await_past_prefix_state(db0_fixture):
    px_name = db0.get_current_prefix().name

    await db0.await_prefix_state(px_name, db0.get_state_num(px_name, True))

    current_state_num = db0.get_state_num(px_name)
    obj = MemoTestClass(123)
    for _ in range(3):
        await db0.await_prefix_state(px_name, current_state_num)
    assert db0.get_state_num(px_name, True) == current_state_num

    current_state_num = db0.get_state_num(px_name)
    obj = MemoTestClass(123)
    await db0.await_prefix_state(px_name, current_state_num)
    assert db0.get_state_num(px_name, True) == current_state_num
    await db0.await_prefix_state(px_name, current_state_num - 1)
    await db0.await_prefix_state(px_name, current_state_num - 2)


async def test_await_future_prefix_state(db0_fixture):
    px_name = db0.get_current_prefix().name
    current_state_num = db0.get_state_num(px_name)

    state1 = db0.await_prefix_state(px_name, current_state_num)
    state2 = db0.await_prefix_state(px_name, current_state_num + 1)
    state3 = db0.await_prefix_state(px_name, current_state_num + 2)

    obj = MemoTestClass(123)
    await state1
    assert db0.get_state_num(px_name, True) == current_state_num

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(state2), 1)

    obj.value = 42
    await state2
    assert db0.get_state_num(px_name, True) == current_state_num + 1

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(state3), 1)

    obj.value = 999
    await state3
    assert db0.get_state_num(px_name, True) == current_state_num + 2


async def test_await_prefix_state_explicit_commit(db0_fixture):
    px_name = db0.get_current_prefix().name
    current_state_num = db0.get_state_num(px_name)
    
    aw = db0.await_prefix_state(px_name, current_state_num)
    obj = MemoTestClass(123)
    db0.commit()
    await aw
    assert db0.get_state_num(px_name, True) == current_state_num

    for i in range(10):
        current_state_num = db0.get_state_num(px_name)
        aws = [db0.await_prefix_state(px_name, current_state_num) for _ in range(5)]
        obj.value = i
        db0.commit()
        for aw in aws:
            await aw
        assert db0.get_state_num(px_name, True) == current_state_num


async def test_multiple_await_prefix_state(db0_fixture):
    px_name = db0.get_current_prefix().name
    current_state_num = db0.get_state_num(px_name)
    aws = [db0.await_prefix_state(px_name, current_state_num) for _ in range(3)]
    obj = MemoTestClass(123)
    for aw in aws:
        await aw
    assert db0.get_state_num(px_name, True) == current_state_num

    current_state_num = db0.get_state_num(px_name)
    aws = [db0.await_prefix_state(px_name, current_state_num) for _ in range(2)]
    aws += [db0.await_prefix_state(px_name, current_state_num - 1) for _ in range(2)]
    obj = MemoTestClass(123)
    for aw in aws:
        await aw
    assert db0.get_state_num(px_name, True) == current_state_num

    current_state_num = db0.get_state_num(px_name)
    aws = [db0.await_prefix_state(px_name, current_state_num - i) for i in range(3)]
    obj = MemoTestClass(123)
    for aw in aws:
        await aw
    assert db0.get_state_num(px_name, True) == current_state_num


async def test_await_prefix_state_multi_prefix(db0_fixture):
    prefixes = [db0.get_current_prefix().name, 'prefix2', 'prefix3']
    for px in prefixes:
        db0.open(px)
    
    objs = {prefix: MemoScopedClass(0, prefix) for prefix in prefixes}
    db0.commit()

    random.seed(1234)
    for i in range(25):
        run_prefixes = [prefix for prefix in prefixes if random.choice((True, False))]
        if not run_prefixes:
            continue

        state_nums = {prefix: db0.get_state_num(prefix) for prefix in run_prefixes}
        awaits = []
        for prefix in run_prefixes:
            state_nums[prefix] = db0.get_state_num(prefix)
            awaits.extend((prefix,) * random.randint(1, 5))
        # Shuffle the order in which multiple state observers are registeres
        random.shuffle(awaits)
        awaits_before = awaits[:random.randint(0, len(awaits))]
        awaits_after = awaits[len(awaits_before):]
        
        aws = [db0.await_prefix_state(prefix, state_nums[prefix]) for prefix in awaits_before]
        for prefix in run_prefixes:
            objs[prefix].value = random.randint(0, 1000000)
        aws += [db0.await_prefix_state(prefix, state_nums[prefix]) for prefix in awaits_after]
        for aw in aws:
            await aw

        for prefix in run_prefixes:
            assert db0.get_state_num(prefix, True) == state_nums[prefix]


async def test_await_prefix_state_partialy_unawaited(db0_fixture):
    px_name = db0.get_current_prefix().name
    obj = MemoScopedClass(0, px_name)
    db0.commit()

    for i in range(10):
        current_state_num = db0.get_state_num(px_name)
        obj.value = i
        aws = [db0.await_prefix_state(px_name, current_state_num) for _ in range(2)]
        for aw in aws[:1]:
            await aw
        assert db0.get_state_num(px_name, True) == current_state_num


async def test_await_prefix_state_many_unawaited(db0_fixture):
    px_name = db0.get_current_prefix().name
    obj = MemoScopedClass(0, px_name)
    db0.commit()

    for i in range(100):
        current_state_num = db0.get_state_num(px_name)
        obj.value = i
        db0.await_prefix_state(px_name, current_state_num)
        # Slight delay to give some of these futures the opportunity to finish 
        await asyncio.sleep(0.01)


async def test_await_prefix_state_invalid_args(db0_fixture):
    px_name = db0.get_current_prefix().name
    current_state_num = db0.get_state_num(px_name, True)

    with pytest.raises(ValueError):
        db0.await_prefix_state(px_name, -123)

    with pytest.raises(Exception):
        db0.await_prefix_state('invalid_prefix_name', 1)

    readonly_prefix = 'readonly-prefix'
    db0.open('readonly-prefix')
    db0.close(readonly_prefix)
    db0.open(readonly_prefix, 'r')
    with pytest.raises(Exception):
        db0.await_prefix_state(readonly_prefix, 1)

    with pytest.raises(TypeError):
        db0.await_prefix_state(None, None)  


async def test_await_commit_single_prefix(db0_fixture):
    px_name = db0.get_current_prefix().name
    obj = MemoTestClass(1234)
    db0.commit()

    for i in range(25):
        current_state_num = db0.get_state_num(px_name)
        async with db0.locked(await_commit=True):
            obj.value = i
        assert db0.get_state_num(px_name, True) == current_state_num


async def test_await_commit_multi_prefix(db0_fixture):
    prefixes = [db0.get_current_prefix().name, 'prefix2', 'prefix3']
    for px in prefixes:
        db0.open(px)
    
    objs = {prefix: MemoScopedClass(0, prefix) for prefix in prefixes}
    db0.commit()

    random.seed(1234)
    for i in range(25):
        run_prefixes = [prefix for prefix in prefixes if random.choice((True, False))]
        if not run_prefixes:
            continue

        state_nums = {prefix: db0.get_state_num(prefix) for prefix in run_prefixes}
        async with db0.locked(await_commit=True):
            for prefix in run_prefixes:
                objs[prefix].value = random.randint(0, 1000000)

        for prefix in run_prefixes:
            assert db0.get_state_num(prefix, True) == state_nums[prefix]
