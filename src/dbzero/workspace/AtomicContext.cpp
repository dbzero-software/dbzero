// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "AtomicContext.hpp"
#include "Workspace.hpp"
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/list/List.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <Python.h>

namespace db0

{
    
    std::recursive_mutex AtomicContext::m_atomic_mutex;
    std::mutex AtomicContext::m_owner_state_mutex;
    AtomicContext::ExecutionIdentity AtomicContext::m_owner_identity;
    unsigned int AtomicContext::m_active_depth = 0;
    std::atomic<unsigned int> AtomicContext::m_active_depth_fast = 0;
    thread_local unsigned int AtomicContext::m_mutating_api_atomic_owner_depth = 0;

    // NOTE: since objects might've been destroyed inside atomic operation, we need to check before detaching
    template <typename T> void detachExisting(const T &obj)
    {
        if (obj.hasInstance()) {
            obj.detach();
        }
    }

    // MEMO_OBJECT specialization
    template <> void detachObject<TypeId::MEMO_OBJECT, PyToolkit>(PyObjectPtr obj_ptr) 
    {
        using MemoObject = PyToolkit::TypeManager::MemoObject;
        detachExisting(PyToolkit::getTypeManager().extractObject<MemoObject>(obj_ptr));
    }
    
    // DB0_LIST specialization
    template <> void detachObject<TypeId::DB0_LIST, PyToolkit>(PyObjectPtr obj_ptr) {
        detachExisting(PyToolkit::getTypeManager().extractList(obj_ptr));
    }

    // DB0_INDEX specialization
    template <> void detachObject<TypeId::DB0_INDEX, PyToolkit>(PyObjectPtr obj_ptr) {
        detachExisting(PyToolkit::getTypeManager().extractIndex(obj_ptr));
    }

    // DB0_SET specialization
    template <> void detachObject<TypeId::DB0_SET, PyToolkit>(PyObjectPtr obj_ptr) {
        detachExisting(PyToolkit::getTypeManager().extractSet(obj_ptr));
    }

    // DB0_DICT specialization
    template <> void detachObject<TypeId::DB0_DICT, PyToolkit>(PyObjectPtr obj_ptr) {
        detachExisting(PyToolkit::getTypeManager().extractDict(obj_ptr));
    }

    // DB0_TUPLE specialization
    template <> void detachObject<TypeId::DB0_TUPLE, PyToolkit>(PyObjectPtr obj_ptr) {
        detachExisting(PyToolkit::getTypeManager().extractTuple(obj_ptr));
    }
    
    template <> void registerDetachFunctions<PyToolkit>(std::vector<void (*)(PyObjectPtr)> &functions)
    {
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = detachObject<TypeId::MEMO_OBJECT, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_LIST)] = detachObject<TypeId::DB0_LIST, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_INDEX)] = detachObject<TypeId::DB0_INDEX, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_SET)] = detachObject<TypeId::DB0_SET, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_DICT)] = detachObject<TypeId::DB0_DICT, PyToolkit>;
        functions[static_cast<int>(TypeId::DB0_TUPLE)] = detachObject<TypeId::DB0_TUPLE, PyToolkit>;
    }
    
    AtomicContext::AtomicContext(std::shared_ptr<Workspace> &workspace, std::unique_lock<std::recursive_mutex> &&lock)
        : m_workspace(workspace)
        , m_parent(workspace->currentAtomicContext())
        , m_atomic_lock(std::move(lock))
    {
        assert(isActive());
        beginActiveOwner();
        if (!m_parent) {
            m_workspace->preAtomic();
        }
        m_workspace->beginAtomic(this);
    }
        
    void AtomicContext::cancel()
    {
        if (!isActive()) {
            THROWF(db0::InternalException) << "atomic 'cancel' failed: operation already completed" << THROWF_END;
        }

        try {
            // all objects from context need to be detached
            auto &type_manager = LangToolkit::getTypeManager();
            for (auto &pair : m_objects) {            
                detachObject<PyToolkit>(type_manager.getTypeId(pair.second.get()), pair.second.get());
            }
            m_objects.clear();
            m_workspace->cancelAtomic(this);
        } catch (...) {
            m_atomic_lock.unlock();
            throw;
        }
        // unlock the atomic mutex
        endActiveOwner();
        m_atomic_lock.unlock();
    }
    
    void AtomicContext::close()
    {
        if (isActive()) {
            approve();
        }
    }

    void AtomicContext::approve()
    {
        if (!isActive()) {
            THROWF(db0::InternalException) << "atomic 'approve' failed: operation already completed" << THROWF_END;
        }

        try {
            // detach / flush all workspace objects
            m_workspace->detach();
            // all objects from context need to be detached
            auto &type_manager = LangToolkit::getTypeManager();
            for (auto &pair : m_objects) {
                detachObject<PyToolkit>(type_manager.getTypeId(pair.second.get()), pair.second.get());
            }        
            
            m_workspace->endAtomic(this);
            if (m_parent) {
                for (auto &pair : m_objects) {
                    m_parent->add(pair.first, pair.second.get());
                }
            }
            m_objects.clear();
        } catch (...) {
            m_atomic_lock.unlock();
            throw;
        }
        // unlock the atomic mutext
        endActiveOwner();
        m_atomic_lock.unlock();
    }
    
    void AtomicContext::add(Address address, ObjectPtr lang_object)
    {
        if (m_objects.find(address) == m_objects.end()) {
            m_objects.insert({address, lang_object});            
        }        
    }

    void AtomicContext::moveFrom(AtomicContext &other, Address src_address, Address dst_address)
    {
        auto it = other.m_objects.find(src_address);
        if (it != other.m_objects.end()) {
            add(dst_address, it->second.get());
            other.m_objects.erase(it);
        }
    }
    
    std::unique_lock<std::recursive_mutex> AtomicContext::lock() {
        return std::unique_lock<std::recursive_mutex>(m_atomic_mutex);
    }

    AtomicContext::ExecutionIdentity AtomicContext::getCurrentExecutionIdentity()
    {
        ExecutionIdentity result;
        result.thread_id = std::this_thread::get_id();
        result.py_thread_state = PyThreadState_Get();
        result.async_task = nullptr;

        auto asyncio = Py_OWN(PyImport_ImportModule("asyncio"));
        if (!asyncio) {
            PyErr_Clear();
            return result;
        }

        auto current_task = Py_OWN(PyObject_GetAttrString(*asyncio, "current_task"));
        if (!current_task) {
            PyErr_Clear();
            return result;
        }

        auto task = Py_OWN(PyObject_CallNoArgs(*current_task));
        if (!task) {
            PyErr_Clear();
            return result;
        }

        if (*task != Py_None) {
            result.async_task = task.steal();
        }
        return result;
    }

    bool AtomicContext::isInAsyncTask()
    {
        auto identity = getCurrentExecutionIdentity();
        if (identity.async_task) {
            Py_DECREF(identity.async_task);
            return true;
        }
        return false;
    }

    void AtomicContext::assertSyncAtomicAllowed()
    {
        if (isInAsyncTask()) {
            THROWF(db0::InputException)
                << "db0.atomic is synchronous; use db0.async_atomic() inside asyncio tasks"
                << THROWF_END;
        }
    }

    void AtomicContext::assertAsyncAtomicAllowed()
    {
        if (!isInAsyncTask()) {
            THROWF(db0::InputException)
                << "db0.async_atomic requires a running asyncio task"
                << THROWF_END;
        }
    }

    bool AtomicContext::isSameExecution(const ExecutionIdentity &lhs, const ExecutionIdentity &rhs)
    {
        return lhs.thread_id == rhs.thread_id
            && lhs.py_thread_state == rhs.py_thread_state
            && lhs.async_task == rhs.async_task;
    }

    void AtomicContext::beginActiveOwner()
    {
        auto identity = getCurrentExecutionIdentity();
        std::lock_guard<std::mutex> guard(m_owner_state_mutex);
        if (m_active_depth == 0) {
            m_owner_identity = identity;
        } else {
            if (!isSameExecution(m_owner_identity, identity)) {
                if (identity.async_task) {
                    Py_DECREF(identity.async_task);
                }
                THROWF(db0::InternalException) << "db0 atomic owner changed during nested atomic operation" << THROWF_END;
            }
            if (identity.async_task) {
                Py_DECREF(identity.async_task);
            }
        }
        ++m_active_depth;
        m_active_depth_fast.store(m_active_depth, std::memory_order_release);
    }

    void AtomicContext::endActiveOwner()
    {
        std::lock_guard<std::mutex> guard(m_owner_state_mutex);
        assert(m_active_depth > 0);
        --m_active_depth;
        m_active_depth_fast.store(m_active_depth, std::memory_order_release);
        if (m_active_depth == 0) {
            if (m_owner_identity.async_task) {
                Py_DECREF(m_owner_identity.async_task);
            }
            m_owner_identity = {};
        }
    }

    bool AtomicContext::isOwnedByCurrentExecution()
    {
        return getOwnerRelation() == OwnerRelation::owner;
    }

    bool AtomicContext::isMutatingApiAtomicOwner()
    {
        return m_mutating_api_atomic_owner_depth > 0;
    }

    void AtomicContext::enterMutatingApiAtomicOwner()
    {
        ++m_mutating_api_atomic_owner_depth;
    }

    void AtomicContext::exitMutatingApiAtomicOwner()
    {
        assert(m_mutating_api_atomic_owner_depth > 0);
        --m_mutating_api_atomic_owner_depth;
    }

    AtomicContext::OwnerRelation AtomicContext::getOwnerRelation()
    {
        if (m_active_depth_fast.load(std::memory_order_acquire) == 0) {
            return OwnerRelation::inactive;
        }

        auto thread_id = std::this_thread::get_id();
        auto py_thread_state = PyThreadState_Get();
        PyObjectPtr owner_async_task = nullptr;
        {
            std::lock_guard<std::mutex> guard(m_owner_state_mutex);
            if (m_active_depth == 0) {
                return OwnerRelation::inactive;
            }
            if (m_owner_identity.thread_id != thread_id || m_owner_identity.py_thread_state != py_thread_state) {
                return OwnerRelation::other_thread;
            }
            if (!m_owner_identity.async_task) {
                return OwnerRelation::owner;
            }
            owner_async_task = m_owner_identity.async_task;
            Py_INCREF(owner_async_task);
        }

        auto identity = getCurrentExecutionIdentity();
        bool same_execution = isSameExecution({thread_id, py_thread_state, owner_async_task}, identity);
        Py_DECREF(owner_async_task);
        if (identity.async_task) {
            Py_DECREF(identity.async_task);
        }
        return same_execution ? OwnerRelation::owner : OwnerRelation::same_thread_non_owner;
    }

    void AtomicContext::waitIfBlockedByActiveOwner(bool fail_same_thread)
    {
        waitIfBlockedByOwnerRelation(getOwnerRelation(), fail_same_thread);
    }

    void AtomicContext::waitIfBlockedByOwnerRelation(OwnerRelation relation, bool fail_same_thread)
    {
        if (relation == OwnerRelation::inactive || relation == OwnerRelation::owner) {
            return;
        }

        if (relation == OwnerRelation::same_thread_non_owner) {
            if (!fail_same_thread) {
                return;
            }
            PyErr_SetString(
                PyExc_RuntimeError,
                "db0.async_atomic is active in another asyncio task; use db0.async_atomic() to serialize dbzero mutations"
            );
            THROWF(db0::InputException)
                << "db0.async_atomic is active in another asyncio task; use db0.async_atomic() to serialize dbzero mutations"
                << THROWF_END;
        }

        assert(relation == OwnerRelation::other_thread);
        {
            db0::python::WithGIL_Unlocked no_gil;
            auto atomic_lock = lock();
        }
    }
    
    bool AtomicContext::isActive() const {
        return m_atomic_lock.owns_lock();
    }

}
