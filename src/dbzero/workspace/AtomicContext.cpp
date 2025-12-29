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

namespace db0

{
    
    std::mutex AtomicContext::m_atomic_mutex;

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
    
    AtomicContext::AtomicContext(std::shared_ptr<Workspace> &workspace, std::unique_lock<std::mutex> &&lock)
        : m_workspace(workspace)
        , m_atomic_lock(std::move(lock))
    {
        assert(isActive());
        m_workspace->preAtomic();
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
            m_workspace->cancelAtomic();
            m_objects.clear();
        } catch (...) {
            m_atomic_lock.unlock();
            throw;
        }
        // unlock the atomic mutex
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
            
            m_workspace->endAtomic();
            m_objects.clear();
        } catch (...) {
            m_atomic_lock.unlock();
            throw;
        }
        // unlock the atomic mutext
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
    
    std::unique_lock<std::mutex> AtomicContext::lock() {
        return std::unique_lock<std::mutex>(m_atomic_mutex);
    }
    
    bool AtomicContext::isActive() const {
        return m_atomic_lock.owns_lock();
    }

}