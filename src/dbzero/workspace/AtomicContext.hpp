// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include <dbzero/object_model/LangConfig.hpp>
#include <unordered_set>
#include <vector>
#include <dbzero/bindings/TypeId.hpp>

namespace db0

{

    class Workspace;
    using TypeId = db0::bindings::TypeId;
    using PyToolkit = db0::python::PyToolkit;
    using PyObjectPtr = PyToolkit::ObjectPtr;

    // register TypeId specialized detach functions
    template <typename LangToolkit> void registerDetachFunctions(
        std::vector<void (*)(typename LangToolkit::ObjectPtr)> &functions);

    template <TypeId type_id, typename LangToolkit> void detachObject(typename LangToolkit::ObjectPtr);

    template <typename LangToolkit> void detachObject(TypeId type_id, typename LangToolkit::ObjectPtr obj_ptr)
    {
        // detach object function pointer
        using DetachObjectFunc = void (*)(typename LangToolkit::ObjectPtr);
        static std::vector<DetachObjectFunc> detach_object_functions;
        if (detach_object_functions.empty()) {
            registerDetachFunctions<LangToolkit>(detach_object_functions);
        }
        
        assert(static_cast<int>(type_id) < detach_object_functions.size());
        auto func_ptr = detach_object_functions[static_cast<int>(type_id)];
        if (!func_ptr) {
            THROWF(db0::InternalException) << "Unable to detach object of TypeID: " << (int)type_id << THROWF_END;
        }
        return func_ptr(obj_ptr);
    }

    class AtomicContext
    {
    public:
        using LangToolkit = db0::object_model::LangConfig::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;
        using ObjectSharedExtPtr = LangToolkit::ObjectSharedExtPtr;

        AtomicContext(std::shared_ptr<Workspace> &, std::unique_lock<std::mutex> &&);

        // Register specific instance with the current transaction (for rollback/detach)
        void add(Address, ObjectPtr);
        
        // Try moving instance from a different AtomicContext
        void moveFrom(AtomicContext &, Address src_address, Address dst_address);
        
        void approve();
        void cancel();
        void close();
        
        static std::unique_lock<std::mutex> lock();
        
    private:
        std::shared_ptr<Workspace> m_workspace;
        std::unordered_map<Address, ObjectSharedExtPtr> m_objects;
        
        // mutex / lock to prevent mutliple concurrent atomic operations
        // also acquired by the autocommit-thread to prevent auto-commit during atomic operation
        static std::mutex m_atomic_mutex;
        std::unique_lock<std::mutex> m_atomic_lock;

        bool isActive() const;
    };

}