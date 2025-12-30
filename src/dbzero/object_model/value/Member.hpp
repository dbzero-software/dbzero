// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/bindings/TypeId.hpp>
#include "Value.hpp"
#include "StorageClass.hpp"
#include <dbzero/bindings/python/collections/PyList.hpp>
#include <dbzero/bindings/python/collections/PySet.hpp>
#include <dbzero/bindings/python/collections/PyDict.hpp>
#include <dbzero/bindings/python/collections/PyTuple.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/index/Index.hpp>

namespace db0::object_model

{
    
    using TypeId = db0::bindings::TypeId;
    using PyToolkit = db0::python::PyToolkit;
    using PyObjectPtr = PyToolkit::ObjectPtr;
    using AccessFlags = db0::AccessFlags;
    
    template <TypeId type_id, typename LangToolkit> Value createMember(db0::swine_ptr<Fixture> &fixture,
        typename LangToolkit::ObjectPtr obj_ptr, StorageClass, AccessFlags);
    
    // Register TypeId specialized functions
    template <typename LangToolkit> void registerCreateMemberFunctions(
        std::vector<Value (*)(db0::swine_ptr<Fixture> &, typename LangToolkit::ObjectPtr, StorageClass, AccessFlags)> &functions);
    
    template <typename LangToolkit> Value createMember(db0::swine_ptr<Fixture> &fixture,
        TypeId type_id, StorageClass storage_class, typename LangToolkit::ObjectPtr obj_ptr, AccessFlags access_mode)
    {   
        // create member function pointer
        using CreateMemberFunc = Value (*)(db0::swine_ptr<Fixture> &, typename LangToolkit::ObjectPtr, StorageClass, AccessFlags);
        static std::vector<CreateMemberFunc> create_member_functions;
        if (create_member_functions.empty()) {
            registerCreateMemberFunctions<LangToolkit>(create_member_functions);
        }
        
        assert(static_cast<int>(type_id) < create_member_functions.size());
        auto func_ptr = create_member_functions[static_cast<int>(type_id)];
        if (!func_ptr) {
            THROWF(db0::InternalException) << "Value of TypeID: " << (int)type_id << " cannot be converted to a member" << THROWF_END;
        }
        return func_ptr(fixture, obj_ptr, storage_class, access_mode);
    }
    
    template <typename LangToolkit> typename LangToolkit::ObjectSharedPtr unloadMember(
        db0::swine_ptr<Fixture> &fixture, o_typed_item typed_item, unsigned int offset = 0, AccessFlags access_mode = {})
    {
        return unloadMember<LangToolkit>(
            fixture, typed_item.m_storage_class, typed_item.m_value, offset, access_mode
        );
    }
    
    template <StorageClass storage_class, typename LangToolkit> typename LangToolkit::ObjectSharedPtr unloadMember(
        db0::swine_ptr<Fixture> &fixture, Value value, unsigned int offset = 0, AccessFlags access_mode = {});
    
    // register StorageClass specializations
    template <typename LangToolkit> void registerUnloadMemberFunctions(
        std::vector<typename LangToolkit::ObjectSharedPtr (*)(db0::swine_ptr<Fixture> &, Value, unsigned int, AccessFlags)> &functions);
    
    /**
     * @param name optional name (for error reporting only)
     * @param offset optional offset for lo-fi members (default = 0)
    */
    template <typename LangToolkit> typename LangToolkit::ObjectSharedPtr unloadMember(
        db0::swine_ptr<Fixture> &fixture, StorageClass storage_class, Value value, unsigned int offset = 0, AccessFlags access_mode = {})
    {
        // create member function pointer
        using UnloadMemberFunc = typename LangToolkit::ObjectSharedPtr (*)(db0::swine_ptr<Fixture> &, Value, unsigned int, AccessFlags);
        static auto *unload_member_functions_ptr = new std::vector<UnloadMemberFunc>();
        auto &unload_member_functions = *unload_member_functions_ptr;
        if (unload_member_functions.empty()) {
            registerUnloadMemberFunctions<LangToolkit>(unload_member_functions);
        }

        assert(static_cast<int>(storage_class) < unload_member_functions.size());
        assert(unload_member_functions[static_cast<int>(storage_class)]);
        return unload_member_functions[static_cast<int>(storage_class)](fixture, value, offset, access_mode);
    }
    
    // unreference a member (decref / destroy where applicable)
    template <StorageClass storage_class, typename LangToolkit> void unrefMember(
        db0::swine_ptr<Fixture> &fixture, Value value);
    
    // register StorageClass specializations
    template <typename LangToolkit> void registerUnrefMemberFunctions(
        std::vector<void (*)(db0::swine_ptr<Fixture> &, Value)> &functions);
    
    template <typename LangToolkit> void unrefMember(
        db0::swine_ptr<Fixture> &fixture, StorageClass storage_class, Value value)
    {
        // create member function pointer
        using UnrefMemberFunc = void (*)(db0::swine_ptr<Fixture> &, Value);
        static auto *unref_member_functions_ptr = new std::vector<UnrefMemberFunc>();
        auto &unref_member_functions = *unref_member_functions_ptr;
        if (unref_member_functions.empty()) {
            registerUnrefMemberFunctions<LangToolkit>(unref_member_functions);
        }

        assert(static_cast<int>(storage_class) < unref_member_functions.size());
        if (unref_member_functions[static_cast<int>(storage_class)]) {
            unref_member_functions[static_cast<int>(storage_class)](fixture, value);
        }
    }
    
    /**
     * Invoke materialize before setting obj_ptr as a member
     * this is to materialize objects (where hasInstance = false) before using them as members
    */    
    void materialize(FixtureLock &fixture, PyObjectPtr obj_ptr);
    bool isMaterialized(PyObjectPtr obj_ptr);
    
}
