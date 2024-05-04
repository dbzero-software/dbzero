#pragma once

#include <dbzero/bindings/TypeId.hpp>
#include "Value.hpp"
#include "StorageClass.hpp"
#include <dbzero/bindings/python/collections/List.hpp>
#include <dbzero/bindings/python/collections/Set.hpp>
#include <dbzero/bindings/python/collections/Dict.hpp>
#include <dbzero/bindings/python/collections/Tuple.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/pandas/Block.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include <dbzero/object_model/dict/Dict.hpp>
#include <dbzero/object_model/tuple/Tuple.hpp>
#include <dbzero/object_model/index/Index.hpp>

namespace db0::object_model

{
    
    using TypeId = db0::bindings::TypeId;
    using PyToolkit = db0::python::PyToolkit;
    using PyObjectPtr = PyToolkit::ObjectPtr;

    template <TypeId type_id, typename LangToolkit> Value createMember(db0::swine_ptr<Fixture> &fixture,
        typename LangToolkit::ObjectPtr lang_value);

    // register TypeId specialized functions
    template <typename LangToolkit> void registerCreateMemberFunctions(
        std::vector<Value (*)(db0::swine_ptr<Fixture> &, typename LangToolkit::ObjectPtr)> &functions);

    template <typename LangToolkit> Value createMember(db0::swine_ptr<Fixture> &fixture,
        TypeId type_id, typename LangToolkit::ObjectPtr lang_value)
    {   
        // create member function pointer
        using CreateMemberFunc = Value (*)(db0::swine_ptr<Fixture> &, typename LangToolkit::ObjectPtr);
        static std::vector<CreateMemberFunc> create_member_functions;
        if (create_member_functions.empty()) {
            registerCreateMemberFunctions<LangToolkit>(create_member_functions);
        }

        assert(static_cast<int>(type_id) < create_member_functions.size());
        auto func_ptr = create_member_functions[static_cast<int>(type_id)];
        if (!func_ptr) {
            THROWF(db0::InternalException) << "Value of TypeID: " << (int)type_id << " cannot be converted to a member" << THROWF_END;

        }
        return func_ptr(fixture, lang_value);
    }
    
    template <typename LangToolkit> typename LangToolkit::ObjectSharedPtr unloadMember(
        db0::swine_ptr<Fixture> &fixture, o_typed_item typed_item, const char *name = nullptr)
    {
        return unloadMember<LangToolkit>(fixture, typed_item.m_storage_class, typed_item.m_value, name);
    }
    
    template <StorageClass storage_class, typename LangToolkit> typename LangToolkit::ObjectSharedPtr unloadMember(
        db0::swine_ptr<Fixture> &fixture, Value value, const char *name = nullptr);

    // register StorageClass specializations
    template <typename LangToolkit> void registerUnloadMemberFunctions(
        std::vector<typename LangToolkit::ObjectSharedPtr (*)(db0::swine_ptr<Fixture> &, Value, const char *)> &functions);

    /**
     * @param name optional name (for error reporting only)
    */
    template <typename LangToolkit> typename LangToolkit::ObjectSharedPtr unloadMember(
        db0::swine_ptr<Fixture> &fixture, StorageClass storage_class, Value value, const char *name = nullptr)
    {
        // create member function pointer
        using UnloadMemberFunc = typename LangToolkit::ObjectSharedPtr (*)(db0::swine_ptr<Fixture> &, Value, const char *);
        static std::vector<UnloadMemberFunc> unload_member_functions;
        if (unload_member_functions.empty()) {
            registerUnloadMemberFunctions<LangToolkit>(unload_member_functions);
        }

        assert(static_cast<int>(storage_class) < unload_member_functions.size());        
        assert(unload_member_functions[static_cast<int>(storage_class)]);
        return unload_member_functions[static_cast<int>(storage_class)](fixture, value, name);
    }
    
    /**
     * Invoke materialize before setting lang_value as a member
     * this is to materialize objects (where hasInstance = false) before using them as members
    */
    void materialize(FixtureLock &, PyObjectPtr lang_value);

    bool isMaterialized(PyObjectPtr lang_value);

}
