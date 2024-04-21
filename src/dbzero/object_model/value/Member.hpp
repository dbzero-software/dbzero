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
        TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass storage_class)
    {   
        // create member function pointer
        using CreateMemberFunc = Value (*)(db0::swine_ptr<Fixture> &, typename LangToolkit::ObjectPtr);
        static std::vector<CreateMemberFunc> create_member_functions;
        if (create_member_functions.empty()) {
            registerCreateMemberFunctions<LangToolkit>(create_member_functions);
        }

        if (storage_class == StorageClass::DB0_SELF) {
            // address to self has no storage representation
            return 0;
        }
        
        assert(static_cast<int>(type_id) < create_member_functions.size());
        return create_member_functions[static_cast<int>(type_id)](fixture, lang_value);
    }
    
    template <typename LangToolkit, typename ContainerT> typename LangToolkit::ObjectSharedPtr unloadMember(
        const ContainerT &object, o_typed_item typed_item, const char *name = nullptr) 
    {
        return unloadMember<LangToolkit>(object, typed_item.m_storage_class, typed_item.m_value, name);
    }
    
    /**
     * @param name optional name (for error reporting only)
    */
    template <typename LangToolkit, typename ContainerT> typename LangToolkit::ObjectSharedPtr unloadMember(
        const ContainerT &object, StorageClass storage_class, Value value, const char *name = nullptr)
    {
        switch (storage_class)
        {
            case StorageClass::STRING_REF: {
                auto fixture = object.getFixture();
                db0::v_object<db0::o_string> string_ref(fixture->myPtr(value.cast<std::uint64_t>()));
                auto str_ptr = string_ref->get();
                return PyUnicode_FromStringAndSize(str_ptr.get_raw(), str_ptr.size());
            }
            break;
            
            case StorageClass::INT64: { 
                return PyLong_FromLong(value.cast<std::int64_t>());
            }
            break;
            
            case StorageClass::FP_NUMERIC64: {
                return PyFloat_FromDouble(value.cast<double>());
            }
            break;

            case StorageClass::OBJECT_REF: {
                auto fixture = object.getFixture();
                auto &class_factory = fixture->template get<ClassFactory>();
                
                // unload language specific object from DBZero
                return LangToolkit::unloadObject(fixture, value.cast<std::uint64_t>(), class_factory);
            }
            break;

            case StorageClass::DB0_SELF: {
                auto &obj = reinterpret_cast<const Object&>(object);
                // unload self-reference from DBZero (address and type known), no instance ID validation
                auto fixture = object.getFixture();
                return LangToolkit::unloadObject(fixture, obj.getAddress(), obj.getClassPtr());
            }
            break;

            case StorageClass::DB0_LIST: {
                auto fixture = object.getFixture();
                return LangToolkit::unloadList(fixture, value.cast<std::uint64_t>());
            }
            break;

            case StorageClass::DB0_BLOCK: {
                auto fixture = object.getFixture();
                return LangToolkit::unloadBlock(fixture, value.cast<std::uint64_t>());
            }
            break;

            case StorageClass::DB0_INDEX: {
                auto fixture = object.getFixture();
                return LangToolkit::unloadIndex(fixture, value.cast<std::uint64_t>());
            }
            break;

            case StorageClass::DB0_SET: {
                auto fixture = object.getFixture();
                return LangToolkit::unloadSet(fixture, value.cast<std::uint64_t>());
            }
            break;

            case StorageClass::DB0_DICT: {
                auto fixture = object.getFixture();
                return LangToolkit::unloadDict(fixture, value.cast<std::uint64_t>());
            }
            break;

            case StorageClass::DB0_TUPLE: {
                auto fixture = object.getFixture();
                return LangToolkit::unloadTuple(fixture, value.cast<std::uint64_t>());
            }
            break;

            case StorageClass::DB0_DATETIME: {
                return db0::python::uint64ToPyDatetime(value.cast<std::uint64_t>());
            }
            break;

            case StorageClass::DB0_BYTES: {
                auto fixture = object.getFixture();
                db0::v_object<db0::o_binary> bytes = fixture->myPtr(value.cast<std::uint64_t>());
                auto bytes_ptr = bytes->getBuffer();
                return PyBytes_FromStringAndSize(reinterpret_cast<const char *>(bytes_ptr), bytes->size());
            }

            case StorageClass::NONE: {
                return Py_None;
            }

            /* FIXME:
            case StorageClass::pooled_string: {
                return bp::object(pyzero::toString(memberAt(index)));
            }
            break;

            case StorageClass::dbz_list: {
                return bp::object(DBZList::__ref(memberAt(index)));
            }
            break;

            case StorageClass::dbz_dict: {
                return bp::object(DBZDict::__ref(memberAt(index)));
            }
            break;

            case StorageClass::dbz_set: {
                return bp::object(DB0Set::__ref(memberAt(index)));
            }
            break;

            case StorageClass::ptime64: {
                auto ptime_ = db0::CopyCast<std::uint64_t, boost::posix_time::ptime>()(memberAt(index));
                return bp::object(ptime_);
            }
            break;

            case StorageClass::date: {
                auto date_ = db0::CopyCast<std::uint64_t, db0::daydate>()(memberAt(index));
                return bp::object(date_);
            }
            break;
            */
            
            default: {
                THROWF(db0::InternalException)
                    << "Unable to get member: " << (name ? name : "<name-unknown>") << " as storage class " 
                    << (int)storage_class << THROWF_END;
            }
            break;
        }
    }
    
}
