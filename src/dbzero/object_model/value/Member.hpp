#pragma once

#include <dbzero/bindings/TypeId.hpp>
#include "Value.hpp"
#include "StorageClass.hpp"
#include <dbzero/bindings/python/List.hpp>
#include <dbzero/bindings/python/Set.hpp>
#include <dbzero/bindings/python/Dict.hpp>
#include <dbzero/bindings/python/Tuple.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
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
    
    template <typename LangToolkit, typename ContainerT> Value createMember(const ContainerT &object,
        db0::bindings::TypeId type_id, typename LangToolkit::ObjectPtr lang_value, StorageClass)
    {
        using TypeId = db0::bindings::TypeId;
        switch (type_id) {
            case TypeId::INTEGER: {
                auto int_value = PyLong_AsLong(lang_value);
                return db0::binary_cast<std::uint64_t, std::int64_t>()(int_value);
            }
            break;
            
            case TypeId::FLOAT: {
                auto fp_value = PyFloat_AsDouble(lang_value);
                return db0::binary_cast<std::uint64_t, double>()(fp_value);
            }
            break;
            
            case TypeId::STRING: {
                // create string-ref member and take its address
                auto fixture = object.getFixture();
                return db0::v_object<db0::o_string>(*fixture, PyUnicode_AsUTF8(lang_value)).getAddress();
            }
            break;

            case TypeId::MEMO_OBJECT:
            {
                // extract address from the Memo object
                auto &object = LangToolkit::getTypeManager().extractObject(lang_value);
                object.modify().incRef();
                return object.getAddress();
            }
            case TypeId::DB0_BLOCK: {
                // extract address from the Block object
                return LangToolkit::getTypeManager().extractBlock(lang_value).getAddress();
            }
            break;

            case TypeId::DB0_LIST: {
                // extract address from the List object
                auto &list = LangToolkit::getTypeManager().extractList(lang_value);
                list.modify().incRef();
                return list.getAddress();
            }
            break;

            case TypeId::DB0_INDEX: {
                // extract address from the List object
                auto &index = LangToolkit::getTypeManager().extractIndex(lang_value);
                index.incRef();
                return index.getAddress();
            }
            break;

            case TypeId::DB0_SET: {
                // extract address from the Set object
                auto &set = LangToolkit::getTypeManager().extractSet(lang_value);
                set.incRef();
                return set.getAddress();
            }
            break;
            
            case TypeId::DB0_DICT: {
                // extract address from the Dict object
                auto &dict = LangToolkit::getTypeManager().extractDict(lang_value);
                dict.incRef();
                return dict.getAddress();
            }
            break;
            
            case TypeId::DB0_TUPLE: {
                // extract address from the Tuple object
                auto &tuple = LangToolkit::getTypeManager().extractTuple(lang_value);
                tuple.incRef();
                return tuple.getAddress();
            }
            break;
            // Python types 

            case TypeId::LIST: {
                auto list = db0::python::makeList(nullptr, &lang_value, 1);
                list->ext().modify().incRef();
                return list->ext().getAddress();
            }
            break;

            case TypeId::SET: {
                auto set = db0::python::makeSet(nullptr, &lang_value, 1);
                set->ext().incRef();                
                return set->ext().getAddress();
            }
            break;

            case TypeId::DICT: {
                PyObject* args = PyTuple_New(1);
                PyTuple_SetItem(args, 0, lang_value);
                auto dict = db0::python::makeDict(nullptr, args, nullptr);
                dict->ext().incRef();
                return dict->ext().getAddress();
            }
            break;

            case TypeId::TUPLE: {
                PyObject* args = PyTuple_New(1);
                PyTuple_SetItem(args, 0, lang_value);
                auto tuple = db0::python::makeTuple(nullptr, &lang_value, 1);
                tuple->ext().incRef();
                return tuple->ext().getAddress();
            }
            break;

            case TypeId::DATETIME: {
                return db0::python::pyDateTimeToToUint64(lang_value);
            }
            break;

            case TypeId::NONE: {
                return 0;
            }
            break;
            /*
            case StorageClass::pooled_string: {
                return pyzero::toVarChar(PyUnicode_AsUTF8(py_value));
            }
            break;
            
            case StorageClass::dbz_list: {
                return makeDBZList(bp::extract<bp::list>(py_value)())->getAddress();
            }
            break;

            case StorageClass::dbz_dict: {
                return makeDBZDict(bp::extract<bp::dict>(py_value)())->getAddress();
            }
            break;

            case StorageClass::dbz_set: {
                return makeDBZSet(bp::extract<bp::object>(py_value)())->getAddress();
            }
            break;

            case StorageClass::ptime64: {
                auto ptime_ = bp::extract<boost::posix_time::ptime>(py_value)();
                return db0::CopyCast<std::uint64_t, boost::posix_time::ptime>()(ptime_);
            }
            break;
            
            case StorageClass::date: {
                auto date_ = bp::extract<db0::daydate>(py_value)();
                return db0::CopyCast<std::uint64_t, db0::daydate>()(date_);
            }
            break;
            */
            
            default:
                THROWF(db0::InternalException)
                    << "Invalid type ID: " << (int)type_id << THROWF_END;
            break;
        }
    }
    
    template <typename LangToolkit, typename ContainerT> typename LangToolkit::ObjectSharedPtr unloadMember(
        const ContainerT &object, o_typed_item typed_item) 
    {
        return unloadMember<LangToolkit>(object, typed_item.m_storage_class, typed_item.m_value);
    }

    template <typename LangToolkit, typename ContainerT> typename LangToolkit::ObjectSharedPtr unloadMember(
        const ContainerT &object, StorageClass storage_class, Value value)
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
                    << "Invalid storage class in DB0 object (" << (int)storage_class << ")" << THROWF_END;
            }
            break;
        }
    }
    
}
