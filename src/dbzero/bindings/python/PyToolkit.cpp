#include "PyToolkit.hpp"
#include "Memo.hpp"
#include <dbzero/bindings/python/collections/List.hpp>
#include <dbzero/bindings/python/collections/Tuple.hpp>
#include "Index.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/memory/mptr.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/object.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/pandas/Block.hpp>
#include <dbzero/bindings/python/Pandas/PandasBlock.hpp>
#include <dbzero/bindings/python/collections/Set.hpp>
#include <dbzero/bindings/python/collections/Dict.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
#include <dbzero/object_model/index/Index.hpp>
#include <dbzero/object_model/set/Set.hpp>
#include "ObjectId.hpp"
#include "PyObjectIterator.hpp"
#include "PyEnum.hpp"
#include "PyClassFields.hpp"

namespace db0::python

{
    
    PyToolkit::TypeManager PyToolkit::m_type_manager;
    PyToolkit::PyWorkspace PyToolkit::m_py_workspace;
    
    std::string PyToolkit::getTypeName(ObjectPtr py_object) {
        return getTypeName(Py_TYPE(py_object));
    }
    
    std::string PyToolkit::getTypeName(TypeObjectPtr py_type) {
        return std::string(py_type->tp_name);
    }

    std::string PyToolkit::getModuleName(TypeObjectPtr py_type)
    {
        auto py_module_name = PyObject_GetAttrString(reinterpret_cast<ObjectPtr>(py_type), "__module__");
        if (!py_module_name) {
            THROWF(db0::InputException) << "Could not get module name for class " << getTypeName(py_type);
        }
        auto result = std::string(PyUnicode_AsUTF8(py_module_name));
        Py_DECREF(py_module_name);
        return result;
    }
    
    PyToolkit::ObjectPtr PyToolkit::unloadObject(db0::swine_ptr<Fixture> &fixture, std::uint64_t address,
        const ClassFactory &class_factory, std::optional<std::uint32_t> instance_id, std::shared_ptr<Class> expected_type)
    {
        auto stem = db0::object_model::Object::unloadStem(fixture, address, instance_id);
        auto type = db0::object_model::unloadClass(stem->m_class_ref, class_factory);
        if (expected_type && type != expected_type) {
            THROWF(db0::InputException) << "Type mismatch";
        }
        
        // construct Python's memo object (placeholder for actual DBZero instance)
        auto memo_object = MemoObjectStub_new(type->getLangClass().steal());
        // unload from stem
        db0::object_model::Object::unload(&memo_object->ext(), std::move(stem), type);

        return memo_object;
    }
    
    PyToolkit::ObjectPtr PyToolkit::unloadObject(db0::swine_ptr<Fixture> &, std::uint64_t address,
        std::shared_ptr<Class> type, std::optional<std::uint32_t> instance_id)
    {
        auto memo_object = MemoObjectStub_new(type->getLangClass().steal());        
        db0::object_model::Object::unload(&memo_object->ext(), address, type);
        return memo_object;
    }
    
    PyToolkit::ObjectPtr PyToolkit::unloadBlock(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }

        auto block_object = BlockDefaultObject_new();
        // retrieve actual DBZero instance        
        db0::object_model::pandas::Block::unload(&block_object->ext(), fixture, address);
        // add blockobject to cache
        lang_cache.add(address, block_object, false);
        return block_object;
    }
    
    PyToolkit::ObjectPtr PyToolkit::unloadList(db0::swine_ptr<Fixture> &fixture, std::uint64_t address,
        std::optional<std::uint32_t> instance_id)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }
        
        auto list_object = ListDefaultObject_new();
        // retrieve actual DBZero instance        
        db0::object_model::List::unload(&list_object->ext(), fixture, address);
        
        // validate instance_id
        if (instance_id && *instance_id != list_object->ext()->m_instance_id) {
            Py_DECREF(list_object);
            THROWF(db0::InputException) << "Instance IDs don't match";
        }

        // add list object to cache
        lang_cache.add(address, list_object, false);
        return list_object;
    }
    
    PyToolkit::ObjectPtr PyToolkit::unloadIndex(db0::swine_ptr<Fixture> &fixture, std::uint64_t address,
        std::optional<std::uint32_t> instance_id)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }
        
        auto index_object = IndexDefaultObject_new();
        // retrieve actual DBZero instance        
        db0::object_model::Index::unload(&index_object->ext(), fixture, address);
        
        // validate instance_id
        if (instance_id && *instance_id != index_object->ext()->m_instance_id) {
            Py_DECREF(index_object);
            THROWF(db0::InputException) << "Instance IDs don't match";
        }

        // add list object to cache
        lang_cache.add(address, index_object, false);
        return index_object;
    }

    PyToolkit::ObjectPtr PyToolkit::unloadSet(db0::swine_ptr<Fixture> &fixture, std::uint64_t address,
        std::optional<std::uint32_t> instance_id)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }
        
        auto set_object = SetDefaultObject_new();
        // retrieve actual DBZero instance        
        db0::object_model::Set::unload(&set_object->ext(), fixture, address);
    

        // add list object to cache
        lang_cache.add(address, set_object, false);
        return set_object;
    }
    
    PyToolkit::ObjectPtr PyToolkit::unloadDict(db0::swine_ptr<Fixture> &fixture, std::uint64_t address,
        std::optional<std::uint32_t> instance_id)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }
        
        auto dict_object = DictDefaultObject_new();
        // retrieve actual DBZero instance        
        db0::object_model::Dict::unload(&dict_object->ext(), fixture, address);
    
        // add list object to cache
        lang_cache.add(address, dict_object, false);
        return dict_object;
    }
    
    PyToolkit::ObjectPtr PyToolkit::unloadTuple(db0::swine_ptr<Fixture> &fixture, std::uint64_t address,
        std::optional<std::uint32_t> instance_id)
    {
        // try pulling from cache first
        auto &lang_cache = fixture->getLangCache();
        auto object_ptr = lang_cache.get(address);
        if (object_ptr) {
            // return from cache
            return object_ptr;
        }
        
        auto tuple_object = TupleDefaultObject_new();
        // retrieve actual DBZero instance        
        db0::object_model::Tuple::unload(&tuple_object->ext(), fixture, address);

        // add list object to cache
        lang_cache.add(address, tuple_object, false);
        return tuple_object;
    }

    PyToolkit::ObjectPtr PyToolkit::unloadObjectIterator(db0::swine_ptr<Fixture> &fixture, std::vector<std::byte>::const_iterator &iter,
            std::vector<std::byte>::const_iterator end)
    {
        auto obj_iter = db0::object_model::ObjectIterator::deserialize(fixture, iter, end);
        auto py_iter = PyObjectIteratorDefault_new();
        Iterator::makeNew(&py_iter->ext(), std::move(obj_iter));
        return py_iter;
    }
    
    std::uint64_t PyToolkit::addTag(ObjectPtr py_object, db0::pools::RC_LimitedStringPool &string_pool)
    {
        if (PyUnicode_Check(py_object)) {
            return string_pool.toAddress(string_pool.add(PyUnicode_AsUTF8(py_object)));
        }

        // unable to resolve as tag
        THROWF(db0::InputException) << "Unable to resolve object as tag" << THROWF_END;        
    }
    
    bool PyToolkit::isIterable(ObjectPtr py_object) {
        return Py_TYPE(py_object)->tp_iter != nullptr;
    }

    bool PyToolkit::isString(ObjectPtr py_object) {
        return PyUnicode_Check(py_object);
    }

    PyToolkit::ObjectSharedPtr PyToolkit::getIterator(ObjectPtr py_object)
    {
        auto py_iterator = PyObject_GetIter(py_object);
        if (!py_iterator) {
            THROWF(db0::InputException) << "Unable to get iterator for object" << THROWF_END;
        }
        return { py_iterator, false };
    }

    PyToolkit::ObjectSharedPtr PyToolkit::next(ObjectPtr py_object)
    {
        auto py_next = PyIter_Next(py_object);
        if (!py_next) {
            // StopIteration exception raised
            PyErr_Clear();
        }

        return { py_next, false };
    }
    
    bool PyToolkit::isSingleton(TypeObjectPtr py_type) {
        return PyMemoType_IsSingleton(py_type);
    }
    
    bool PyToolkit::isType(ObjectPtr py_object) {
        return PyType_Check(py_object);
    }

    bool PyToolkit::isMemoObject(ObjectPtr py_object) {
        return PyMemo_Check(py_object);
    }

    PyToolkit::ObjectPtr PyToolkit::getUUID(ObjectPtr py_object) {
        return db0::python::getUUID(nullptr, &py_object, 1);
    }
    
    bool PyToolkit::isEnumValue(ObjectPtr py_object) {
        return PyEnumValue_Check(py_object);
    }

    bool PyToolkit::isFieldDef(ObjectPtr py_object) {
        return PyFieldDef_Check(py_object);
    }
    
    PyToolkit::ObjectPtr PyToolkit::unloadEnumValue(const EnumValue &value) {
        return makePyEnumValue(value);
    }

}