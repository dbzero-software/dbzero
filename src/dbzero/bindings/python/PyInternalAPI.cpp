#include "PyInternalAPI.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/value/TypeUtils.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/WorkspaceView.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/workspace/Utils.hpp>
#include <dbzero/object_model/tags/ObjectIterator.hpp>
#include <dbzero/object_model/tags/TypedObjectIterator.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/tags/QueryObserver.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include "PyToolkit.hpp"
#include "PyObjectIterator.hpp"

namespace db0::python

{
    
    ObjectId extractObjectId(PyObject *args)
    {
        // extact ObjectId from args
        PyObject *py_object_id;
        if (!PyArg_ParseTuple(args, "O", &py_object_id)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        if (!ObjectId_Check(py_object_id)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        return *reinterpret_cast<ObjectId*>(py_object_id);
    }
    
    PyObject *tryFetchFrom(db0::Snapshot &snapshot, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs < 1 || nargs > 2) {
            PyErr_SetString(PyExc_TypeError, "fetch requires 1 or 2 arguments");
            return NULL;
        }

        PyTypeObject *type_arg = nullptr;
        PyObject *uuid_arg = nullptr;
        if (nargs == 1) {
            uuid_arg = args[0];
        } else {
            if (!PyType_Check(args[0])) {
                PyErr_SetString(PyExc_TypeError, "Invalid argument type");
                return NULL;
            }
            type_arg = reinterpret_cast<PyTypeObject*>(args[0]);
            uuid_arg = args[1];
        }
        
        // decode ObjectId from string
        if (PyUnicode_Check(uuid_arg)) {
            auto uuid = PyUnicode_AsUTF8(uuid_arg);
            return fetchObject(snapshot, ObjectId::fromBase32(uuid), type_arg);
        }
        
        if (PyType_Check(uuid_arg)) {
            auto uuid_type = reinterpret_cast<PyTypeObject*>(uuid_arg);
            // check if type_arg is exact or a base of uuid_arg
            if (type_arg && !isBase(uuid_type, reinterpret_cast<PyTypeObject*>(type_arg))) {
                PyErr_SetString(PyExc_TypeError, "Type mismatch");
                return NULL;
            }
            return fetchSingletonObject(snapshot, uuid_type);
        }

        PyErr_SetString(PyExc_TypeError, "Invalid argument type");
        return NULL;        
    }

    PyObject *fetchObject(db0::swine_ptr<Fixture> &fixture, ObjectId object_id, PyTypeObject *py_expected_type)
    {   
        using ClassFactory = db0::object_model::ClassFactory;
        using Class = db0::object_model::Class;

        auto storage_class = object_id.m_typed_addr.getType();
        auto addr = object_id.m_typed_addr;
        auto &lang_cache = fixture->getLangCache();

        // validate storage class first
        if (py_expected_type) {
            auto type_id = PyToolkit::getTypeManager().getTypeId(py_expected_type);
            if (storage_class != db0::object_model::TypeUtils::m_storage_class_mapper.getStorageClass(type_id)) {
                PyErr_SetString(PyExc_TypeError, "Object ID type mismatch");
                return NULL;
            }
        }
        
        if (storage_class == db0::object_model::StorageClass::OBJECT_REF) {
            auto &class_factory = fixture->get<ClassFactory>();
            std::shared_ptr<Class> type;
            // find class associated class with the ClassFactory
            if (py_expected_type) {
                type = class_factory.getExistingType(py_expected_type);
            }
            // try pulling from cache first
            auto object_ptr = lang_cache.get(addr);
            if (object_ptr) {
                // validate type if requested
                if (type) {
                    MemoObject *ptr = reinterpret_cast<MemoObject*>(object_ptr);
                    if (ptr->ext().getType() != *type) {
                        PyErr_SetString(PyExc_TypeError, "Object ID type mismatch");
                        return NULL;
                    }
                }                
                Py_INCREF(object_ptr);
                return object_ptr;
            }

            // unload with instance_id validation & optional type validation
            object_ptr = PyToolkit::unloadObject(fixture, addr, class_factory, object_id.m_instance_id, type);
            // keep in cache (weak reference only)
            fixture->getLangCache().add(addr, object_ptr, false);
            return object_ptr;
        }

        if (storage_class == db0::object_model::StorageClass::DB0_LIST) {
            // unload with instance_id validation
            return PyToolkit::unloadList(fixture, object_id.m_typed_addr, object_id.m_instance_id);
        } 
        
        if (storage_class == db0::object_model::StorageClass::DB0_INDEX) {
            // unload with instance_id validation
            return PyToolkit::unloadIndex(fixture, object_id.m_typed_addr, object_id.m_instance_id);
        } 

        PyErr_SetString(PyExc_TypeError, "Invalid object ID");
        return NULL;
    }
    
    PyObject *fetchSingletonObject(db0::swine_ptr<Fixture> &fixture, PyTypeObject *py_type)
    {
        auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
        // find class associated class with the ClassFactory
        auto type = class_factory.getExistingType(py_type);
        if (!type->isSingleton()) {
            THROWF(db0::InputException) << "Not a DBZero singleton type";
        }

        if (!type->isExistingSingleton()) {
            THROWF(db0::InputException) << "Singleton instance does not exist";
        }

        MemoObject *memo_obj = reinterpret_cast<MemoObject*>(py_type->tp_alloc(py_type, 0));
        type->unloadSingleton(&memo_obj->ext());
        return memo_obj;
    }
        
    PyObject *fetchSingletonObject(db0::Snapshot &snapshot, PyTypeObject *py_type)
    {
        auto uuid = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture()->getUUID();
        auto fixture = snapshot.getFixture(uuid, AccessType::READ_ONLY);
        return fetchSingletonObject(fixture, py_type);
    }
    
    void renameField(PyTypeObject *py_type, const char *from_name, const char *to_name)
    {
        assert(from_name);
        assert(to_name);
        using ClassFactory = db0::object_model::ClassFactory;

        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        auto &class_factory = fixture->get<ClassFactory>();
        // resolve existing DB0 type from python type
        auto type = class_factory.getExistingType(py_type);
        type->renameField(from_name, to_name);
    }
    
#ifndef NDEBUG    

    PyObject *writeBytes(PyObject *self, PyObject *args)
    {
        // extract string from args
        const char *data;
        if (!PyArg_ParseTuple(args, "s", &data)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        auto addr = db0::writeBytes(*fixture, data, strlen(data));
        return PyLong_FromUnsignedLongLong(addr);        
    }
    
    PyObject *freeBytes(PyObject *, PyObject *args)
    {
        // extract address from args
        std::uint64_t address;
        if (!PyArg_ParseTuple(args, "K", &address)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        db0::freeBytes(*fixture, address);
        Py_RETURN_NONE;
    }

    PyObject *readBytes(PyObject *, PyObject *args)
    {
        // extract address from args
        std::uint64_t address;
        if (!PyArg_ParseTuple(args, "K", &address)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
        std::string str_data = db0::readBytes(*fixture, address);
        return PyUnicode_FromString(str_data.c_str());
    }
    
#endif

    bool isBase(PyTypeObject *py_type, PyTypeObject *base_type)
    {
        return PyObject_IsSubclass(reinterpret_cast<PyObject*>(py_type), reinterpret_cast<PyObject*>(base_type));
    }

    PyObject *fetchObject(db0::Snapshot &snapshot, ObjectId object_id, PyTypeObject *py_expected_type)
    {        
        auto fixture = snapshot.getFixture(object_id.m_fixture_uuid, AccessType::READ_ONLY);
        assert(fixture);
        fixture->refreshIfUpdated();
        // open from specific fixture
        return fetchObject(fixture, object_id, py_expected_type);
    }
    
    PyObject *findIn(db0::Snapshot &snapshot, PyObject* const *args, Py_ssize_t nargs)
    {
        using ObjectIterator = db0::object_model::ObjectIterator;
        using TypedObjectIterator = db0::object_model::TypedObjectIterator;
        using TagIndex = db0::object_model::TagIndex;
        using Class = db0::object_model::Class;

        std::shared_ptr<Class> type;
        auto fixture = snapshot.getCurrentFixture();
        auto &tag_index = fixture->get<TagIndex>();
        std::vector<std::unique_ptr<db0::object_model::QueryObserver> > query_observers;
        auto query_iterator = tag_index.find(args, nargs, type, query_observers);
        if (type) {
            // construct as typed iterator when a type was specified
            auto iter_obj = PyTypedObjectIterator_new(&PyTypedObjectIteratorType, NULL, NULL);
            TypedObjectIterator::makeNew(&iter_obj->ext(), fixture, std::move(query_iterator), 
                type, std::move(query_observers));
            return iter_obj;
        } else {
            auto iter_obj = PyObjectIterator_new(&PyObjectIteratorType, NULL, NULL);
            ObjectIterator::makeNew(&iter_obj->ext(), fixture, std::move(query_iterator), 
                std::move(query_observers));
            return iter_obj;
        }
    }

    PyObject *trySerialize(PyObject *py_serializable)
    {
        using TypeId = db0::bindings::TypeId;
        std::vector<std::byte> bytes;
        auto type_id = PyToolkit::getTypeManager().getTypeId(py_serializable);
        db0::serial::write(bytes, type_id);
        
        if (type_id == TypeId::OBJECT_ITERATOR) {
            reinterpret_cast<PyObjectIterator*>(py_serializable)->ext().serialize(bytes);
        } else {
            THROWF(db0::InputException) << "Unsupported or non-serializable type: " 
                << static_cast<int>(type_id) << THROWF_END;
        }
                
        return PyBytes_FromStringAndSize(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    }
    
    PyObject *tryDeserialize(db0::Snapshot *workspace, PyObject *py_bytes)
    {
        using TypeId = db0::bindings::TypeId;

        if (!PyBytes_Check(py_bytes)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type (expected bytes)");
            return NULL;
        }

        Py_ssize_t size;
        char *data = nullptr;
        PyBytes_AsStringAndSize(py_bytes, &data, &size);
        // extract bytes
        std::vector<std::byte> bytes(size);
        std::copy(data, data + size, reinterpret_cast<char*>(bytes.data()));
        
        auto fixture = workspace->getCurrentFixture();
        auto iter = bytes.cbegin(), end = bytes.cend();
        auto type_id = db0::serial::read<TypeId>(iter, end);
        if (type_id == TypeId::OBJECT_ITERATOR) {
            return PyToolkit::unloadObjectIterator(fixture, iter, end);            
        } else {
            THROWF(db0::InputException) << "Unsupported serialized type id: " 
                << static_cast<int>(type_id) << THROWF_END;
        }
    }

    PyObject *_PyObject_GetDescrOptional(PyObject *obj, PyObject *name)
    {
        // This implementation is based on _PyObject_GenericGetAttrWithDict function
        PyTypeObject *tp = Py_TYPE(obj);
        PyObject *descr = NULL;
        PyObject *res = NULL;
        descrgetfunc f;

        if (!PyUnicode_Check(name)) {
            PyErr_Format(PyExc_TypeError,
                        "attribute name must be string, not '%.200s'",
                        Py_TYPE(name)->tp_name);
            return NULL;
        }
        Py_INCREF(name);

        descr = _PyType_Lookup(tp, name);
        f = NULL;
        if (descr != NULL) {
            Py_INCREF(descr);
            f = Py_TYPE(descr)->tp_descr_get;
            if (f != NULL /*&& PyDescr_IsData(descr)*/) {
                res = f(descr, obj, (PyObject *)Py_TYPE(obj));
                if (res == NULL && PyErr_ExceptionMatches(PyExc_AttributeError)) {
                    PyErr_Clear();
                }
                goto done;
            }
        }
    done:
        Py_XDECREF(descr);
        Py_DECREF(name);
        return res;
    }
    
}