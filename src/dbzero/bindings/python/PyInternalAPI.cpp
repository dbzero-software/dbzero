#include "PyInternalAPI.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/value/TypeUtils.hpp>
#include <dbzero/object_model/index/Index.hpp>
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
#include <dbzero/core/memory/SlabAllocator.hpp>
#include "PyToolkit.hpp"
#include "PyObjectIterator.hpp"
#include "Memo.hpp"
#include "PyClass.hpp"

namespace db0::python

{
    
    std::mutex py_api_mutex;

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
    
    shared_py_object<PyObject*> tryFetchFrom(db0::Snapshot &snapshot, PyObject *const *args, Py_ssize_t nargs)
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
                return nullptr;
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
                THROWF(db0::InputException) << "Type mismatch";                                
            }
            return fetchSingletonObject(snapshot, uuid_type);
        }

        THROWF(db0::InputException) << "Invalid argument type" << THROWF_END;
    }
    
    shared_py_object<PyObject*> fetchObject(db0::swine_ptr<Fixture> &fixture, ObjectId object_id,
        PyTypeObject *py_expected_type)
    {   
        using ClassFactory = db0::object_model::ClassFactory;
        using Class = db0::object_model::Class;

        auto storage_class = object_id.m_typed_addr.getType();
        // use logical address to access the object
        auto addr = db0::makeLogicalAddress(object_id.m_typed_addr, object_id.m_instance_id);

        // validate storage class first
        if (py_expected_type) {
            auto type_id = PyToolkit::getTypeManager().getTypeId(py_expected_type);
            if (storage_class != db0::object_model::TypeUtils::m_storage_class_mapper.getStorageClass(type_id)) {
                THROWF(db0::InputException) << "Object ID type mismatch";
            }
        }
        
        if (storage_class == db0::object_model::StorageClass::OBJECT_REF) {
            auto &class_factory = fixture->get<ClassFactory>();
            std::shared_ptr<Class> type;
            // find instance associated class with the ClassFactory
            if (py_expected_type) {
                type = class_factory.getExistingType(py_expected_type);
            }
            
            // unload with optional type validation
            return PyToolkit::unloadObject(fixture, addr, class_factory, type);
        } else if (storage_class == db0::object_model::StorageClass::DB0_LIST) {
            // unload by logical address
            return PyToolkit::unloadList(fixture, addr);
        } else if (storage_class == db0::object_model::StorageClass::DB0_INDEX) {
            // unload by logical address
            return PyToolkit::unloadIndex(fixture, addr);
        } else if (storage_class == db0::object_model::StorageClass::DB0_CLASS) {
            auto &class_factory = fixture->get<ClassFactory>();
            // return as Python native type (if it can be located)
            auto class_ptr = class_factory.getConstTypeByPtr(db0_ptr_reinterpret_cast<Class>()(addr));
            if (class_ptr->hasLangClass()) {
                // return as a Python type
                return (PyObject*)class_ptr->getLangClass().steal();
            } else {
                // return as a DBZero class instance
                return makeClass(class_ptr);
            }
        }
        
        THROWF(db0::InputException) << "Invalid object ID" << THROWF_END;
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
        type->unloadSingleton(&memo_obj->modifyExt());
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
        std::lock_guard api_lock(py_api_mutex);
        using ClassFactory = db0::object_model::ClassFactory;
        auto &decor = *reinterpret_cast<MemoTypeDecoration*>((char*)py_type + sizeof(PyHeapTypeObject));        

        assert(PyMemoType_Check(py_type));        
        assert(from_name);
        assert(to_name);
        
        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getFixture(decor.m_fixture_uuid, AccessType::READ_WRITE));
        auto &class_factory = lock->get<ClassFactory>();
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

        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        auto addr = db0::writeBytes(*lock, data, strlen(data));
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

        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        db0::freeBytes(*lock, address);
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

        db0::FixtureLock lock(PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture());
        std::string str_data = db0::readBytes(*lock, address);
        return PyUnicode_FromString(str_data.c_str());
    }
    
#endif

    bool isBase(PyTypeObject *py_type, PyTypeObject *base_type) {
        return PyObject_IsSubclass(reinterpret_cast<PyObject*>(py_type), reinterpret_cast<PyObject*>(base_type));
    }

    shared_py_object<PyObject*> fetchObject(db0::Snapshot &snapshot, ObjectId object_id, PyTypeObject *py_expected_type)
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
        
        std::size_t args_offset = 0;
        bool no_result = false;
        std::shared_ptr<Class> type;
        auto fixture = db0::object_model::getFindParams(snapshot, args, nargs, args_offset, type, no_result);
        auto &tag_index = fixture->get<TagIndex>();
        std::vector<std::unique_ptr<db0::object_model::QueryObserver> > query_observers;
        auto query_iterator = tag_index.find(args + args_offset, nargs - args_offset, type, query_observers, no_result);
        auto iter_obj = PyObjectIteratorDefault_new();
        if (type) {
            // construct as typed iterator when a type was specified
            auto typed_iter = std::make_unique<TypedObjectIterator>(fixture, std::move(query_iterator), 
                type, std::move(query_observers));
            Iterator::makeNew(&(iter_obj.get())->modifyExt(), std::move(typed_iter));
        } else {
            auto _iter = std::make_unique<ObjectIterator>(fixture, std::move(query_iterator), 
                std::move(query_observers));
            Iterator::makeNew(&(iter_obj.get())->modifyExt(), std::move(_iter));
        }
        return iter_obj.steal();
    }

    PyObject *trySerialize(PyObject *py_serializable)
    {
        using TypeId = db0::bindings::TypeId;
        std::vector<std::byte> bytes;
        auto type_id = PyToolkit::getTypeManager().getTypeId(py_serializable);
        db0::serial::write(bytes, type_id);
        
        if (type_id == TypeId::OBJECT_ITERATOR) {
            reinterpret_cast<PyObjectIterator*>(py_serializable)->ext()->serialize(bytes);
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
            return PyToolkit::unloadObjectIterator(fixture, iter, end).steal();
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
    
    PyObject *getSlabMetrics(const db0::SlabAllocator &slab)
    {
        std::lock_guard api_lock(py_api_mutex);
        PyObject *py_dict = PyDict_New();
        PyDict_SetItemString(py_dict, "size", PyLong_FromUnsignedLong(slab.getSlabSize()));
        PyDict_SetItemString(py_dict, "admin_space_size", PyLong_FromUnsignedLong(slab.getAdminSpaceSize(true)));
        PyDict_SetItemString(py_dict, "remaining_capacity", PyLong_FromUnsignedLong(slab.getRemainingCapacity()));
        PyDict_SetItemString(py_dict, "max_alloc_size", PyLong_FromUnsignedLong(slab.getMaxAllocSize()));
        return py_dict;
    }

    PyObject *tryGetSlabMetrics(db0::Workspace *workspace)
    {
        auto fixture = workspace->getCurrentFixture();        
        PyObject *py_dict = PyDict_New();
        auto get_slab_metrics = [py_dict](const db0::SlabAllocator &slab, std::uint32_t slab_id) {
            // report remaining capacity as dict item
            PyObject *py_slab_id = PyLong_FromUnsignedLong(slab_id);
            PyDict_SetItem(py_dict, py_slab_id, getSlabMetrics(slab));
        };
        fixture->forAllSlabs(get_slab_metrics);
        return py_dict;
    }

    PyObject *trySetCacheSize(db0::Workspace *workspace, std::size_t new_cache_size) 
    {
        workspace->setCacheSize(new_cache_size);
        Py_RETURN_NONE;
    }
    
    // MEMO_OBJECT specialization
    template <> void dropInstance<TypeId::MEMO_OBJECT>(PyObject *py_wrapper) {
        MemoObject_drop(reinterpret_cast<MemoObject*>(py_wrapper));                
    }

    // DB0_INDEX specialization
    template <> void dropInstance<TypeId::DB0_INDEX>(PyObject *py_wrapper) {
        PyWrapper_drop(reinterpret_cast<IndexObject*>(py_wrapper));
    }

    void registerDropInstanceFunctions(std::vector<void (*)(PyObject *)> &functions)
    {
        using TypeId = db0::bindings::TypeId;
        functions.resize(static_cast<int>(TypeId::COUNT));
        std::fill(functions.begin(), functions.end(), nullptr);
        functions[static_cast<int>(TypeId::MEMO_OBJECT)] = dropInstance<TypeId::MEMO_OBJECT>;
    }
    
    void dropInstance(db0::bindings::TypeId type_id, PyObject *py_instance)
    {        
        // type-id specializations
        using DropInstanceFunc = void (*)(PyObject *);
        static std::vector<DropInstanceFunc> drop_instance_functions;
        if (drop_instance_functions.empty()) {
            registerDropInstanceFunctions(drop_instance_functions);
        }
        
        assert(static_cast<int>(type_id) < drop_instance_functions.size());
        if (drop_instance_functions[static_cast<int>(type_id)]) {
            drop_instance_functions[static_cast<int>(type_id)](py_instance);
        }
    }
        
}