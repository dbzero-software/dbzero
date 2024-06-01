#include "PyAPI.hpp"
#include "PyInternalAPI.hpp"
#include "PyToolkit.hpp"
#include "PyEnum.hpp"
#include "ObjectId.hpp"
#include "PyTypeManager.hpp"
#include "PyWorkspace.hpp"
#include "Memo.hpp"
#include "PySnapshot.hpp"
#include "PyInternalAPI.hpp"
#include "PyObjectIterator.hpp"
#include "Memo.hpp"
#include <dbzero/bindings/python/collections/List.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <dbzero/object_model/tags/QueryObserver.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>


namespace db0::python

{

    PyObject *cacheStats(PyObject *, PyObject *)
    {
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        auto &cache_recycler = workspace.getCacheRecycler();
        
        PyObject* dict = PyDict_New();
        if (dict == NULL) {
            PyErr_SetString(PyExc_MemoryError, "Failed to create a dictionary.");
            return NULL;
        }
        
        PyDict_SetItemString(dict, "size", PyLong_FromLong(cache_recycler.size()));
        PyDict_SetItemString(dict, "capacity", PyLong_FromLong(cache_recycler.getCapacity()));
        return dict;
    }

    PyObject *clearCache(PyObject *, PyObject *)
    {
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        auto &cache_recycler = const_cast<CacheRecycler&>(workspace.getCacheRecycler());
        cache_recycler.clear();
        Py_RETURN_NONE;
    }
        
    PyObject *tryFetch(PyObject *const *args, Py_ssize_t nargs) {
        return tryFetchFrom(PyToolkit::getPyWorkspace().getWorkspace(), args, nargs);
    }

    PyObject *fetch(PyObject *, PyObject *const *args, Py_ssize_t nargs) {
        return runSafe(tryFetch, args, nargs);
    }

    PyObject *tryOpen(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        // prefix_name, open_mode, autocommit (bool)
        static const char *kwlist[] = {"prefix_name", "open_mode", "autocommit", NULL};
        const char *prefix_name = nullptr;
        const char *open_mode = nullptr;
        PyObject *py_autocommit = nullptr;
        if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|sO", const_cast<char**>(kwlist), &prefix_name, &open_mode, &py_autocommit)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        bool autocommit = py_autocommit ? PyObject_IsTrue(py_autocommit) : true;
        auto access_type = open_mode ? parseAccessType(open_mode) : db0::AccessType::READ_WRITE;
        PyToolkit::getPyWorkspace().open(prefix_name, access_type, autocommit);
        Py_RETURN_NONE;
    }

    PyObject *open(PyObject *self, PyObject *args, PyObject *kwargs)
    {
        return runSafe(tryOpen, self, args, kwargs);
    }

    PyObject *tryInit(PyObject *self, PyObject *args)
    {
        // extract optional "path" string argument
        const char *path = "";
        if (!PyArg_ParseTuple(args, "|s", &path)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        PyToolkit::getPyWorkspace().initWorkspace(path);
        Py_RETURN_NONE;
    }
    
    PyObject *init(PyObject *self, PyObject *args)
    {
        return runSafe(tryInit, self, args);
    }

    PyObject *tryDrop(PyObject *self, PyObject *args)
    {
        // extract prefix_name & optional if_exists
        const char *prefix_name = nullptr;
        int if_exists = true;
        if (!PyArg_ParseTuple(args, "s|p", &prefix_name, &if_exists)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        PyToolkit::getPyWorkspace().getWorkspace().drop(prefix_name, if_exists);
        Py_RETURN_NONE;
    }
    
    PyObject *drop(PyObject *self, PyObject *args)
    {
        return runSafe(tryDrop, self, args);
    }
    
    PyObject *tryCommit(PyObject *, PyObject *args)
    {
        // extract optional prefix name
        const char *prefix_name = nullptr;
        if (!PyArg_ParseTuple(args, "|s", &prefix_name)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        if (prefix_name) {
            PyToolkit::getPyWorkspace().getWorkspace().commit(prefix_name);
        } else {
            PyToolkit::getPyWorkspace().getWorkspace().commit();
        }
        Py_RETURN_NONE;
    }
    
    PyObject *commit(PyObject *self, PyObject *args)
    {
        return runSafe(tryCommit, self, args);
    }

    PyObject *tryClose(PyObject *, PyObject *args)
    {
        // extract optional prefix name
        const char *prefix_name = nullptr;
        if (!PyArg_ParseTuple(args, "|s", &prefix_name)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        if (prefix_name) {
            PyToolkit::getPyWorkspace().getWorkspace().close(prefix_name);
        } else {
            PyToolkit::getPyWorkspace().close();
        }
        Py_RETURN_NONE;
    }
    
    PyObject *close(PyObject *self, PyObject *args) {
        return runSafe(tryClose, self, args);
    }
        
    PyObject *getPrefixName(PyObject *self, PyObject *args)
    {
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }
        
        db0::swine_ptr<Fixture> fixture;
        if (PyMemo_Check(py_object)) {
            fixture = reinterpret_cast<MemoObject*>(py_object)->ext().getFixture();            
        } else if (ListObject_Check(py_object)) {
            fixture = reinterpret_cast<ListObject*>(py_object)->ext().getFixture();            
        } else {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        return PyUnicode_FromString(fixture->getPrefix().getName().c_str());
    }
    
    PyObject *getCurrentPrefixName(PyObject *, PyObject *)
    {
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getCurrentFixture();
        return PyUnicode_FromString(fixture->getPrefix().getName().c_str());
    }
    
    PyObject *tryDel(PyObject *self, PyObject *args)
    {
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (!PyMemo_Check(py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        MemoObject_drop(reinterpret_cast<MemoObject*>(py_object));
        Py_RETURN_NONE;
    }

    PyObject *del(PyObject *self, PyObject *args)
    {
        return runSafe(tryDel, self, args);
    }
    
    PyObject *tryRefresh(PyObject *self, PyObject *args)
    {
        PyToolkit::getPyWorkspace().refresh();
        Py_RETURN_NONE;
    }
    
    PyObject *refresh(PyObject *self, PyObject *args)
    {
        return runSafe(tryRefresh, self, args);
    }

    PyObject *getStateNum(PyObject *self, PyObject *args)
    { 
        const char *prefix_name = nullptr;
        // parse default prefix name
        if (!PyArg_ParseTuple(args, "|s", &prefix_name)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        db0::swine_ptr<Fixture> fixture;
        if (prefix_name) {
            fixture = workspace.findFixture(prefix_name);
        } else {
            fixture = workspace.getCurrentFixture();            
        }
        
        fixture->refreshIfUpdated();
        return PyLong_FromLong(fixture->getPrefix().getStateNum());
    }
    
    PyObject *tryGetDBMetrics(PyObject *self, PyObject *args)
    {
        // construct python List
        PyObject *py_list = PyList_New(0);
        if (!py_list) {
            PyErr_SetString(PyExc_MemoryError, "Failed to create a list.");
            return NULL;
        }

        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        workspace.forAll([&py_list](auto &fixture) {
            auto dict_item = PyDict_New();
            if (dict_item == NULL) {
                PyErr_SetString(PyExc_MemoryError, "Failed to create a dictionary.");
                return;
            }

            PyDict_SetItemString(dict_item, "name", PyUnicode_FromString(fixture.getPrefix().getName().c_str()));
            PyDict_SetItemString(dict_item, "uuid", PyLong_FromLong(fixture.getUUID()));
            PyDict_SetItemString(dict_item, "vptr_reg_size", PyLong_FromLong(fixture.getGC0().size()));
            PyDict_SetItemString(dict_item, "py_cache_size", PyLong_FromLong(fixture.getLangCache().size()));
            PyList_Append(py_list, dict_item);
        });
        
        return py_list;
    }

    PyObject *getDBMetrics(PyObject *self, PyObject *args) {
        return runSafe(tryGetDBMetrics, self, args);
    }
    
    PyObject *getSnapshot(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
        return runSafe(tryGetSnapshot, self, args, nargs);
    }

    PyObject *tryDescribeObject(PyObject *self, PyObject *args)
    {
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (!PyMemo_Check(py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        return MemoObject_DescribeObject(reinterpret_cast<MemoObject*>(py_object));
    }
    
    PyObject *describeObject(PyObject *self, PyObject *args)
    {
        return runSafe(tryDescribeObject, self, args);
    }
    
    PyObject *tryRenameField(PyObject *args)
    {
        // extract 3 required arguments: class, from name, to name
        PyTypeObject *py_type;        
        const char *from_name = nullptr;
        const char *to_name = nullptr;
        if (!PyArg_ParseTuple(args, "Oss", &py_type, &from_name, &to_name)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        // check if py type
        if (!PyType_Check(py_type)) {
            PyErr_SetString(PyExc_TypeError, "First argument must be a type");
            return nullptr;
        }

        renameField(py_type, from_name, to_name);
        Py_RETURN_NONE;       
    }
    
    PyObject *renameField(PyObject *, PyObject *args)
    {
        return runSafe(tryRenameField, args);
    }

    PyObject *isSingleton(PyObject *, PyObject *args)
    {
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (!PyMemo_Check(py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        return (reinterpret_cast<MemoObject*>(py_object)->ext().isSingleton()) ? Py_True : Py_False;
    }

    PyObject *getRefCount(PyObject *, PyObject *args)
    {
        PyObject *py_object;
        if (!PyArg_ParseTuple(args, "O", &py_object)) {
            PyErr_SetString(PyExc_TypeError, "Invalid argument type");
            return NULL;
        }

        if (PyMemo_Check(py_object)) {
            return PyLong_FromLong(reinterpret_cast<MemoObject*>(py_object)->ext().getRefCount());
        }

        PyErr_SetString(PyExc_TypeError, "Invalid argument type");
        return NULL;
    }
    
    PyObject *negTags(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {
        return NULL;
    }

    PyObject *toDict(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "toDict requires exactly 1 argument");
            return NULL;
        }

        if (!PyMemo_Check(args[0])) {
            PyErr_SetString(PyExc_TypeError, "Invalid object type");
            return NULL;
        }
        
        auto &memo_obj = *reinterpret_cast<MemoObject*>(args[0]);
        PyObject *py_result = PyDict_New();
        memo_obj.ext().forAll([py_result](const std::string &key, ObjectSharedPtr py_value) {
            PyDict_SetItemString(py_result, key.c_str(), py_value.steal());
        });
        return py_result;
    }
    
    PyObject *getBuildFlags(PyObject *, PyObject *)
    {
        std::stringstream str_flags;
#ifndef NDEBUG
        str_flags << "D";
#endif  
        return PyUnicode_FromString(str_flags.str().c_str());
    }

    PyObject *pySerialize(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "serialize requires exactly 1 argument");
            return NULL;
        }
        return runSafe(trySerialize, args[0]);
    }
    
    PyObject *pyDeserialize(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "deserialize requires exactly 1 argument");
            return NULL;
        }        
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        return runSafe(tryDeserialize, &workspace, args[0]);
    }
    
    template <> db0::object_model::StorageClass getStorageClass<MemoObject>() {
        return db0::object_model::StorageClass::OBJECT_REF;
    }

    template <> db0::object_model::StorageClass getStorageClass<ListObject>() {
        return db0::object_model::StorageClass::DB0_LIST;
    }

    template <> db0::object_model::StorageClass getStorageClass<IndexObject>() {
        return db0::object_model::StorageClass::DB0_INDEX;
    }
    
    PyObject *makeEnum(PyObject *self, PyObject *const *args, Py_ssize_t nargs)
    {
        if (nargs != 2) {
            PyErr_SetString(PyExc_TypeError, "makeEnum requires exactly 2 arguments");
            return NULL;
        }

        const char *enum_name = PyUnicode_AsUTF8(args[0]);
        // second argument must be an array
        if (!PyList_Check(args[1])) {
            PyErr_SetString(PyExc_TypeError, "Second argument must be a list");
            return NULL;
        }

        std::vector<std::string> enum_values;
        for (Py_ssize_t i = 0; i < PyList_Size(args[1]); ++i) {
            PyObject *py_item = PyList_GetItem(args[1], i);
            if (!PyUnicode_Check(py_item)) {
                PyErr_SetString(PyExc_TypeError, "List must contain only strings");
                return NULL;
            }
            enum_values.push_back(PyUnicode_AsUTF8(py_item));
        }

        return runSafe(tryMakeEnum, self, enum_name, enum_values, nullptr);
    }
    
    using TagIndex = db0::object_model::TagIndex;
    using ObjectIterator = db0::object_model::ObjectIterator;
    using TypedObjectIterator = db0::object_model::TypedObjectIterator;
    using QueryObserver = db0::object_model::QueryObserver;

    std::pair<std::unique_ptr<TagIndex::QueryIterator>, std::vector<std::unique_ptr<QueryObserver> > >
    splitBy(PyObject *py_tag_list, ObjectIterator &iterator)
    {
        std::vector<std::unique_ptr<QueryObserver> > query_observers;
        auto query = iterator.releaseQuery(query_observers);
        auto &tag_index = iterator.getFixture()->get<db0::object_model::TagIndex>();
        auto result = tag_index.splitBy(py_tag_list, std::move(query));
        query_observers.push_back(std::move(result.second));
        return { std::move(result.first), std::move(query_observers) };
    }
    
    PyObject *trySplitBy(PyObject *args, PyObject *kwargs)
    {
        // extract 2 object arguments
        PyObject *py_tag_list;
        PyObject *py_query;
        if (!PyArg_ParseTuple(args, "OO", &py_tag_list, &py_query)) {
            THROWF(db0::InputException) << "Invalid argument type";
        }
        
        if (PyObjectIterator_Check(py_query)) {
            auto &iter = reinterpret_cast<PyObjectIterator*>(py_query)->ext();            
            auto split_query = splitBy(py_tag_list, *iter);
            PyObjectIterator *py_iter = PyObjectIteratorDefault_new();
            // create decorated iterator (either plain or typed)
            if (iter.isTyped()) {
                auto typed_iter = std::make_unique<TypedObjectIterator>(iter->getFixture(), std::move(split_query.first),
                    iter.m_typed_iterator_ptr->getType(), std::move(split_query.second));
                Iterator::makeNew(&py_iter->ext(), std::move(typed_iter));
            } else {
                auto _iter = std::make_unique<ObjectIterator>(iter->getFixture(), std::move(split_query.first), 
                    std::move(split_query.second));
                Iterator::makeNew(&py_iter->ext(), std::move(_iter));
            }            
            return py_iter;
        } else {
            THROWF(db0::InputException) << "Invalid argument type" << THROWF_END;
        }
    }
    
    PyObject *splitBy(PyObject *, PyObject *args, PyObject *kwargs) {
        return runSafe(trySplitBy, args, kwargs);
    }

}