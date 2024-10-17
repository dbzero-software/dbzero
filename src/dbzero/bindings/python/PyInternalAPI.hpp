#pragma once

#include <Python.h>
#include <cstdint>
#include "PyObjectId.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/value/ObjectId.hpp>
#include <dbzero/core/serialization/Serializable.hpp>
#include "shared_py_object.hpp"
#include <type_traits>
#include "PyToolkit.hpp"

namespace db0

{

    class Snapshot;

}

namespace db0::python

{   

    using ObjectId = db0::object_model::ObjectId;
    extern std::mutex py_api_mutex;
    
    /**
     * Extarct full object UUID from python args compatible with db0.open()
    */
    ObjectId extractObjectId(PyObject *args);
    
    PyObject *fetchMemoObject(db0::swine_ptr<Fixture> &, ObjectId);

    PyObject *fetchListObject(db0::swine_ptr<Fixture> &, ObjectId);
    
    /**
     * Open DBZero object from a specific fixture
     * with optional type validation
    */
    shared_py_object<PyObject*> fetchObject(db0::swine_ptr<Fixture> &fixture, ObjectId object_id, 
        PyTypeObject *py_expected_type = nullptr);
    
    void renameField(PyTypeObject *py_type, const char *from_name, const char *to_name);
    
    /**
     * Runs a function, catch exeptions and translate into Python errors
    */
    template <typename... Args, typename T>
    typename std::invoke_result_t<T, Args...> runSafe(T func, Args&&... args)
    {
        try {
            auto result = func(std::forward<Args>(args)...);
            if (PyErr_Occurred()) {
                return NULL;
            }
            return result;
        } catch (const db0::ClassNotFoundException &e) {
            PyErr_SetString(PyToolkit::getTypeManager().getClassNotFoundError(), e.what());
            return NULL;
        } catch (const db0::AbstractException &e) {
            PyErr_SetString(PyExc_RuntimeError, e.what());
            return NULL;
        } catch (const std::exception &e) {
            PyErr_SetString(PyExc_RuntimeError, e.what());
            return NULL;
        } catch (...) {
            PyErr_SetString(PyExc_RuntimeError, "Unknown exception");
            return NULL;
        }
    }
    
    // Universal implementaton for both Workspace and WorkspaceView (aka Snapshot)
    shared_py_object<PyObject*> tryFetchFrom(db0::Snapshot &, PyObject *const *args, Py_ssize_t nargs);
    
    /**
     * Open DBZero object by UUID     
     * @param py_expected_type - expected Python type of the object
    */    
    shared_py_object<PyObject*> fetchObject(db0::Snapshot &, ObjectId object_id, PyTypeObject *py_expected_type = nullptr);

    /**
     * Open DBZero singleton by its corresponding Python type
    */
    PyObject *fetchSingletonObject(db0::Snapshot &, PyTypeObject *py_type);

    /**
     * Universal find implementation (works on Workspace or WorkspaceView)
    */
    PyObject *findIn(db0::Snapshot &, PyObject* const *args, Py_ssize_t nargs);
    
    // Convert a serializable instance to bytes
    PyObject *trySerialize(PyObject *);
    
    // Construct instance from bytes within a specific snapshot's context
    PyObject *tryDeserialize(db0::Snapshot *, PyObject *py_bytes);
    
    PyObject *tryGetSlabMetrics(db0::Workspace *);

    PyObject *trySetCacheSize(db0::Workspace *, std::size_t new_cache_size);
    
    PyObject *_PyObject_GetDescrOptional(PyObject *obj, PyObject *name);

    PyObject *tryGetRefCount(PyObject *);
    
    PyObject *tryGetPrefixStats(PyObject *args, PyObject *kwargs);

    PyObject *tryGetStorageStats(PyObject *args, PyObject *kwargs);
        
    // Retrieve prefix (its Fixture objects) from the optional argument "prefix"
    db0::swine_ptr<Fixture> getPrefixFromArgs(PyObject *args, PyObject *kwargs, const char *param_name);
    
    std::string normalizedPrefixName(const char *prefix_name);
    
#ifndef NDEBUG

    /**
     * A test function to make an allocation and write random bytes into the current prefix
    */
    PyObject *writeBytes(PyObject *, PyObject *args);
    PyObject *freeBytes(PyObject *, PyObject *args);
    PyObject *readBytes(PyObject *, PyObject *args);

#endif
    
    bool isBase(PyTypeObject *py_type, PyTypeObject *base_type);
    
    // drop unreferenced db0 object with a python representation    
    template <db0::bindings::TypeId> void dropInstance(PyObject *);
    // register TypeId specializations
    void registerDropInstanceFunctions(std::vector<void (*)(PyObject *)> &functions);
    void dropInstance(db0::bindings::TypeId type_id, PyObject *);
    
}

