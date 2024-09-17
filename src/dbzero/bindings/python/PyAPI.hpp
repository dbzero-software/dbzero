#pragma once

// This module contains python specific DBZero API implementation
#include <Python.h>
#include <string>
#include "Memo.hpp"
#include <dbzero/bindings/python/collections/PyList.hpp>

namespace db0

{

    class Snapshot;

}

namespace db0::python

{
    
    /**
     * Retrieve cache utilization statistics dict
    */
    PyObject *getCacheStats(PyObject *, PyObject *);
    
    /**
     * Retrieve LangCache utilization statistics dict
     */
    PyObject *getLangCacheStats(PyObject *, PyObject *);

    /**
     * Forwards to CacheRecycler::clear(expired_only)
    */
    PyObject *clearCache(PyObject *, PyObject *);

    /**    
     * Fetch DBZero object instance by its ID or type (in case of a singleton)
     */

    PyObject *fetch(PyObject *, PyObject *const *args, Py_ssize_t nargs);

    /**
     * Initialize DBZero Python bindings
    */    
    PyObject *init(PyObject *self, PyObject *args, PyObject *kwargs);
    
    /**
     * Opens or creates a prefix for read or read/write
    */
    PyObject *open(PyObject *self, PyObject *args, PyObject *kwargs);
    
    PyObject *drop(PyObject *self, PyObject *args);

    PyObject *commit(PyObject *self, PyObject *args);

    PyObject *close(PyObject *self, PyObject *args);
        
    /**
     * Constructs a DBZero list instance
    */
    PyObject *list(PyObject *self, PyObject *args);
    
    /**
     * Get name of the prefix/file associated with a specific instance
    */
    PyObject *getPrefixNameOf(PyObject *self, PyObject *args);
    
    /**
     * Get name of the current/default prefix
    */
    PyObject *getCurrentPrefixName(PyObject *self, PyObject *args);

    /**
     * Delete specific DBZero object instance
    */
    PyObject *del(PyObject *self, PyObject *args);
    
    /**
     * Refresh all open fixtures to update to the latest changes
    */
    PyObject *refresh(PyObject *self, PyObject *args);
    
    /**
     * Get currently active state number associated with a specific prefix/file
    */
    PyObject *getStateNum(PyObject *self, PyObject *args, PyObject *kwargs);
    
    /**
     * Retrieve the active DBZero fixture metrics
    */
    PyObject *getDBMetrics(PyObject *self, PyObject *args);
    
    /**
     * Get DBZero state snapshot
    */
    PyObject *getSnapshot(PyObject *self, PyObject *const *args, Py_ssize_t nargs);

    PyObject *beginAtomic(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    
    /**
     * Describe field layout and output other properties of a specific DBZero object instance
    */
    PyObject *describeObject(PyObject *self, PyObject *args);
    
    PyObject *renameField(PyObject *self, PyObject *args);

    PyObject *isSingleton(PyObject *self, PyObject *args);

    PyObject *getRefCount(PyObject *self, PyObject *args);
    
    // convert to a Python dict
    PyObject *toDict(PyObject *, PyObject *const *args, Py_ssize_t nargs);

    PyObject *getBuildFlags(PyObject *self, PyObject *args);
    
    // convert a db0::serial::Serializable to bytes
    PyObject *pySerialize(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
    // convert bytes to instance (e.g. ObjectIterator)
    PyObject *pyDeserialize(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
    PyObject *makeEnum(PyObject *, PyObject *args, PyObject *kwargs);
    
    PyObject *splitBy(PyObject *, PyObject *args, PyObject *kwargs);
    
    // implements db0.filter functionality
    PyObject *filter(PyObject *, PyObject *args, PyObject *kwargs);
    
    PyObject *isEnumValue(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
    PyObject *getTypeInfo(PyObject *self, PyObject *args);
    
    PyObject *setPrefix(PyObject *self, PyObject *args, PyObject *kwargs);
    
    PyObject *getSlabMetrics(PyObject *self, PyObject *args);
    
    PyObject *setCacheSize(PyObject *self, PyObject *args);
    
    PyObject *getPrefixes(PyObject *, PyObject *);
    
    PyObject *getMemoClasses(PyObject *self, PyObject *args, PyObject *kwargs);
    
#ifndef NDEBUG
    PyObject *getResourceLockUsage(PyObject *self, PyObject *);
    PyObject *testCreateThenFree(PyObject *self, PyObject *args);
#endif

    template <typename T> db0::object_model::StorageClass getStorageClass();

    template <> db0::object_model::StorageClass getStorageClass<MemoObject>();
    template <> db0::object_model::StorageClass getStorageClass<ListObject>();

}