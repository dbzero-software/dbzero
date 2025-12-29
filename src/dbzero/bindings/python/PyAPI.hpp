// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

// This module contains python specific dbzero API implementation
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
    PyObject *PyAPI_clearCache(PyObject *, PyObject *);
    
    /**    
     * Fetch dbzero object instance by its ID or type (in case of a singleton)
     */
    PyObject *PyAPI_fetch(PyObject *, PyObject *args, PyObject *kwargs);
    // Similar to PyAPI_fetch, but only returns a flag True / False if object can be fetched
    PyObject *PyAPI_exists(PyObject *, PyObject *args, PyObject *kwargs);
    
    /**
     * Initialize dbzero Python bindings
    */    
    PyObject *PyAPI_init(PyObject *self, PyObject *args, PyObject *kwargs);
    
    /**
     * Opens or creates a prefix for read or read/write
    */
    PyObject *PyAPI_open(PyObject *self, PyObject *args, PyObject *kwargs);
    
    PyObject *PyAPI_drop(PyObject *self, PyObject *args);
    
    PyObject *PyAPI_commit(PyObject *self, PyObject *args);

    PyObject *PyAPI_close(PyObject *self, PyObject *args);
    
    /**
     * Constructs a dbzero list instance
    */
    PyObject *list(PyObject *self, PyObject *args);
    
    /**
     * Get name/UUID of the prefix/file hosting specific instance
    */
    PyObject *PyAPI_getPrefixOf(PyObject *self, PyObject *args);
    
    /**
     * Get name of the current/default prefix
    */
    PyObject *PyAPI_getCurrentPrefix(PyObject *self, PyObject *args);

    /**
     * Delete specific dbzero object instance
    */
    PyObject *PyAPI_del(PyObject *self, PyObject *args);
    
    /**
     * Refresh all open fixtures to update to the latest changes
    */
    PyObject *refresh(PyObject *self, PyObject *args);
    
    /**
     * Get currently active state number associated with a specific prefix/file
    */
    PyObject *PyAPI_getStateNum(PyObject *self, PyObject *args, PyObject *kwargs);
    
    /**
     * Retrieve metrics of all active dbzero prefixes
    */
    PyObject *getDBMetrics(PyObject *self, PyObject *args);
    
    /**
     * Get dbzero state snapshot
    */
    PyObject *PyAPI_getSnapshot(PyObject *self, PyObject *args, PyObject *kwargs);
        
    /**
     * Describe field layout and output other properties of a specific dbzero object instance
    */
    PyObject *describeObject(PyObject *self, PyObject *args);
    
    PyObject *renameField(PyObject *self, PyObject *args, PyObject *kwargs);

    PyObject *PyAPI_isSingleton(PyObject *self, PyObject *args);

    PyObject *PyAPI_getRefCount(PyObject *self, PyObject *args);
    
    // convert to a Python dict
    PyObject *toDict(PyObject *, PyObject *const *args, Py_ssize_t nargs);

    PyObject *PyAPI_getBuildFlags(PyObject *self, PyObject *args);
    
    PyObject *PyAPI_makeEnum(PyObject *, PyObject *args, PyObject *kwargs);
    
    // implements db0.filter functionality
    PyObject *filter(PyObject *, PyObject *args, PyObject *kwargs);
    
    PyObject *PyAPI_isEnumValue(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
    PyObject *PyAPI_getTypeInfo(PyObject *self, PyObject *args);
    
    PyObject *PyAPI_setPrefix(PyObject *self, PyObject *args, PyObject *kwargs);
    
    PyObject *getSlabMetrics(PyObject *self, PyObject *args);
    
    PyObject *setCacheSize(PyObject *self, PyObject *args);
    
    PyObject *getPrefixes(PyObject *, PyObject *);

    PyObject *PyAPI_getMutablePrefixes(PyObject *, PyObject *);
    
    PyObject *PyAPI_getMemoClasses(PyObject *self, PyObject *args, PyObject *kwargs);
    
    PyObject *getPrefixStats(PyObject *self, PyObject *args, PyObject *kwargs);
    
    PyObject *getStorageStats(PyObject *, PyObject *args, PyObject *kwargs);

    PyObject *PyAPI_getAttributes(PyObject *self, PyObject *args);
    
    PyObject *PyAPI_getAttrAs(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    
    PyObject *PyAPI_getAddress(PyObject *self, PyObject *const *args, Py_ssize_t nargs);

    PyTypeObject *PyAPI_getType(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    
    PyObject *PyAPI_load(PyObject *self, PyObject *args, PyObject *kwargs);

    PyObject *PyAPI_loadAll(PyObject *self, PyObject *args, PyObject *kwargs);

    PyObject *PyAPI_hash(PyObject *self, PyObject *const *args, Py_ssize_t nargs);

    PyObject *PyAPI_materialized(PyObject *self, PyObject *const *args, Py_ssize_t nargs);

    PyObject *PyAPI_wait(PyObject *self, PyObject *args, PyObject *kwargs);
    
    PyObject *PyAPI_findSingleton(PyObject *self, PyObject *args, PyObject *kwargs);

    PyObject *PyAPI_weakProxy(PyObject *self, PyObject *const *args, Py_ssize_t nargs);

    PyObject *PyAPI_expired(PyObject *self, PyObject *const *args, Py_ssize_t nargs);

    PyObject *PyAPI_async_wait(PyObject *, PyObject *args, PyObject *kwargs);

    PyObject *PyAPI_getConfig(PyObject *, PyObject *);
    
    PyObject *PyAPI_compare(PyObject *, PyObject *args, PyObject *kwargs);

    PyObject *PyAPI_assign(PyObject *, PyObject *args, PyObject *kwargs);
    
    PyObject *PyAPI_touch(PyObject *, PyObject *const *args, Py_ssize_t nargs);

    PyObject *PyAPI_getMemoClass(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
    PyObject *PyAPI_copyPrefix(PyObject *, PyObject *args, PyObject *kwargs);

#ifndef NDEBUG
    PyObject *PyAPI_startDebugLogs(PyObject *self, PyObject *args);

    PyObject *getResourceLockUsage(PyObject *, PyObject *);

    // For a specific prefix, extract page num -> state num mapping related with its DRAM_Prefix
    PyObject *getDRAM_IOMap(PyObject *, PyObject *args, PyObject *kwargs);
        
    PyObject *PyAPI_breakpoint(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
    PyObject *PyAPI_enableStorageValidation(PyObject *, PyObject *args, PyObject *kwargs);
    PyObject *PyAPI_setTestParams(PyObject *, PyObject *args, PyObject *kwargs);
    PyObject *PyAPI_resetTestParams(PyObject *, PyObject *);
#endif
    
    template <typename T> db0::object_model::StorageClass getStorageClass();

    template <> db0::object_model::StorageClass getStorageClass<MemoObject>();
    template <> db0::object_model::StorageClass getStorageClass<ListObject>();

}