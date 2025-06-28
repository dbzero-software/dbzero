#pragma once

#include <cstddef>
#include <Python.h>
#include "PyWrapper.hpp"
#include <vector>
#include <unordered_map>
#include "shared_py_object.hpp"
#include <dbzero/core/memory/AccessOptions.hpp>
#include "Migration.hpp"
#include "MemoTypeDecoration.hpp"

namespace db0::object_model

{
    
    class Object;
    
}

namespace db0::python

{
    
    using AccessType = db0::AccessType;
    using MemoObject = PyWrapper<db0::object_model::Object>;
        
    PyObject *PyAPI_wrapPyClass(PyObject *self, PyObject *, PyObject *kwargs);
    MemoObject *PyAPI_MemoObject_new(PyTypeObject *type, PyObject * = nullptr, PyObject * = nullptr);
    // create a memo object stub
    shared_py_object<MemoObject*> MemoObjectStub_new(PyTypeObject *type);
    PyObject *MemoObject_alloc(PyTypeObject *type, Py_ssize_t nitems);
    
    void MemoObject_del(MemoObject* self);
    void MemoObject_drop(MemoObject* self);
    int PyAPI_MemoObject_init(MemoObject* self, PyObject* args, PyObject* kwds);
    PyObject *PyAPI_MemoObject_getattro(MemoObject *self, PyObject *attr);
    int PyAPI_MemoObject_setattro(MemoObject *self, PyObject *attr, PyObject *value);
    Py_hash_t PyAPI_MemoHash(PyObject *);
    
    // check if memo type has been marked as singleton
    bool PyMemoType_IsSingleton(PyTypeObject *type);
    
    PyObject *MemoObject_GetFieldLayout(MemoObject *);
    
    PyObject *MemoObject_DescribeObject(MemoObject *);
    PyObject *PyAPI_MemoObject_str(MemoObject *);
    
    void MemoType_get_info(PyTypeObject *type, PyObject *dict);
    void MemoType_close(PyTypeObject *type);
    
    PyObject *MemoObject_set_prefix(MemoObject *, const char *prefix_name);
    
    PyObject *tryGetAttributes(PyTypeObject *type);
    // Try retrieving a memo member cast to a specific type
    // type ignored for non-memo members
    PyObject *tryGetAttrAs(MemoObject *, PyObject *attr, PyTypeObject *);
    
    PyObject *tryLoadMemo(MemoObject *memo_obj, PyObject* kwargs,  PyObject* exclude,
        std::unordered_set<const void*> *load_stack_ptr = nullptr);
    
    PyObject *PyAPI_PyMemo_Check(PyObject *self, PyObject *const * args, Py_ssize_t nargs);
    
    // Binary (shallow) compare 2 objects or 2 versions of the same memo object (e.g. from different snapshots)
    // NOTE: ref-counts are not compared (only user-assigned members)
    // @return true if objects are identical
    PyObject *tryCompareMemo(MemoObject *, MemoObject *);

    PyObject *PyAPI_getSchema(PyObject *, PyObject *const *args, Py_ssize_t nargs);
    
}