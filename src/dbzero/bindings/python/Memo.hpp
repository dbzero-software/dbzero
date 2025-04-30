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
    
    // check for same instances - i.e. if fixture and address are the same
    bool isSame(MemoObject *, MemoObject *);
    
    // check if memo type has been marked as singleton
    bool PyMemoType_IsSingleton(PyTypeObject *type);
    
    PyObject *MemoObject_GetFieldLayout(MemoObject *);
    
    PyObject *MemoObject_DescribeObject(MemoObject *);
    PyObject *MemoObject_str(MemoObject *);
    
    void MemoType_get_info(PyTypeObject *type, PyObject *dict);
    void MemoType_close(PyTypeObject *type);
    
    PyObject *MemoObject_set_prefix(MemoObject *, const char *prefix_name);
    
    PyObject *tryGetAttributes(PyTypeObject *type);
    // Try retrieving a memo member cast to a specific type
    // type ignored for non-memo members
    PyObject *tryGetAttrAs(MemoObject *, PyObject *attr, PyTypeObject *);
    
    PyObject *tryLoadMemo(MemoObject *memo_obj, PyObject* kwargs,  PyObject* exclude);

    PyObject *PyAPI_PyMemo_Check(PyObject *self, PyObject *const * args, Py_ssize_t nargs);
    
}