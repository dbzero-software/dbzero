// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>
#include <dbzero/object_model/dict/DictView.hpp>

namespace db0::python 
{
    

    using DictViewObject = PyWrapper<db0::object_model::DictView, false>;
    
    DictViewObject *DictViewObject_new(PyTypeObject *type, PyObject *, PyObject *);
    DictViewObject *DictViewDefaultObject_new();
    void PyAPI_DictViewObject_del(DictViewObject* self);
    Py_ssize_t PyAPI_DictViewObject_len(DictViewObject *);
    extern PyTypeObject DictViewObjectType;
    
    bool DictViewObject_Check(PyObject *);

    void DictViewObject_del(DictViewObject* dict_obj);
    
    DictViewObject *makeDictView(PyObject *py_dict, const db0::object_model::Dict *, 
        db0::object_model::IteratorType);

}