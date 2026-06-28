// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyRestrictedMethod.hpp"
#include "PyToolkit.hpp"
#include <object.h>

#ifndef PYVAROBJECT_HEAD_INIT_DESIGNATED
#define PYVAROBJECT_HEAD_INIT_DESIGNATED \
    .ob_base = { \
        .ob_base = { \
            .ob_refcnt = 1, \
            .ob_type = NULL, \
        }, \
        .ob_size = 0, \
    }
#endif

namespace db0::python

{

    struct PyRestrictedMethod
    {
        PyObject_HEAD
        PyObject *m_method = nullptr;
    };

    PyObject *PyRestrictedMethod_call(PyRestrictedMethod *self, PyObject *args, PyObject *kwargs)
    {
        return PyObject_Call(self->m_method, args, kwargs);
    }

    PyObject *PyRestrictedMethod_getattro(PyRestrictedMethod *, PyObject *attr)
    {
        const char *attr_name = PyUnicode_AsUTF8(attr);
        if (!attr_name) {
            PyErr_SetString(PyExc_AttributeError, "Invalid attribute name");
            return nullptr;
        }
        PyErr_Format(PyExc_AttributeError, "Restricted method attribute access denied: %s", attr_name);
        return nullptr;
    }

    PyObject *PyRestrictedMethod_dir(PyRestrictedMethod *, PyObject *)
    {
        PyErr_SetString(PyExc_AttributeError, "Restricted method directory access denied");
        return nullptr;
    }

    void PyRestrictedMethod_dealloc(PyRestrictedMethod *self)
    {
        Py_XDECREF(self->m_method);
        Py_TYPE(self)->tp_free(reinterpret_cast<PyObject *>(self));
    }

    static PyMethodDef PyRestrictedMethod_methods[] = {
        {"__dir__", reinterpret_cast<PyCFunction>(PyRestrictedMethod_dir), METH_NOARGS, nullptr},
        {nullptr}
    };

    PyTypeObject PyRestrictedMethodType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "RestrictedMethod",
        .tp_basicsize = sizeof(PyRestrictedMethod),
        .tp_itemsize = 0,
        .tp_dealloc = reinterpret_cast<destructor>(PyRestrictedMethod_dealloc),
        .tp_call = reinterpret_cast<ternaryfunc>(PyRestrictedMethod_call),
        .tp_getattro = reinterpret_cast<getattrofunc>(PyRestrictedMethod_getattro),
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "restricted dbzero memo method proxy",
        .tp_methods = PyRestrictedMethod_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = PyType_GenericNew,
        .tp_free = PyObject_Free,
    };

    PyObject *makeRestrictedMethod(PyObject *method)
    {
        auto result = reinterpret_cast<PyRestrictedMethod *>(PyRestrictedMethodType.tp_alloc(&PyRestrictedMethodType, 0));
        if (!result) {
            return nullptr;
        }
        Py_INCREF(method);
        result->m_method = method;
        return reinterpret_cast<PyObject *>(result);
    }

    bool isRestrictedName(const char *attr_name)
    {
        return attr_name[0] == '_';
    }

    PyObject *tryGetRestrictedMethod(PyObject *memo_obj, PyObject *attr)
    {
        auto *type = Py_TYPE(memo_obj);
        auto *mro = type->tp_mro;
        if (!mro || !PyTuple_Check(mro)) {
            Py_RETURN_NONE;
        }

        for (Py_ssize_t i = 0; i < PyTuple_GET_SIZE(mro); ++i) {
            auto *mro_item = PyTuple_GET_ITEM(mro, i);
            if (!PyType_Check(mro_item)) {
                continue;
            }
            auto *mro_type = reinterpret_cast<PyTypeObject *>(mro_item);
            if (!mro_type->tp_dict) {
                continue;
            }
            auto *raw_attr = PyDict_GetItemWithError(mro_type->tp_dict, attr);
            if (!raw_attr) {
                if (PyErr_Occurred()) {
                    return nullptr;
                }
                continue;
            }
            if (PyFunction_Check(raw_attr)) {
                auto method = Py_OWN(PyMethod_New(raw_attr, memo_obj));
                if (!method) {
                    return nullptr;
                }
                return makeRestrictedMethod(*method);
            }
            break;
        }

        Py_RETURN_NONE;
    }

    PyObject *tryRestrictedMemoGetattro(
        PyObject *memo_obj,
        PyObject *attr,
        const char *attr_name,
        PyTypes::ObjectSharedPtr &member
    )
    {
        if (isRestrictedName(attr_name)) {
            PyErr_Format(PyExc_AttributeError, "Restricted memo attribute access denied: %s", attr_name);
            return nullptr;
        }

        if (member.get()) {
            return member.steal();
        }

        auto restricted_method = Py_OWN(tryGetRestrictedMethod(memo_obj, attr));
        if (!restricted_method) {
            return nullptr;
        }
        if (*restricted_method != Py_None) {
            return restricted_method.steal();
        }

        PyErr_Format(PyExc_AttributeError, "Restricted memo attribute access denied: %s", attr_name);
        return nullptr;
    }

}
