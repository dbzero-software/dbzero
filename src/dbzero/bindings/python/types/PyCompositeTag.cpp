// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include "PyCompositeTag.hpp"
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/Utils.hpp>

namespace db0::python

{

    PyCompositeTag *PyCompositeTag_new(PyTypeObject *type, PyObject *, PyObject *)
    {
        return reinterpret_cast<PyCompositeTag*>(type->tp_alloc(type, 0));
    }

    PyCompositeTag *PyCompositeTagDefault_new()
    {
        return PyCompositeTag_new(&PyCompositeTagType, NULL, NULL);
    }

    void PyCompositeTag_del(PyCompositeTag *self)
    {
        PY_API_FUNC
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject *tryPyAPI_PyCompositeTag_richcompare(PyCompositeTag *self, PyObject *other, int op)
    {
        bool result = false;
        if (PyCompositeTag_Check(other)) {
            auto *other_tag = reinterpret_cast<PyCompositeTag*>(other);
            result = self->ext() == other_tag->ext();
        }

        switch (op)
        {
            case Py_EQ:
                return PyBool_fromBool(result);
            case Py_NE:
                return PyBool_fromBool(!result);
            default:
                Py_RETURN_NOTIMPLEMENTED;
        }
    }

    static PyObject *PyAPI_PyCompositeTag_richcompare(PyCompositeTag *self, PyObject *other, int op)
    {
        PY_API_FUNC
        return runSafe(tryPyAPI_PyCompositeTag_richcompare, self, other, op);
    }

    static Py_hash_t TryPyAPI_PyCompositeTag_hash(PyCompositeTag *self)
    {
        return self->ext().getHash();
    }

    static Py_hash_t PyAPI_PyCompositeTag_hash(PyCompositeTag *self)
    {
        PY_API_FUNC
        return runSafe(TryPyAPI_PyCompositeTag_hash, self);
    }

    PyTypeObject PyCompositeTagType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.CompositeTag",
        .tp_basicsize = PyCompositeTag::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyCompositeTag_del,
        .tp_hash = (hashfunc)PyAPI_PyCompositeTag_hash,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_richcompare = (richcmpfunc)PyAPI_PyCompositeTag_richcompare,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyCompositeTag_new,
        .tp_free = PyObject_Free,
    };

    bool PyCompositeTag_Check(PyObject *py_object)
    {
        return Py_TYPE(py_object) == &PyCompositeTagType;
    }

}
