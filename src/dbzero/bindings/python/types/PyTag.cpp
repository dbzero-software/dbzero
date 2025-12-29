// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyTag.hpp"
#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/bindings/python/MemoExpiredRef.hpp>
#include <dbzero/object_model/object/Object.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/Utils.hpp>

namespace db0::python

{

    PyTag *PyTag_new(PyTypeObject *type, PyObject *, PyObject *) {
        return reinterpret_cast<PyTag*>(type->tp_alloc(type, 0));
    }
    
    PyTag *PyTagDefault_new() {
        return PyTag_new(&PyTagType, NULL, NULL);
    }

    void PyTag_del(PyTag* self)
    {
        // destroy associated instance
        self->destroy();
        Py_TYPE(self)->tp_free((PyObject*)self);
    }

    static PyObject *TryPyAPI_PyTag_richcompare(PyTag *self, PyObject *other, int op)
    {
        bool result = false;
        if (PyTag_Check(other)) {
            PyTag * other_tag = reinterpret_cast<PyTag*>(other);
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

    static PyObject *PyAPI_PyTag_richcompare(PyTag *self, PyObject *other, int op){
        PY_API_FUNC
        return runSafe(TryPyAPI_PyTag_richcompare, self, other, op);
    }
    
    static Py_hash_t PyAPI_PyTag_hash(PyTag *self) {
        return self->ext().getHash();
    }
    
    PyTypeObject PyTagType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.Tag",
        .tp_basicsize = PyTag::sizeOf(),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)PyTag_del,
        .tp_hash = (hashfunc)PyAPI_PyTag_hash,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_richcompare = (richcmpfunc)PyAPI_PyTag_richcompare,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = (newfunc)PyTag_new,
        .tp_free = PyObject_Free,
    };

    bool PyTag_Check(PyObject *py_object) {
        return Py_TYPE(py_object) == &PyTagType;
    }

    template <typename MemoImplT>
    PyObject *tryMemoAsTag(PyObject *py_obj)
    {
        assert(PyMemo_Check<MemoImplT>(py_obj));
        auto &memo_obj = reinterpret_cast<MemoImplT*>(py_obj)->ext();        
        PyTag *py_tag = PyTagDefault_new();
        py_tag->makeNew(memo_obj.getFixture()->getUUID(), memo_obj.getAddress(), py_obj);
        return py_tag;
    }
    
    PyObject *tryMemoTypeAsTag(PyTypeObject *py_type)
    {
        assert(PyAnyMemoType_Check(py_type));
        PyTag *py_tag = PyTagDefault_new();
        py_tag->makeNew(py_type, db0::object_model::TagDef::type_as_tag());
        return py_tag;
    }
    
    PyObject *tryMemoExpiredRefAsTag(PyObject *py_obj)
    {
        assert(MemoExpiredRef_Check(py_obj));
        const auto &expired_ref = *reinterpret_cast<const MemoExpiredRef*>(py_obj);
        PyTag *py_tag = PyTagDefault_new();
        py_tag->makeNew(expired_ref.getFixtureUUID(), expired_ref.getAddress(), py_obj);
        return py_tag;
    }
    
    PyObject *PyAPI_as_tag(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "as_tag: Expected 1 argument");
            return NULL;
        }
        if (PyMemo_Check<MemoObject>(args[0])) {
            return runSafe(tryMemoAsTag<MemoObject>, args[0]);
        } else if (PyMemo_Check<MemoImmutableObject>(args[0])) {
            return runSafe(tryMemoAsTag<MemoImmutableObject>, args[0]);
        } else if (PyType_Check(args[0])) {
            auto *py_type = reinterpret_cast<PyTypeObject*>(args[0]);
            if (PyAnyMemoType_Check(py_type)) {
                return runSafe(tryMemoTypeAsTag, py_type);
            }
        } else if (MemoExpiredRef_Check(args[0])) {
            return runSafe(tryMemoExpiredRefAsTag, args[0]);
        }
        PyErr_SetString(PyExc_TypeError, "as_tag: Expected a memo object");
        return NULL;        
    }
    
}
