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

    static PyObject *PyAPI_PyTag_richcompare(PyTag *self, PyObject *other, int op)
    {
        PY_API_FUNC
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

    static Py_hash_t PyAPI_PyTag_hash(PyTag *self)
    {
        static_assert(sizeof(unsigned long) == sizeof(self->ext().m_fixture_uuid));
        static_assert(sizeof(unsigned long) == sizeof(self->ext().m_value));
        PyTypes::ObjectSharedPtr tuple{Py_BuildValue("(kk)", self->ext().m_fixture_uuid, self->ext().m_value), false};
        if(!tuple) {
            return -1;
        }
        return PyObject_Hash(tuple.get());
    }

    PyTypeObject PyTagType = {
        PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = "dbzero_ce.Tag",
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

    PyObject *tryMemoAsTag(PyObject *py_obj)
    {
        assert(PyMemo_Check(py_obj));
        auto &memo_obj = reinterpret_cast<MemoObject*>(py_obj)->ext();        
        PyTag *py_tag = PyTagDefault_new();
        TagDef::makeNew(&py_tag->modifyExt(), memo_obj.getFixture()->getUUID(), memo_obj.getAddress(), py_obj);
        return py_tag;
    }
    
    PyObject *tryMemoExpiredRefAsTag(PyObject *py_obj)
    {
        assert(MemoExpiredRef_Check(py_obj));
        const auto &expired_ref = *reinterpret_cast<const MemoExpiredRef*>(py_obj);
        PyTag *py_tag = PyTagDefault_new();
        TagDef::makeNew(&py_tag->modifyExt(), expired_ref.getFixtureUUID(), expired_ref.getAddress(), py_obj);
        return py_tag;
    }

    PyObject *PyAPI_as_tag(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "as_tag: Expected 1 argument");
            return NULL;
        }        
        if (PyMemo_Check(args[0])) {
            return runSafe(tryMemoAsTag, args[0]);
        } else if (MemoExpiredRef_Check(args[0])) {
            return runSafe(tryMemoExpiredRefAsTag, args[0]);
        } else {
            PyErr_SetString(PyExc_TypeError, "as_tag: Expected a memo object");
            return NULL;
        }        
    }

}
