// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include <dbzero/bindings/python/embedded/EmbeddedTuple.hpp>

#include <dbzero/bindings/python/MemoObject.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/bindings/python/Utils.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/tuple/o_py_tuple.hpp>
#include <dbzero/workspace/Fixture.hpp>

#include <sstream>

namespace db0::python
{
    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
    using namespace db0::object_model;

    EmbeddedTupleRef::EmbeddedTupleRef(PyObject *rootObject, const o_py_tuple *tuple)
        : m_root_object(rootObject)
        , m_tuple(tuple)
    {
        Py_XINCREF(m_root_object);
    }

    EmbeddedTupleRef::~EmbeddedTupleRef()
    {
        Py_XDECREF(m_root_object);
    }

    PyObject *EmbeddedTupleRef::rootObject() const
    {
        return m_root_object;
    }

    const o_py_tuple &EmbeddedTupleRef::tuple() const
    {
        return *m_tuple;
    }

    namespace
    {
        db0::swine_ptr<Fixture> getRootFixture(PyObject *rootObject)
        {
            return reinterpret_cast<MemoImmutableObject *>(rootObject)->ext().getFixture();
        }

        PyObject *tryEmbeddedTupleGetItem(EmbeddedTuple *self, Py_ssize_t index)
        {
            auto fixture = getRootFixture(self->ext().rootObject());
            fixture->refreshIfUpdated();

            auto size = static_cast<Py_ssize_t>(self->ext().tuple().size());
            if (index < 0) {
                index += size;
            }
            if (index < 0 || index >= size) {
                PyErr_SetString(PyExc_IndexError, "tuple index out of range");
                return nullptr;
            }
            return PyToolkit::unloadEmbeddedInstance(
                fixture, self->ext().rootObject(), self->ext().tuple().item(static_cast<std::size_t>(index))
            ).steal();
        }

        PyObject *PyAPI_EmbeddedTuple_GetItem(EmbeddedTuple *self, Py_ssize_t index)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedTupleGetItem, self, index);
        }

        Py_ssize_t tryEmbeddedTupleLen(EmbeddedTuple *self)
        {
            return static_cast<Py_ssize_t>(self->ext().tuple().size());
        }

        Py_ssize_t PyAPI_EmbeddedTuple_len(EmbeddedTuple *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedTupleLen, self);
        }

        PyObject *tryEmbeddedTupleStr(EmbeddedTuple *self)
        {
            std::stringstream str;
            str << "(";
            auto size = self->ext().tuple().size();
            for (std::size_t i = 0; i < size; ++i) {
                if (i != 0) {
                    str << ", ";
                }
                auto item = Py_OWN(tryEmbeddedTupleGetItem(self, static_cast<Py_ssize_t>(i)));
                if (!item) {
                    return nullptr;
                }
                auto repr = Py_OWN(PyObject_Repr(*item));
                if (!repr) {
                    return nullptr;
                }
                str << PyUnicode_AsUTF8(*repr);
            }
            if (size == 1) {
                str << ",";
            }
            str << ")";
            return PyUnicode_FromString(str.str().c_str());
        }

        PyObject *PyAPI_EmbeddedTuple_str(EmbeddedTuple *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedTupleStr, self);
        }

        PyObject *tryEmbeddedTupleCount(EmbeddedTuple *self, PyObject *const *args, Py_ssize_t)
        {
            Py_ssize_t count = 0;
            for (std::size_t i = 0; i < self->ext().tuple().size(); ++i) {
                auto item = Py_OWN(tryEmbeddedTupleGetItem(self, static_cast<Py_ssize_t>(i)));
                if (!item) {
                    return nullptr;
                }
                int equal = PyObject_RichCompareBool(*item, args[0], Py_EQ);
                if (equal < 0) {
                    return nullptr;
                }
                count += equal;
            }
            return PyLong_FromSsize_t(count);
        }

        PyObject *PyAPI_EmbeddedTuple_count(EmbeddedTuple *self, PyObject *const *args, Py_ssize_t nargs)
        {
            PY_API_FUNC
            if (nargs != 1) {
                PyErr_SetString(PyExc_TypeError, "count() takes one argument.");
                return nullptr;
            }
            return runSafe(tryEmbeddedTupleCount, self, args, nargs);
        }

        PyObject *tryEmbeddedTupleIndex(EmbeddedTuple *self, PyObject *const *args, Py_ssize_t)
        {
            for (std::size_t i = 0; i < self->ext().tuple().size(); ++i) {
                auto item = Py_OWN(tryEmbeddedTupleGetItem(self, static_cast<Py_ssize_t>(i)));
                if (!item) {
                    return nullptr;
                }
                int equal = PyObject_RichCompareBool(*item, args[0], Py_EQ);
                if (equal < 0) {
                    return nullptr;
                }
                if (equal) {
                    return PyLong_FromSize_t(i);
                }
            }
            PyErr_SetString(PyExc_ValueError, "tuple.index(x): x not in tuple");
            return nullptr;
        }

        PyObject *PyAPI_EmbeddedTuple_index(EmbeddedTuple *self, PyObject *const *args, Py_ssize_t nargs)
        {
            PY_API_FUNC
            if (nargs != 1) {
                PyErr_SetString(PyExc_TypeError, "index() takes one argument.");
                return nullptr;
            }
            return runSafe(tryEmbeddedTupleIndex, self, args, nargs);
        }

        void PyAPI_EmbeddedTuple_del(EmbeddedTuple *self)
        {
            PY_DEALLOC_GUARD();
            PY_API_FUNC
            if (PyObject_GC_IsTracked(self)) {
                PyObject_GC_UnTrack(self);
            }
            self->destroy();
            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject *>(self));
        }

        int EmbeddedTuple_traverse(EmbeddedTuple *self, visitproc visit, void *arg)
        {
            Py_VISIT(self->ext().rootObject());
            return 0;
        }

        static PySequenceMethods EmbeddedTuple_sq = {
            .sq_length = reinterpret_cast<lenfunc>(PyAPI_EmbeddedTuple_len),
            .sq_item = reinterpret_cast<ssizeargfunc>(PyAPI_EmbeddedTuple_GetItem),
        };

        static PyMethodDef EmbeddedTuple_methods[] = {
            {"count", reinterpret_cast<PyCFunction>(PyAPI_EmbeddedTuple_count), METH_FASTCALL, nullptr},
            {"index", reinterpret_cast<PyCFunction>(PyAPI_EmbeddedTuple_index), METH_FASTCALL, nullptr},
            {NULL}
        };
    }

    PyTypeObject EmbeddedTupleType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.EmbeddedTuple",
        .tp_basicsize = static_cast<Py_ssize_t>(EmbeddedTuple::sizeOf()),
        .tp_itemsize = 0,
        .tp_dealloc = reinterpret_cast<destructor>(PyAPI_EmbeddedTuple_del),
        .tp_repr = reinterpret_cast<reprfunc>(PyAPI_EmbeddedTuple_str),
        .tp_as_sequence = &EmbeddedTuple_sq,
        .tp_str = reinterpret_cast<reprfunc>(PyAPI_EmbeddedTuple_str),
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
        .tp_doc = "dbzero embedded immutable tuple view",
        .tp_traverse = reinterpret_cast<traverseproc>(EmbeddedTuple_traverse),
        .tp_methods = EmbeddedTuple_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_free = PyObject_GC_Del,
    };

    ObjectSharedPtr makeEmbeddedTuple(PyObject *rootObject, const o_py_tuple &tuple)
    {
        auto *pyObject = reinterpret_cast<EmbeddedTuple *>(EmbeddedTupleType.tp_alloc(&EmbeddedTupleType, 0));
        if (!pyObject) {
            return {};
        }
        pyObject->makeNew(rootObject, &tuple);
        return Py_OWN(reinterpret_cast<PyObject *>(pyObject));
    }
}
