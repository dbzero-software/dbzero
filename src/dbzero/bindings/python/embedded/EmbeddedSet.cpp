// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include <dbzero/bindings/python/embedded/EmbeddedSet.hpp>

#include <dbzero/bindings/python/MemoObject.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/bindings/python/Utils.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/set/o_py_set.hpp>
#include <dbzero/workspace/Fixture.hpp>

#include <sstream>

namespace db0::python
{
    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
    using namespace db0::object_model;

    EmbeddedSetRef::EmbeddedSetRef(PyObject *rootObject, const o_py_set *set)
        : m_root_object(rootObject)
        , m_set(set)
    {
        Py_XINCREF(m_root_object);
    }

    EmbeddedSetRef::~EmbeddedSetRef()
    {
        Py_XDECREF(m_root_object);
    }

    PyObject *EmbeddedSetRef::rootObject() const
    {
        return m_root_object;
    }

    const o_py_set &EmbeddedSetRef::set() const
    {
        return *m_set;
    }

    EmbeddedSetIteratorRef::EmbeddedSetIteratorRef(PyObject *setObject)
        : m_set_object(setObject)
        , m_iterator(reinterpret_cast<EmbeddedSet *>(setObject)->ext().set().begin())
        , m_end(reinterpret_cast<EmbeddedSet *>(setObject)->ext().set().end())
    {
        Py_XINCREF(m_set_object);
    }

    EmbeddedSetIteratorRef::~EmbeddedSetIteratorRef()
    {
        Py_XDECREF(m_set_object);
    }

    PyObject *EmbeddedSetIteratorRef::setObject() const
    {
        return m_set_object;
    }

    const o_set::Item *EmbeddedSetIteratorRef::next()
    {
        if (m_iterator == m_end) {
            return nullptr;
        }
        const auto *item = &*m_iterator;
        ++m_iterator;
        return item;
    }

    namespace
    {
        db0::swine_ptr<Fixture> getRootFixture(PyObject *rootObject)
        {
            return reinterpret_cast<MemoImmutableObject *>(rootObject)->ext().getFixture();
        }

        PyObject *unloadEmbeddedSetItem(PyObject *rootObject, const o_set::Item &item)
        {
            auto fixture = getRootFixture(rootObject);
            fixture->refreshIfUpdated();
            return PyToolkit::unloadEmbeddedInstance(fixture, rootObject, item).steal();
        }

        PyObject *tryEmbeddedSetIter(EmbeddedSet *self)
        {
            auto *pyObject = reinterpret_cast<EmbeddedSetIterator *>(
                EmbeddedSetIteratorType.tp_alloc(&EmbeddedSetIteratorType, 0)
            );
            if (!pyObject) {
                return nullptr;
            }
            pyObject->makeNew(reinterpret_cast<PyObject *>(self));
            return reinterpret_cast<PyObject *>(pyObject);
        }

        PyObject *PyAPI_EmbeddedSet_iter(EmbeddedSet *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedSetIter, self);
        }

        PyObject *tryEmbeddedSetIteratorNext(EmbeddedSetIterator *self)
        {
            auto *setObject = reinterpret_cast<EmbeddedSet *>(self->ext().setObject());
            auto *item = self->modifyExt().next();
            if (!item) {
                return nullptr;
            }
            return unloadEmbeddedSetItem(setObject->ext().rootObject(), *item);
        }

        PyObject *PyAPI_EmbeddedSetIterator_next(EmbeddedSetIterator *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedSetIteratorNext, self);
        }

        Py_ssize_t tryEmbeddedSetLen(EmbeddedSet *self)
        {
            return static_cast<Py_ssize_t>(self->ext().set().size());
        }

        Py_ssize_t PyAPI_EmbeddedSet_len(EmbeddedSet *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedSetLen, self);
        }

        int tryEmbeddedSetContains(EmbeddedSet *self, PyObject *needle)
        {
            for (const auto &item: self->ext().set()) {
                auto pyItem = Py_OWN(unloadEmbeddedSetItem(self->ext().rootObject(), item));
                if (!pyItem) {
                    return -1;
                }
                int equal = PyObject_RichCompareBool(*pyItem, needle, Py_EQ);
                if (equal < 0) {
                    return -1;
                }
                if (equal) {
                    return 1;
                }
            }
            return 0;
        }

        int PyAPI_EmbeddedSet_contains(EmbeddedSet *self, PyObject *needle)
        {
            PY_API_FUNC
            return runSafe<-1>(tryEmbeddedSetContains, self, needle);
        }

        PyObject *tryEmbeddedSetStr(EmbeddedSet *self)
        {
            if (self->ext().set().empty()) {
                return PyUnicode_FromString("set()");
            }

            std::stringstream str;
            str << "{";
            bool first = true;
            for (const auto &setItem: self->ext().set()) {
                if (!first) {
                    str << ", ";
                }
                first = false;
                auto item = Py_OWN(unloadEmbeddedSetItem(self->ext().rootObject(), setItem));
                if (!item) {
                    return nullptr;
                }
                auto repr = Py_OWN(PyObject_Repr(*item));
                if (!repr) {
                    return nullptr;
                }
                str << PyUnicode_AsUTF8(*repr);
            }
            str << "}";
            return PyUnicode_FromString(str.str().c_str());
        }

        PyObject *PyAPI_EmbeddedSet_str(EmbeddedSet *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedSetStr, self);
        }

        void PyAPI_EmbeddedSet_del(EmbeddedSet *self)
        {
            PY_DEALLOC_GUARD();
            PY_API_FUNC
            if (PyObject_GC_IsTracked(self)) {
                PyObject_GC_UnTrack(self);
            }
            self->destroy();
            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject *>(self));
        }

        void PyAPI_EmbeddedSetIterator_del(EmbeddedSetIterator *self)
        {
            PY_DEALLOC_GUARD();
            PY_API_FUNC
            if (PyObject_GC_IsTracked(self)) {
                PyObject_GC_UnTrack(self);
            }
            self->destroy();
            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject *>(self));
        }

        int EmbeddedSet_traverse(EmbeddedSet *self, visitproc visit, void *arg)
        {
            Py_VISIT(self->ext().rootObject());
            return 0;
        }

        int EmbeddedSetIterator_traverse(EmbeddedSetIterator *self, visitproc visit, void *arg)
        {
            Py_VISIT(self->ext().setObject());
            return 0;
        }

        static PySequenceMethods EmbeddedSet_sq = {
            .sq_length = reinterpret_cast<lenfunc>(PyAPI_EmbeddedSet_len),
            .sq_contains = reinterpret_cast<objobjproc>(PyAPI_EmbeddedSet_contains),
        };
    }

    PyTypeObject EmbeddedSetType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.EmbeddedSet",
        .tp_basicsize = static_cast<Py_ssize_t>(EmbeddedSet::sizeOf()),
        .tp_itemsize = 0,
        .tp_dealloc = reinterpret_cast<destructor>(PyAPI_EmbeddedSet_del),
        .tp_repr = reinterpret_cast<reprfunc>(PyAPI_EmbeddedSet_str),
        .tp_as_sequence = &EmbeddedSet_sq,
        .tp_str = reinterpret_cast<reprfunc>(PyAPI_EmbeddedSet_str),
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
        .tp_doc = "dbzero embedded immutable set view",
        .tp_traverse = reinterpret_cast<traverseproc>(EmbeddedSet_traverse),
        .tp_iter = reinterpret_cast<getiterfunc>(PyAPI_EmbeddedSet_iter),
        .tp_alloc = PyType_GenericAlloc,
        .tp_free = PyObject_GC_Del,
    };

    PyTypeObject EmbeddedSetIteratorType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.EmbeddedSetIterator",
        .tp_basicsize = static_cast<Py_ssize_t>(EmbeddedSetIterator::sizeOf()),
        .tp_itemsize = 0,
        .tp_dealloc = reinterpret_cast<destructor>(PyAPI_EmbeddedSetIterator_del),
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
        .tp_doc = "dbzero embedded immutable set iterator",
        .tp_traverse = reinterpret_cast<traverseproc>(EmbeddedSetIterator_traverse),
        .tp_iter = PyObject_SelfIter,
        .tp_iternext = reinterpret_cast<iternextfunc>(PyAPI_EmbeddedSetIterator_next),
        .tp_alloc = PyType_GenericAlloc,
        .tp_free = PyObject_GC_Del,
    };

    ObjectSharedPtr makeEmbeddedSet(PyObject *rootObject, const o_py_set &set)
    {
        auto *pyObject = reinterpret_cast<EmbeddedSet *>(EmbeddedSetType.tp_alloc(&EmbeddedSetType, 0));
        if (!pyObject) {
            return {};
        }
        pyObject->makeNew(rootObject, &set);
        return Py_OWN(reinterpret_cast<PyObject *>(pyObject));
    }
}
