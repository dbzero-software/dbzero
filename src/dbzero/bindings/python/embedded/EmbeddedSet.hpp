// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>

#include <dbzero/bindings/python/PyTypes.hpp>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/set/o_set.hpp>

namespace db0::object_model
{
    class o_py_set;
}

namespace db0::python
{
    class EmbeddedSetRef
    {
    public:
        EmbeddedSetRef(PyObject *rootObject, const db0::object_model::o_py_set *set);
        ~EmbeddedSetRef();

        EmbeddedSetRef(const EmbeddedSetRef &) = delete;
        EmbeddedSetRef &operator=(const EmbeddedSetRef &) = delete;

        PyObject *rootObject() const;
        const db0::object_model::o_py_set &set() const;

    private:
        PyObject *m_root_object = nullptr;
        const db0::object_model::o_py_set *m_set = nullptr;
    };

    class EmbeddedSetIteratorRef
    {
    public:
        explicit EmbeddedSetIteratorRef(PyObject *setObject);
        ~EmbeddedSetIteratorRef();

        EmbeddedSetIteratorRef(const EmbeddedSetIteratorRef &) = delete;
        EmbeddedSetIteratorRef &operator=(const EmbeddedSetIteratorRef &) = delete;

        PyObject *setObject() const;
        const db0::object_model::o_set::Item *next();

    private:
        PyObject *m_set_object = nullptr;
        db0::object_model::o_set::const_iterator m_iterator;
        db0::object_model::o_set::const_iterator m_end;
    };

    using EmbeddedSet = PyWrapper<EmbeddedSetRef, false>;
    using EmbeddedSetIterator = PyWrapper<EmbeddedSetIteratorRef, false>;

    extern PyTypeObject EmbeddedSetType;
    extern PyTypeObject EmbeddedSetIteratorType;

    PyTypes::ObjectSharedPtr makeEmbeddedSet(
        PyObject *rootObject, const db0::object_model::o_py_set &set
    );
}
