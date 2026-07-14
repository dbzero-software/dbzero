// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>

#include <dbzero/bindings/python/PyTypes.hpp>
#include <dbzero/bindings/python/PyWrapper.hpp>

namespace db0::object_model
{
    class o_py_tuple;
}

namespace db0::python
{
    class EmbeddedTupleRef
    {
    public:
        EmbeddedTupleRef(PyObject *rootObject, const db0::object_model::o_py_tuple *tuple);
        ~EmbeddedTupleRef();

        EmbeddedTupleRef(const EmbeddedTupleRef &) = delete;
        EmbeddedTupleRef &operator=(const EmbeddedTupleRef &) = delete;

        PyObject *rootObject() const;
        const db0::object_model::o_py_tuple &tuple() const;

    private:
        PyObject *m_root_object = nullptr;
        const db0::object_model::o_py_tuple *m_tuple = nullptr;
    };

    using EmbeddedTuple = PyWrapper<EmbeddedTupleRef, false>;

    extern PyTypeObject EmbeddedTupleType;

    PyTypes::ObjectSharedPtr makeEmbeddedTuple(
        PyObject *rootObject, const db0::object_model::o_py_tuple &tuple
    );
}
