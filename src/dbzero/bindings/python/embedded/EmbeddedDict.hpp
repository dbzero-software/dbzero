// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>

#include <dbzero/bindings/python/PyTypes.hpp>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/object_model/dict/o_dict.hpp>

namespace db0::object_model
{
    class o_py_dict;
}

namespace db0::python
{
    class EmbeddedDictRef
    {
    public:
        EmbeddedDictRef(PyObject *rootObject, const db0::object_model::o_py_dict *dict);
        ~EmbeddedDictRef();

        EmbeddedDictRef(const EmbeddedDictRef &) = delete;
        EmbeddedDictRef &operator=(const EmbeddedDictRef &) = delete;

        PyObject *rootObject() const;
        const db0::object_model::o_py_dict &dict() const;

    private:
        PyObject *m_root_object = nullptr;
        const db0::object_model::o_py_dict *m_dict = nullptr;
    };

    class EmbeddedDictIteratorRef
    {
    public:
        enum class ItemKind
        {
            KEY,
            VALUE,
            PAIR,
        };

        EmbeddedDictIteratorRef(PyObject *dictObject, ItemKind itemKind);
        ~EmbeddedDictIteratorRef();

        EmbeddedDictIteratorRef(const EmbeddedDictIteratorRef &) = delete;
        EmbeddedDictIteratorRef &operator=(const EmbeddedDictIteratorRef &) = delete;

        PyObject *dictObject() const;
        ItemKind itemKind() const;
        const db0::object_model::o_dict::Pair *next();

    private:
        PyObject *m_dict_object = nullptr;
        ItemKind m_item_kind = ItemKind::KEY;
        db0::object_model::o_dict::const_iterator m_iterator;
        db0::object_model::o_dict::const_iterator m_end;
    };

    using EmbeddedDict = PyWrapper<EmbeddedDictRef, false>;
    using EmbeddedDictIterator = PyWrapper<EmbeddedDictIteratorRef, false>;

    extern PyTypeObject EmbeddedDictType;
    extern PyTypeObject EmbeddedDictIteratorType;

    PyTypes::ObjectSharedPtr makeEmbeddedDict(
        PyObject *rootObject, const db0::object_model::o_py_dict &dict
    );
}
