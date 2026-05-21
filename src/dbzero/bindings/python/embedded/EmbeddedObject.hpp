// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>

#include <memory>

#include <dbzero/bindings/python/MemoObject.hpp>
#include <dbzero/bindings/python/PyTypes.hpp>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/core/memory/swine_ptr.hpp>

namespace db0
{
    class Fixture;
}

namespace db0::object_model
{
    class Class;
    class o_embedded_object;
    class o_py_tuple;
    class o_py_set;
}

namespace db0::python
{
    class EmbeddedObjectRef
    {
    public:
        EmbeddedObjectRef(
            PyObject *rootObject, const db0::object_model::o_embedded_object *embeddedObject,
            std::shared_ptr<db0::object_model::Class> type
        );
        ~EmbeddedObjectRef();

        EmbeddedObjectRef(const EmbeddedObjectRef &) = delete;
        EmbeddedObjectRef &operator=(const EmbeddedObjectRef &) = delete;

        PyObject *rootObject() const;
        const db0::object_model::o_embedded_object &embeddedObject() const;
        db0::object_model::Class &type() const;

    private:
        PyObject *m_root_object = nullptr;
        const db0::object_model::o_embedded_object *m_embedded_object = nullptr;
        std::shared_ptr<db0::object_model::Class> m_type;
    };

    using EmbeddedObject = PyWrapper<EmbeddedObjectRef, false>;

    extern PyTypeObject EmbeddedObjectType;

    PyTypes::ObjectSharedPtr makeEmbeddedObject(
        PyObject *rootObject, const db0::object_model::o_embedded_object &embeddedObject,
        std::shared_ptr<db0::object_model::Class> type
    );

    PyTypes::ObjectSharedPtr makeEmbeddedMemoObject(
        PyObject *rootObject, const db0::object_model::o_embedded_object &embeddedObject,
        std::shared_ptr<db0::object_model::Class> type, PyTypeObject *memoType
    );

    void transformMemoImmutableObjectToEmbedded(
        MemoImmutableObject *object, PyObject *rootObject, const db0::object_model::o_embedded_object &embeddedObject,
        std::shared_ptr<db0::object_model::Class> type
    );

    void transformEmbeddedObject(
        db0::swine_ptr<Fixture> &fixture, PyTypes::ObjectPtr rootObject, PyTypes::ObjectPtr sourceObject,
        const db0::object_model::o_embedded_object &embeddedObject
    );

    void transformEmbeddedTuple(
        db0::swine_ptr<Fixture> &fixture, PyTypes::ObjectPtr rootObject, PyTypes::ObjectPtr sourceSequence,
        const db0::object_model::o_py_tuple &embeddedTuple
    );

    void transformEmbeddedSet(
        db0::swine_ptr<Fixture> &fixture, PyTypes::ObjectPtr rootObject, PyTypes::ObjectPtr sourceSet,
        const db0::object_model::o_py_set &embeddedSet
    );

    bool PyEmbeddedMemo_Check(PyObject *object);
    bool PyEmbeddedMemoType_Check(PyTypeObject *type);
}
