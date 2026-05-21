// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>

#include <memory>

#include "MemoObject.hpp"
#include "PyTypes.hpp"
#include "PyWrapper.hpp"

namespace db0::object_model
{
    class Class;
    class o_embedded_object;
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

    bool PyEmbeddedMemo_Check(PyObject *object);
    bool PyEmbeddedMemoType_Check(PyTypeObject *type);
}
