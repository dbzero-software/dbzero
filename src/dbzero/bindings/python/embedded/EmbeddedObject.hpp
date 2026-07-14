// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <Python.h>

#include <memory>

#include <dbzero/bindings/python/MemoObject.hpp>
#include <dbzero/bindings/python/PyTypes.hpp>
#include <dbzero/bindings/python/PyWrapper.hpp>
#include <dbzero/core/memory/Address.hpp>
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
    class o_py_dict;
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
        db0::swine_ptr<db0::Fixture> fixture() const;
        db0::Address address() const;
        db0::UniqueAddress uniqueAddress() const;
        std::uint64_t offset() const;

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

    void transformEmbeddedDict(
        db0::swine_ptr<Fixture> &fixture, PyTypes::ObjectPtr rootObject, PyTypes::ObjectPtr sourceDict,
        const db0::object_model::o_py_dict &embeddedDict
    );

    bool PyEmbeddedMemo_Check(PyObject *object);
    bool PyEmbeddedMemoType_Check(PyTypeObject *type);
    EmbeddedObjectRef &getEmbeddedMemoRef(MemoImmutableObject *object);
    const EmbeddedObjectRef &getEmbeddedMemoRef(const MemoImmutableObject *object);
    db0::swine_ptr<db0::Fixture> getEmbeddedMemoFixture(PyObject *object);
    db0::Address getEmbeddedMemoAddress(PyObject *object);
    db0::UniqueAddress getEmbeddedMemoUniqueAddress(PyObject *object);
    void incEmbeddedMemoRef(PyObject *object, bool isTag);
}
