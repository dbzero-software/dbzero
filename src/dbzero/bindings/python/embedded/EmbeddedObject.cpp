// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <dbzero/bindings/python/embedded/EmbeddedObject.hpp>

#include <dbzero/bindings/python/Memo.hpp>
#include <dbzero/bindings/python/MemoObject.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/bindings/python/Utils.hpp>

#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/utils/hash_combine.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/dict/o_py_dict.hpp>
#include <dbzero/object_model/object/ObjectInitializer.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
#include <dbzero/object_model/set/o_py_set.hpp>
#include <dbzero/object_model/tuple/o_py_tuple.hpp>
#include <dbzero/object_model/value/Member.hpp>
#include <dbzero/workspace/Fixture.hpp>

#include <sstream>
#include <cstdint>
#include <unordered_set>

namespace db0::python
{
    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
    using namespace db0::object_model;

    static_assert(EmbeddedObject::sizeOf() > sizeof(PyObject), "EmbeddedObject must allocate storage for its view state");
    static_assert(
        sizeof(EmbeddedObjectRef) <= sizeof(MemoImmutableObject::ExtT),
        "EmbeddedObjectRef must fit in MemoImmutableObject native storage for in-place morphing"
    );
    static_assert(
        alignof(EmbeddedObjectRef) <= alignof(MemoImmutableObject::ExtT),
        "EmbeddedObjectRef alignment must be compatible with MemoImmutableObject native storage"
    );

    EmbeddedObjectRef::EmbeddedObjectRef(
        PyObject *rootObject, const o_embedded_object *embeddedObject, std::shared_ptr<Class> type
    )
        : m_root_object(rootObject)
        , m_embedded_object(embeddedObject)
        , m_type(std::move(type))
    {
        Py_XINCREF(m_root_object);
    }

    EmbeddedObjectRef::~EmbeddedObjectRef()
    {
        Py_XDECREF(m_root_object);
    }

    PyObject *EmbeddedObjectRef::rootObject() const
    {
        return m_root_object;
    }

    const o_embedded_object &EmbeddedObjectRef::embeddedObject() const
    {
        return *m_embedded_object;
    }

    Class &EmbeddedObjectRef::type() const
    {
        return *m_type;
    }

    db0::swine_ptr<Fixture> EmbeddedObjectRef::fixture() const
    {
        return reinterpret_cast<MemoImmutableObject *>(m_root_object)->ext().getFixture();
    }

    std::uint64_t EmbeddedObjectRef::offset() const
    {
        const auto *root = reinterpret_cast<const std::byte *>(
            reinterpret_cast<MemoImmutableObject *>(m_root_object)->ext().operator->()
        );
        const auto *embedded = reinterpret_cast<const std::byte *>(m_embedded_object);
        assert(root <= embedded);
        return static_cast<std::uint64_t>(embedded - root);
    }

    db0::Address EmbeddedObjectRef::address() const
    {
        return reinterpret_cast<MemoImmutableObject *>(m_root_object)->ext().getAddress() + offset();
    }

    db0::UniqueAddress EmbeddedObjectRef::uniqueAddress() const
    {
        auto rootUniqueAddress = reinterpret_cast<MemoImmutableObject *>(m_root_object)->ext().getUniqueAddress();
        return db0::UniqueAddress(address(), rootUniqueAddress.getInstanceId());
    }

    namespace
    {
        EmbeddedObjectRef &embeddedMemoRef(MemoImmutableObject *object)
        {
            return getEmbeddedMemoRef(object);
        }

        db0::swine_ptr<Fixture> getRootFixture(PyObject *rootObject)
        {
            return reinterpret_cast<MemoImmutableObject *>(rootObject)->ext().getFixture();
        }

        ObjectSharedPtr unloadMember(EmbeddedObjectRef &embeddedRef, const FieldInfo &fieldInfo)
        {
            auto fixture = getRootFixture(embeddedRef.rootObject());
            return ObjectImmutableImpl::tryGetEmbeddedField(
                fixture, embeddedRef.rootObject(), embeddedRef.embeddedObject(), fieldInfo,
                reinterpret_cast<MemoImmutableObject *>(embeddedRef.rootObject())->ext().getMemberFlags()
            );
        }

        ObjectSharedPtr tryGetMember(EmbeddedObjectRef &embeddedRef, const char *attrName)
        {
            auto memberLoc = embeddedRef.type().findField(attrName);
            if (!memberLoc.first) {
                return {};
            }
            for (const auto &fieldInfo: memberLoc.first) {
                auto result = unloadMember(embeddedRef, fieldInfo);
                if (result.get()) {
                    return result;
                }
            }
            return {};
        }

        std::unordered_set<std::string> getEmbeddedMemberNames(
            const o_embedded_object &embeddedObject, Class &type
        )
        {
            std::unordered_set<std::string> result;
            auto &types = embeddedObject.pos_vt().types();
            unsigned int index = types.offset();
            for (unsigned int pos = 0; pos < types.size(); ++pos, ++index) {
                if (types[pos] == StorageClass::DELETED || types[pos] == StorageClass::UNDEFINED) {
                    continue;
                }
                result.insert(type.getMember(FieldID::fromIndex(index)).m_name);
            }

            for (const auto &xvalue: embeddedObject.index_vt().xvalues()) {
                if (xvalue.m_type == StorageClass::DELETED || xvalue.m_type == StorageClass::UNDEFINED) {
                    continue;
                }
                result.insert(type.getMember(FieldID::fromIndex(xvalue.getIndex())).m_name);
            }

            for (const auto &entry: embeddedObject.field_map()) {
                const auto &value = entry.value();
                if (value.itemKind() == StorageClass::DELETED || value.itemKind() == StorageClass::UNDEFINED) {
                    continue;
                }
                std::uint32_t memberIndex = 0;
                if (entry.key().itemKind() == StorageClass::PACKED_INT32) {
                    memberIndex = entry.key().packedIntPayload().value();
                } else if (entry.key().itemKind() == StorageClass::INT64) {
                    memberIndex = static_cast<std::uint32_t>(entry.key().intPayload().value());
                } else {
                    continue;
                }
                result.insert(type.getMember(FieldID::fromIndex(memberIndex)).m_name);
            }
            return result;
        }

        PyObject *tryEmbeddedObjectGetAttr(EmbeddedObject *self, PyObject *attr)
        {
            const char *attrName = PyUnicode_AsUTF8(attr);
            if (!attrName) {
                PyErr_SetString(PyExc_AttributeError, "Invalid attribute name");
                return nullptr;
            }

            if (!(attrName[0] == '_' && attrName[1] == 'X' && attrName[2] == '_' && attrName[3] == '_')) {
                auto fixture = getRootFixture(self->ext().rootObject());
                fixture->refreshIfUpdated();
                auto member = tryGetMember(self->modifyExt(), attrName);
                if (member.get()) {
                    return member.steal();
                }
            }

            return PyObject_GenericGetAttr(reinterpret_cast<PyObject *>(self), attr);
        }

        PyObject *PyAPI_EmbeddedObject_getattro(EmbeddedObject *self, PyObject *attr)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedObjectGetAttr, self, attr);
        }

        PyObject *tryEmbeddedMemoGetAttr(MemoImmutableObject *self, PyObject *attr)
        {
            const char *attrName = PyUnicode_AsUTF8(attr);
            if (!attrName) {
                PyErr_SetString(PyExc_AttributeError, "Invalid attribute name");
                return nullptr;
            }

            if (!(attrName[0] == '_' && attrName[1] == 'X' && attrName[2] == '_' && attrName[3] == '_')) {
                auto &embeddedRef = embeddedMemoRef(self);
                auto fixture = getRootFixture(embeddedRef.rootObject());
                fixture->refreshIfUpdated();
                auto member = tryGetMember(embeddedRef, attrName);
                if (member.get()) {
                    return member.steal();
                }
            }

            return PyObject_GenericGetAttr(reinterpret_cast<PyObject *>(self), attr);
        }

        PyObject *PyAPI_EmbeddedMemo_getattro(MemoImmutableObject *self, PyObject *attr)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedMemoGetAttr, self, attr);
        }

        int PyAPI_EmbeddedMemo_setattro(MemoImmutableObject *, PyObject *, PyObject *)
        {
            PY_API_FUNC
            PyErr_SetString(PyExc_AttributeError, "Cannot modify an embedded immutable memo object");
            return -1;
        }

        PyObject *tryEmbeddedObjectStr(EmbeddedObject *self)
        {
            std::stringstream str;
            str << "<dbzero.EmbeddedObject type=" << self->ext().type().getName() << ">";
            return PyUnicode_FromString(str.str().c_str());
        }

        PyObject *PyAPI_EmbeddedObject_str(EmbeddedObject *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedObjectStr, self);
        }

        void PyAPI_EmbeddedObject_del(EmbeddedObject *self)
        {
            PY_API_FUNC
            if (PyObject_GC_IsTracked(self)) {
                PyObject_GC_UnTrack(self);
            }
            self->destroy();
            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject *>(self));
        }

        void PyAPI_EmbeddedMemo_del(MemoImmutableObject *self)
        {
            PY_API_FUNC
            if (Py_IsInitialized()) {
                if (PyObject_GC_IsTracked(self)) {
                    PyObject_GC_UnTrack(self);
                }
                embeddedMemoRef(self).~EmbeddedObjectRef();
                Py_TYPE(self)->tp_free(reinterpret_cast<PyObject *>(self));
            }
        }

        int EmbeddedObject_traverse(EmbeddedObject *self, visitproc visit, void *arg)
        {
            Py_VISIT(self->ext().rootObject());
            return 0;
        }

        [[maybe_unused]] int EmbeddedMemo_traverse(MemoImmutableObject *self, visitproc visit, void *arg)
        {
            Py_VISIT(embeddedMemoRef(self).rootObject());
            return 0;
        }

        [[maybe_unused]] int EmbeddedMemo_clear(MemoImmutableObject *)
        {
            return 0;
        }

        PyObject *tryEmbeddedMemoStr(MemoImmutableObject *self)
        {
            std::stringstream str;
            str << "<" << Py_TYPE(self)->tp_base->tp_name
                << " embedded instance type=" << embeddedMemoRef(self).type().getName() << ">";
            return PyUnicode_FromString(str.str().c_str());
        }

        PyObject *PyAPI_EmbeddedMemo_str(MemoImmutableObject *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedMemoStr, self);
        }

        PyObject *PyAPI_EmbeddedMemo_dir(MemoImmutableObject *self, PyObject *)
        {
            PY_API_FUNC
            auto result = Py_OWN(PyObject_CallMethod(
                reinterpret_cast<PyObject *>(&PyBaseObject_Type), "__dir__",
                "O", reinterpret_cast<PyObject *>(self)
            ));
            if (!result) {
                return nullptr;
            }

            auto &type = embeddedMemoRef(self).type();
            for (const auto &name: getEmbeddedMemberNames(embeddedMemoRef(self).embeddedObject(), type)) {
                auto pyName = Py_OWN(PyUnicode_FromString(name.c_str()));
                if (!pyName || PySequence_Contains(*result, *pyName) == 1) {
                    continue;
                }
                if (PyList_Append(*result, *pyName) < 0) {
                    return nullptr;
                }
            }
            return result.steal();
        }

        PyObject *PyAPI_EmbeddedMemo_get_dict(MemoImmutableObject *self, void *)
        {
            PY_API_FUNC
            auto result = Py_OWN(PyDict_New());
            if (!result) {
                return nullptr;
            }

            auto &type = embeddedMemoRef(self).type();
            for (const auto &name: getEmbeddedMemberNames(embeddedMemoRef(self).embeddedObject(), type)) {
                auto value = tryGetMember(embeddedMemoRef(self), name.c_str());
                if (!value.get()) {
                    continue;
                }
                auto pyName = Py_OWN(PyUnicode_FromString(name.c_str()));
                if (!pyName || PyDict_SetItem(*result, *pyName, *value) < 0) {
                    return nullptr;
                }
            }
            return result.steal();
        }

        Py_hash_t PyAPI_EmbeddedMemo_hash(MemoImmutableObject *self)
        {
            PY_API_FUNC
            // Runtime Python hash only. Embedded memo wrappers may be transformed
            // in-place after insertion into a Python set, so keep the hash tied
            // to the wrapper address. Durable db0 hashing must use getPyHash().
            auto hash = static_cast<Py_hash_t>(reinterpret_cast<std::uintptr_t>(self));
            return hash == -1 ? -2 : hash;
        }

        PyObject *tryEmbeddedMemoRichCompare(MemoImmutableObject *self, PyObject *other, int op)
        {
            if (op != Py_EQ && op != Py_NE) {
                Py_RETURN_NOTIMPLEMENTED;
            }

            bool isEqual = false;
            if (PyEmbeddedMemo_Check(other)) {
                auto &lhs = embeddedMemoRef(self);
                auto &rhs = embeddedMemoRef(reinterpret_cast<MemoImmutableObject *>(other));
                isEqual = lhs.fixture()->getUUID() == rhs.fixture()->getUUID()
                    && lhs.uniqueAddress() == rhs.uniqueAddress();
            }

            return PyBool_fromBool(op == Py_EQ ? isEqual : !isEqual);
        }

        PyObject *PyAPI_EmbeddedMemo_richcompare(MemoImmutableObject *self, PyObject *other, int op)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedMemoRichCompare, self, other, op);
        }

        static PyMethodDef EmbeddedMemo_methods[] = {
            {"__dir__", (PyCFunction)PyAPI_EmbeddedMemo_dir, METH_NOARGS, nullptr},
            {NULL}
        };

        static PyGetSetDef EmbeddedMemo_getsets[] = {
            {"__dict__", (getter)PyAPI_EmbeddedMemo_get_dict, nullptr, nullptr, nullptr},
            {nullptr}
        };

        std::string consumePyErrorMessage();

        PyTypeObject *createEmbeddedMemoType(PyTypeObject *memoType)
        {
            std::vector<PyType_Slot> slots = {
                {Py_tp_dealloc, reinterpret_cast<void *>(PyAPI_EmbeddedMemo_del)},
                {Py_tp_getattro, reinterpret_cast<void *>(PyAPI_EmbeddedMemo_getattro)},
                {Py_tp_setattro, reinterpret_cast<void *>(PyAPI_EmbeddedMemo_setattro)},
                {Py_tp_methods, reinterpret_cast<void *>(EmbeddedMemo_methods)},
                {Py_tp_getset, reinterpret_cast<void *>(EmbeddedMemo_getsets)},
                {Py_tp_hash, reinterpret_cast<void *>(PyAPI_EmbeddedMemo_hash)},
                {Py_tp_richcompare, reinterpret_cast<void *>(PyAPI_EmbeddedMemo_richcompare)},
                {Py_tp_repr, reinterpret_cast<void *>(PyAPI_EmbeddedMemo_str)},
                {Py_tp_str, reinterpret_cast<void *>(PyAPI_EmbeddedMemo_str)},
                {0, 0}
            };
            if (memoType->tp_flags & Py_TPFLAGS_HAVE_GC) {
                slots.insert(slots.end() - 1, {
                    {Py_tp_traverse, reinterpret_cast<void *>(EmbeddedMemo_traverse)},
                    {Py_tp_clear, reinterpret_cast<void *>(EmbeddedMemo_clear)}
                });
            }

            std::stringstream typeName;
            typeName << memoType->tp_name << ".__dbzero_embedded_view__";
            const char *safeName = PyToolkit::getTypeManager().getPooledString(typeName.str());
            std::uint32_t flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
            if (memoType->tp_flags & Py_TPFLAGS_HAVE_GC) {
                flags |= Py_TPFLAGS_HAVE_GC;
            }
            flags &= ~Py_TPFLAGS_MANAGED_DICT;

            auto spec = PyType_Spec {
                .name = safeName,
                .basicsize = static_cast<int>(memoType->tp_basicsize),
                .itemsize = 0,
                .flags = flags,
                .slots = slots.data()
            };
            auto bases = Py_OWN(PySafeTuple_Pack(Py_BORROW(memoType)));
            auto shadowType = reinterpret_cast<PyTypeObject *>(PyType_FromSpecWithBases(&spec, *bases));
            if (!shadowType) {
                return nullptr;
            }

            shadowType->tp_weaklistoffset = memoType->tp_weaklistoffset;
            shadowType->tp_dictoffset = memoType->tp_dictoffset;
            if (shadowType->tp_basicsize != memoType->tp_basicsize) {
                Py_DECREF(shadowType);
                PyErr_SetString(PyExc_RuntimeError, "Embedded memo shadow type size mismatch");
                return nullptr;
            }
            if ((shadowType->tp_flags & Py_TPFLAGS_HAVE_GC) != (memoType->tp_flags & Py_TPFLAGS_HAVE_GC)) {
                Py_DECREF(shadowType);
                PyErr_SetString(PyExc_RuntimeError, "Embedded memo shadow type GC flag mismatch");
                return nullptr;
            }

            return shadowType;
        }

        PyTypeObject *getEmbeddedMemoType(PyTypeObject *memoType)
        {
            auto *embeddedType = PyToolkit::getTypeManager().getEmbeddedMemoType(memoType, createEmbeddedMemoType);
            if (!embeddedType) {
                THROWF(db0::InternalException)
                    << "Unable to create embedded memo shadow type: " << consumePyErrorMessage();
            }
            return embeddedType;
        }

        Py_ssize_t embeddedSequenceSize(PyObject *sequence)
        {
            if (PyTuple_Check(sequence)) {
                return PyTuple_GET_SIZE(sequence);
            }
            if (PyList_Check(sequence)) {
                return PyList_GET_SIZE(sequence);
            }
            return -1;
        }

        PyObject *embeddedSequenceItem(PyObject *sequence, Py_ssize_t index)
        {
            if (PyTuple_Check(sequence)) {
                return PyTuple_GET_ITEM(sequence, index);
            }
            return PyList_GET_ITEM(sequence, index);
        }

        void transformEmbeddedTupleObjects(
            db0::swine_ptr<Fixture> &fixture, ClassFactory &classFactory, PyObject *rootObject,
            PyObject *sourceSequence, const o_py_tuple &embeddedTuple
        );

        void transformEmbeddedSetObjects(
            db0::swine_ptr<Fixture> &fixture, ClassFactory &classFactory, PyObject *rootObject,
            PyObject *sourceSet, const o_py_set &embeddedSet
        );

        void transformEmbeddedDictObjects(
            db0::swine_ptr<Fixture> &fixture, ClassFactory &classFactory, PyObject *rootObject,
            PyObject *sourceDict, const o_py_dict &embeddedDict
        );

        void transformEmbeddedItem(
            db0::swine_ptr<Fixture> &fixture, ClassFactory &classFactory, PyObject *rootObject,
            PyObject *sourceItem, const o_tuple_item &embeddedItem
        )
        {
            if (PyEmbeddedMemo_Check(sourceItem)) {
                return;
            }

            if (PyMemo_Check<MemoImmutableObject>(sourceItem)) {
                assert(embeddedItem.itemKind() == StorageClass::EMBEDDED_OBJECT);
                const auto &embeddedObject = o_embedded_object::__const_ref(
                    embeddedItem.embeddedPayload().begin()
                );
                transformEmbeddedObject(fixture, rootObject, sourceItem, embeddedObject);
                return;
            }

            if (PyTuple_Check(sourceItem) || PyList_Check(sourceItem)) {
                assert(embeddedItem.itemKind() == StorageClass::EMBEDDED_TUPLE);
                const auto &nestedTuple = o_py_tuple::__const_ref(embeddedItem.embeddedPayload().begin());
                transformEmbeddedTupleObjects(fixture, classFactory, rootObject, sourceItem, nestedTuple);
                return;
            }

            if (PySet_Check(sourceItem)) {
                assert(embeddedItem.itemKind() == StorageClass::EMBEDDED_SET);
                const auto &nestedSet = o_py_set::__const_ref(embeddedItem.embeddedPayload().begin());
                transformEmbeddedSetObjects(fixture, classFactory, rootObject, sourceItem, nestedSet);
                return;
            }

            if (PyDict_Check(sourceItem)) {
                assert(embeddedItem.itemKind() == StorageClass::EMBEDDED_DICT);
                const auto &nestedDict = o_py_dict::__const_ref(embeddedItem.embeddedPayload().begin());
                transformEmbeddedDictObjects(fixture, classFactory, rootObject, sourceItem, nestedDict);
            }
        }

        void transformEmbeddedTupleObjects(
            db0::swine_ptr<Fixture> &fixture, ClassFactory &classFactory, PyObject *rootObject,
            PyObject *sourceSequence, const o_py_tuple &embeddedTuple
        )
        {
            // During immutable materialization, tuple/list fields are copied into the root object's embedded
            // storage. Any non-materialized immutable memo object originally present in that Python sequence
            // must then be morphed in place into an embedded memo view. The Python object keeps its identity,
            // but its native payload now points at the embedded object stored under rootObject. Walk the source
            // Python sequence in lockstep with the persisted embedded tuple so nested tuple/list elements can
            // be fixed up recursively.
            auto sourceSize = embeddedSequenceSize(sourceSequence);
            assert(sourceSize >= 0);
            assert(static_cast<std::size_t>(sourceSize) == embeddedTuple.size());

            for (Py_ssize_t index = 0; index < sourceSize; ++index) {
                auto *sourceItem = embeddedSequenceItem(sourceSequence, index);
                const auto &embeddedItem = embeddedTuple.item(static_cast<std::size_t>(index));
                transformEmbeddedItem(fixture, classFactory, rootObject, sourceItem, embeddedItem);
            }
        }

        void transformEmbeddedSetObjects(
            db0::swine_ptr<Fixture> &fixture, ClassFactory &classFactory, PyObject *rootObject,
            PyObject *sourceSet, const o_py_set &embeddedSet
        )
        {
            // o_py_set is constructed by iterating the source Python set, so while that set is unchanged
            // we can walk both containers in the same order and morph any immutable memo elements in place.
            assert(PySet_Check(sourceSet));
            assert(static_cast<std::size_t>(PySet_GET_SIZE(sourceSet)) == embeddedSet.size());

            auto iterator = Py_OWN(PyObject_GetIter(sourceSet));
            assert(iterator.get());

            auto embeddedItem = embeddedSet.begin();
            Py_FOR(sourceItem, iterator) {
                assert(embeddedItem != embeddedSet.end());
                transformEmbeddedItem(fixture, classFactory, rootObject, *sourceItem, *embeddedItem);
                ++embeddedItem;
            }
            assert(embeddedItem == embeddedSet.end());
        }

        void transformEmbeddedDictObjects(
            db0::swine_ptr<Fixture> &fixture, ClassFactory &classFactory, PyObject *rootObject,
            PyObject *sourceDict, const o_py_dict &embeddedDict
        )
        {
            assert(PyDict_Check(sourceDict));
            assert(static_cast<std::size_t>(PyDict_Size(sourceDict)) == embeddedDict.size());

            auto iterator = Py_OWN(PyObject_GetIter(sourceDict));
            assert(iterator.get());

            auto embeddedPair = embeddedDict.begin();
            Py_FOR(sourceKey, iterator) {
                assert(embeddedPair != embeddedDict.end());
                auto *sourceValue = PyDict_GetItemWithError(sourceDict, *sourceKey);
                assert(sourceValue);
                transformEmbeddedItem(fixture, classFactory, rootObject, *sourceKey, embeddedPair->key());
                transformEmbeddedItem(fixture, classFactory, rootObject, sourceValue, embeddedPair->value());
                ++embeddedPair;
            }
            assert(embeddedPair == embeddedDict.end());
        }

        std::string consumePyErrorMessage()
        {
            if (!PyErr_Occurred()) {
                return "unknown Python error";
            }
            PyObject *ptype = nullptr;
            PyObject *pvalue = nullptr;
            PyObject *ptraceback = nullptr;
            PyErr_Fetch(&ptype, &pvalue, &ptraceback);
            PyErr_NormalizeException(&ptype, &pvalue, &ptraceback);
            auto str = Py_OWN(PyObject_Str(pvalue ? pvalue : Py_None));
            std::string result = str.get() ? PyUnicode_AsUTF8(*str) : "unable to format Python error";
            Py_XDECREF(ptype);
            Py_XDECREF(pvalue);
            Py_XDECREF(ptraceback);
            return result;
        }
    }

    PyTypeObject EmbeddedObjectType = {
        PyVarObject_HEAD_INIT(nullptr, 0)
        .tp_name = "dbzero.EmbeddedObject",
        .tp_basicsize = static_cast<Py_ssize_t>(EmbeddedObject::sizeOf()),
        .tp_itemsize = 0,
        .tp_dealloc = reinterpret_cast<destructor>(PyAPI_EmbeddedObject_del),
        .tp_vectorcall_offset = 0,
        .tp_getattr = nullptr,
        .tp_setattr = nullptr,
        .tp_as_async = nullptr,
        .tp_repr = reinterpret_cast<reprfunc>(PyAPI_EmbeddedObject_str),
        .tp_as_number = nullptr,
        .tp_as_sequence = nullptr,
        .tp_as_mapping = nullptr,
        .tp_hash = nullptr,
        .tp_call = nullptr,
        .tp_str = reinterpret_cast<reprfunc>(PyAPI_EmbeddedObject_str),
        .tp_getattro = reinterpret_cast<getattrofunc>(PyAPI_EmbeddedObject_getattro),
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
        .tp_doc = "dbzero embedded immutable object view",
        .tp_traverse = reinterpret_cast<traverseproc>(EmbeddedObject_traverse),
        .tp_alloc = PyType_GenericAlloc,
        .tp_free = PyObject_GC_Del,
    };

    ObjectSharedPtr makeEmbeddedObject(
        PyObject *rootObject, const o_embedded_object &embeddedObject, std::shared_ptr<Class> type
    )
    {
        auto *pyObject = reinterpret_cast<EmbeddedObject *>(EmbeddedObjectType.tp_alloc(&EmbeddedObjectType, 0));
        if (!pyObject) {
            return {};
        }
        pyObject->makeNew(rootObject, &embeddedObject, std::move(type));
        return Py_OWN(reinterpret_cast<PyObject *>(pyObject));
    }

    ObjectSharedPtr makeEmbeddedMemoObject(
        PyObject *rootObject, const o_embedded_object &embeddedObject, std::shared_ptr<Class> type,
        PyTypeObject *memoType
    )
    {
        auto *embeddedType = getEmbeddedMemoType(memoType);
        auto *pyObject = reinterpret_cast<MemoImmutableObject *>(embeddedType->tp_alloc(embeddedType, 0));
        if (!pyObject) {
            return {};
        }
        new ((void *)const_cast<MemoImmutableObject::ExtT *>(&pyObject->ext()))
            EmbeddedObjectRef(rootObject, &embeddedObject, std::move(type));
        return Py_OWN(reinterpret_cast<PyObject *>(pyObject));
    }

    void transformMemoImmutableObjectToEmbedded(
        MemoImmutableObject *object, PyObject *rootObject, const o_embedded_object &embeddedObject,
        std::shared_ptr<Class> type
    )
    {
        auto *oldType = Py_TYPE(object);
        auto *embeddedType = getEmbeddedMemoType(oldType);
        if (PyObject_GC_IsTracked(object)) {
            PyObject_GC_UnTrack(object);
        }
        InitManager::instance.tryCloseInitializer(object->ext());
        object->destroy();
        new ((void *)const_cast<MemoImmutableObject::ExtT *>(&object->ext()))
            EmbeddedObjectRef(rootObject, &embeddedObject, std::move(type));
        Py_INCREF(embeddedType);
        Py_SET_TYPE(object, embeddedType);
        Py_DECREF(oldType);
        if (Py_TYPE(object)->tp_flags & Py_TPFLAGS_HAVE_GC) {
            PyObject_GC_Track(object);
        }
    }

    void transformEmbeddedObject(
        db0::swine_ptr<Fixture> &fixture, PyTypes::ObjectPtr rootObject, PyTypes::ObjectPtr sourceObject,
        const o_embedded_object &embeddedObject
    )
    {
        if (PyEmbeddedMemo_Check(sourceObject)) {
            return;
        }

        assert(PyMemo_Check<MemoImmutableObject>(sourceObject));
        auto &classFactory = fixture->get<ClassFactory>();
        auto type = classFactory.getTypeByClassRef(embeddedObject.getClassRef()).m_class;
        auto *embeddedMemo = reinterpret_cast<MemoImmutableObject *>(sourceObject);
        transformMemoImmutableObjectToEmbedded(embeddedMemo, rootObject, embeddedObject, std::move(type));
    }

    void transformEmbeddedTuple(
        db0::swine_ptr<Fixture> &fixture, PyTypes::ObjectPtr rootObject, PyTypes::ObjectPtr sourceSequence,
        const o_py_tuple &embeddedTuple
    )
    {
        auto &classFactory = fixture->get<ClassFactory>();
        transformEmbeddedTupleObjects(fixture, classFactory, rootObject, sourceSequence, embeddedTuple);
    }

    void transformEmbeddedSet(
        db0::swine_ptr<Fixture> &fixture, PyTypes::ObjectPtr rootObject, PyTypes::ObjectPtr sourceSet,
        const o_py_set &embeddedSet
    )
    {
        auto &classFactory = fixture->get<ClassFactory>();
        transformEmbeddedSetObjects(fixture, classFactory, rootObject, sourceSet, embeddedSet);
    }

    void transformEmbeddedDict(
        db0::swine_ptr<Fixture> &fixture, PyTypes::ObjectPtr rootObject, PyTypes::ObjectPtr sourceDict,
        const o_py_dict &embeddedDict
    )
    {
        auto &classFactory = fixture->get<ClassFactory>();
        transformEmbeddedDictObjects(fixture, classFactory, rootObject, sourceDict, embeddedDict);
    }

    bool PyEmbeddedMemoType_Check(PyTypeObject *type)
    {
        return PyToolkit::getTypeManager().isEmbeddedMemoType(type);
    }

    bool PyEmbeddedMemo_Check(PyObject *object)
    {
        return object && PyEmbeddedMemoType_Check(Py_TYPE(object));
    }

    EmbeddedObjectRef &getEmbeddedMemoRef(MemoImmutableObject *object)
    {
        return *reinterpret_cast<EmbeddedObjectRef *>(const_cast<MemoImmutableObject::ExtT *>(&object->ext()));
    }

    const EmbeddedObjectRef &getEmbeddedMemoRef(const MemoImmutableObject *object)
    {
        return *reinterpret_cast<const EmbeddedObjectRef *>(&object->ext());
    }

    db0::swine_ptr<Fixture> getEmbeddedMemoFixture(PyObject *object)
    {
        assert(PyEmbeddedMemo_Check(object));
        return getEmbeddedMemoRef(reinterpret_cast<MemoImmutableObject *>(object)).fixture();
    }

    db0::Address getEmbeddedMemoAddress(PyObject *object)
    {
        assert(PyEmbeddedMemo_Check(object));
        return getEmbeddedMemoRef(reinterpret_cast<MemoImmutableObject *>(object)).address();
    }

    db0::UniqueAddress getEmbeddedMemoUniqueAddress(PyObject *object)
    {
        assert(PyEmbeddedMemo_Check(object));
        return getEmbeddedMemoRef(reinterpret_cast<MemoImmutableObject *>(object)).uniqueAddress();
    }

    void incEmbeddedMemoRef(PyObject *object, bool isTag)
    {
        assert(PyEmbeddedMemo_Check(object));
        auto *rootObject = getEmbeddedMemoRef(reinterpret_cast<MemoImmutableObject *>(object)).rootObject();
        reinterpret_cast<MemoImmutableObject *>(rootObject)->modifyExt().incRef(isTag);
    }
}
