// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <dbzero/bindings/python/embedded/EmbeddedDict.hpp>

#include <dbzero/bindings/python/embedded/EmbeddedObject.hpp>
#include <dbzero/bindings/python/MemoObject.hpp>
#include <dbzero/bindings/python/PyInternalAPI.hpp>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/object_model/dict/o_py_dict.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
#include <dbzero/workspace/Fixture.hpp>

#include <climits>
#include <cmath>
#include <sstream>
#include <utility>

namespace db0::python
{
    using ObjectSharedPtr = PyTypes::ObjectSharedPtr;
    using namespace db0::object_model;

    EmbeddedDictRef::EmbeddedDictRef(PyObject *rootObject, const o_py_dict *dict)
        : m_root_object(rootObject)
        , m_dict(dict)
    {
        Py_XINCREF(m_root_object);
    }

    EmbeddedDictRef::~EmbeddedDictRef()
    {
        Py_XDECREF(m_root_object);
    }

    PyObject *EmbeddedDictRef::rootObject() const
    {
        return m_root_object;
    }

    const o_py_dict &EmbeddedDictRef::dict() const
    {
        return *m_dict;
    }

    EmbeddedDictIteratorRef::EmbeddedDictIteratorRef(PyObject *dictObject, ItemKind itemKind)
        : m_dict_object(dictObject)
        , m_item_kind(itemKind)
        , m_iterator(reinterpret_cast<EmbeddedDict *>(dictObject)->ext().dict().begin())
        , m_end(reinterpret_cast<EmbeddedDict *>(dictObject)->ext().dict().end())
    {
        Py_XINCREF(m_dict_object);
    }

    EmbeddedDictIteratorRef::~EmbeddedDictIteratorRef()
    {
        Py_XDECREF(m_dict_object);
    }

    PyObject *EmbeddedDictIteratorRef::dictObject() const
    {
        return m_dict_object;
    }

    EmbeddedDictIteratorRef::ItemKind EmbeddedDictIteratorRef::itemKind() const
    {
        return m_item_kind;
    }

    const o_dict::Pair *EmbeddedDictIteratorRef::next()
    {
        if (m_iterator == m_end) {
            return nullptr;
        }
        const auto *pair = &*m_iterator;
        ++m_iterator;
        return pair;
    }

    namespace
    {
        db0::swine_ptr<Fixture> getRootFixture(PyObject *rootObject)
        {
            return reinterpret_cast<MemoImmutableObject *>(rootObject)->ext().getFixture();
        }

        PyObject *unloadEmbeddedDictItem(PyObject *rootObject, const o_dict::Item &item)
        {
            auto fixture = getRootFixture(rootObject);
            fixture->refreshIfUpdated();
            return PyToolkit::unloadEmbeddedInstance(fixture, rootObject, item).steal();
        }

        PyObject *makeEmbeddedDictIterator(EmbeddedDict *self, EmbeddedDictIteratorRef::ItemKind itemKind)
        {
            auto *pyObject = reinterpret_cast<EmbeddedDictIterator *>(
                EmbeddedDictIteratorType.tp_alloc(&EmbeddedDictIteratorType, 0)
            );
            if (!pyObject) {
                return nullptr;
            }
            pyObject->makeNew(reinterpret_cast<PyObject *>(self), itemKind);
            return reinterpret_cast<PyObject *>(pyObject);
        }

        PyObject *tryEmbeddedDictIter(EmbeddedDict *self)
        {
            return makeEmbeddedDictIterator(self, EmbeddedDictIteratorRef::ItemKind::KEY);
        }

        PyObject *PyAPI_EmbeddedDict_iter(EmbeddedDict *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedDictIter, self);
        }

        PyObject *tryEmbeddedDictIteratorNext(EmbeddedDictIterator *self)
        {
            auto *dictObject = reinterpret_cast<EmbeddedDict *>(self->ext().dictObject());
            auto *pair = self->modifyExt().next();
            if (!pair) {
                return nullptr;
            }
            switch (self->ext().itemKind()) {
                case EmbeddedDictIteratorRef::ItemKind::KEY:
                    return unloadEmbeddedDictItem(dictObject->ext().rootObject(), pair->key());
                case EmbeddedDictIteratorRef::ItemKind::VALUE:
                    return unloadEmbeddedDictItem(dictObject->ext().rootObject(), pair->value());
                case EmbeddedDictIteratorRef::ItemKind::PAIR: {
                    auto key = Py_OWN(unloadEmbeddedDictItem(dictObject->ext().rootObject(), pair->key()));
                    auto value = Py_OWN(unloadEmbeddedDictItem(dictObject->ext().rootObject(), pair->value()));
                    if (!key || !value) {
                        return nullptr;
                    }
                    auto pairTuple = Py_OWN(PyTuple_New(2));
                    if (!pairTuple) {
                        return nullptr;
                    }
                    PySafeTuple_SetItem(*pairTuple, 0, std::move(key));
                    PySafeTuple_SetItem(*pairTuple, 1, std::move(value));
                    return pairTuple.steal();
                }
            }
            PyErr_SetString(PyExc_SystemError, "unknown embedded dict iterator mode");
            return nullptr;
        }

        PyObject *PyAPI_EmbeddedDictIterator_next(EmbeddedDictIterator *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedDictIteratorNext, self);
        }

        Py_ssize_t tryEmbeddedDictLen(EmbeddedDict *self)
        {
            return static_cast<Py_ssize_t>(self->ext().dict().size());
        }

        Py_ssize_t PyAPI_EmbeddedDict_len(EmbeddedDict *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedDictLen, self);
        }

        template <typename ActionT>
        void forLookupElementsFromPythonObject(EmbeddedDict *self, PyObject *needle, ActionT action)
        {
            if (PyEmbeddedMemo_Check(needle)) {
                auto *embeddedMemo = reinterpret_cast<MemoImmutableObject *>(needle);
                const auto *embeddedRef = reinterpret_cast<const EmbeddedObjectRef *>(&embeddedMemo->ext());
                if (embeddedRef->rootObject() != self->ext().rootObject()) {
                    return;
                }
                const auto &embeddedObject = embeddedRef->embeddedObject();
                action(o_dict::Element::embeddedObject(&embeddedObject, embeddedObject.sizeOf()));
                return;
            }

            if (PyObject_Hash(needle) == -1) {
                return;
            }

            auto primaryElement = o_py_dict::elementFromPythonObject(needle);
            if (action(primaryElement)) {
                return;
            }

            if (PyBool_Check(needle)) {
                auto value = needle == Py_True ? 1 : 0;
                if (action(o_dict::Element::integer(value))) {
                    return;
                }
                action(o_dict::Element::floating(static_cast<double>(value)));
            } else if (PyLong_Check(needle)) {
                auto value = PyLong_AsLongLong(needle);
                if (PyErr_Occurred()) {
                    PyErr_Clear();
                } else {
                    if (value == 0 || value == 1) {
                        if (action(o_dict::Element::boolean(value != 0))) {
                            return;
                        }
                    }
                    auto floatValue = PyFloat_AsDouble(needle);
                    if (!PyErr_Occurred() && std::isfinite(floatValue)
                        && floatValue >= static_cast<double>(LLONG_MIN)
                        && floatValue <= static_cast<double>(LLONG_MAX)
                        && static_cast<long long>(floatValue) == value) {
                        action(o_dict::Element::floating(floatValue));
                    } else if (PyErr_Occurred()) {
                        PyErr_Clear();
                    }
                }
            } else if (PyFloat_Check(needle)) {
                auto floatValue = PyFloat_AsDouble(needle);
                if (!PyErr_Occurred() && std::isfinite(floatValue)
                    && std::trunc(floatValue) == floatValue
                    && floatValue >= static_cast<double>(LLONG_MIN)
                    && floatValue <= static_cast<double>(LLONG_MAX)) {
                    auto intValue = static_cast<long long>(floatValue);
                    if (action(o_dict::Element::integer(intValue))) {
                        return;
                    }
                    if (intValue == 0 || intValue == 1) {
                        action(o_dict::Element::boolean(intValue != 0));
                    }
                }
                if (PyErr_Occurred()) {
                    PyErr_Clear();
                }
            }
        }

        const o_dict::Item *findEmbeddedDictValue(EmbeddedDict *self, PyObject *needle)
        {
            const o_dict::Item *foundValue = nullptr;
            forLookupElementsFromPythonObject(self, needle, [&](const o_dict::Element &element) {
                auto *value = self->ext().dict().get(element);
                if (value) {
                    foundValue = value;
                    return true;
                }
                return false;
            });
            return foundValue;
        }

        int tryEmbeddedDictContains(EmbeddedDict *self, PyObject *needle)
        {
            auto *value = findEmbeddedDictValue(self, needle);
            if (PyErr_Occurred()) {
                return -1;
            }
            return value ? 1 : 0;
        }

        int PyAPI_EmbeddedDict_contains(EmbeddedDict *self, PyObject *needle)
        {
            PY_API_FUNC
            return runSafe<-1>(tryEmbeddedDictContains, self, needle);
        }

        PyObject *tryEmbeddedDictSubscript(EmbeddedDict *self, PyObject *key)
        {
            auto *value = findEmbeddedDictValue(self, key);
            if (PyErr_Occurred()) {
                return nullptr;
            }
            if (!value) {
                PyErr_SetObject(PyExc_KeyError, key);
                return nullptr;
            }
            return unloadEmbeddedDictItem(self->ext().rootObject(), *value);
        }

        PyObject *PyAPI_EmbeddedDict_subscript(EmbeddedDict *self, PyObject *key)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedDictSubscript, self, key);
        }

        PyObject *tryEmbeddedDictKeys(EmbeddedDict *self, PyObject *)
        {
            return makeEmbeddedDictIterator(self, EmbeddedDictIteratorRef::ItemKind::KEY);
        }

        PyObject *PyAPI_EmbeddedDict_keys(EmbeddedDict *self, PyObject *args)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedDictKeys, self, args);
        }

        PyObject *tryEmbeddedDictValues(EmbeddedDict *self, PyObject *)
        {
            return makeEmbeddedDictIterator(self, EmbeddedDictIteratorRef::ItemKind::VALUE);
        }

        PyObject *PyAPI_EmbeddedDict_values(EmbeddedDict *self, PyObject *args)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedDictValues, self, args);
        }

        PyObject *tryEmbeddedDictItems(EmbeddedDict *self, PyObject *)
        {
            return makeEmbeddedDictIterator(self, EmbeddedDictIteratorRef::ItemKind::PAIR);
        }

        PyObject *PyAPI_EmbeddedDict_items(EmbeddedDict *self, PyObject *args)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedDictItems, self, args);
        }

        PyObject *tryEmbeddedDictGet(EmbeddedDict *self, PyObject *const *args, Py_ssize_t nargs)
        {
            auto *value = findEmbeddedDictValue(self, args[0]);
            if (PyErr_Occurred()) {
                return nullptr;
            }
            if (value) {
                return unloadEmbeddedDictItem(self->ext().rootObject(), *value);
            }
            if (nargs == 2) {
                Py_INCREF(args[1]);
                return args[1];
            }
            Py_RETURN_NONE;
        }

        PyObject *PyAPI_EmbeddedDict_get(EmbeddedDict *self, PyObject *const *args, Py_ssize_t nargs)
        {
            PY_API_FUNC
            if (nargs < 1 || nargs > 2) {
                PyErr_SetString(PyExc_TypeError, "get() takes one or two arguments.");
                return nullptr;
            }
            return runSafe(tryEmbeddedDictGet, self, args, nargs);
        }

        PyObject *tryEmbeddedDictStr(EmbeddedDict *self)
        {
            std::stringstream str;
            str << "{";
            bool first = true;
            for (const auto &pair: self->ext().dict()) {
                if (!first) {
                    str << ", ";
                }
                first = false;

                auto key = Py_OWN(unloadEmbeddedDictItem(self->ext().rootObject(), pair.key()));
                auto value = Py_OWN(unloadEmbeddedDictItem(self->ext().rootObject(), pair.value()));
                if (!key || !value) {
                    return nullptr;
                }
                auto keyRepr = Py_OWN(PyObject_Repr(*key));
                auto valueRepr = Py_OWN(PyObject_Repr(*value));
                if (!keyRepr || !valueRepr) {
                    return nullptr;
                }
                str << PyUnicode_AsUTF8(*keyRepr) << ": " << PyUnicode_AsUTF8(*valueRepr);
            }
            str << "}";
            return PyUnicode_FromString(str.str().c_str());
        }

        PyObject *PyAPI_EmbeddedDict_str(EmbeddedDict *self)
        {
            PY_API_FUNC
            return runSafe(tryEmbeddedDictStr, self);
        }

        void PyAPI_EmbeddedDict_del(EmbeddedDict *self)
        {
            PY_API_FUNC
            if (PyObject_GC_IsTracked(self)) {
                PyObject_GC_UnTrack(self);
            }
            self->destroy();
            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject *>(self));
        }

        void PyAPI_EmbeddedDictIterator_del(EmbeddedDictIterator *self)
        {
            PY_API_FUNC
            if (PyObject_GC_IsTracked(self)) {
                PyObject_GC_UnTrack(self);
            }
            self->destroy();
            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject *>(self));
        }

        int EmbeddedDict_traverse(EmbeddedDict *self, visitproc visit, void *arg)
        {
            Py_VISIT(self->ext().rootObject());
            return 0;
        }

        int EmbeddedDictIterator_traverse(EmbeddedDictIterator *self, visitproc visit, void *arg)
        {
            Py_VISIT(self->ext().dictObject());
            return 0;
        }

        static PySequenceMethods EmbeddedDict_sq = {
            .sq_length = reinterpret_cast<lenfunc>(PyAPI_EmbeddedDict_len),
            .sq_contains = reinterpret_cast<objobjproc>(PyAPI_EmbeddedDict_contains),
        };

        static PyMappingMethods EmbeddedDict_mp = {
            .mp_length = reinterpret_cast<lenfunc>(PyAPI_EmbeddedDict_len),
            .mp_subscript = reinterpret_cast<binaryfunc>(PyAPI_EmbeddedDict_subscript),
        };

        static PyMethodDef EmbeddedDict_methods[] = {
            {"get", reinterpret_cast<PyCFunction>(PyAPI_EmbeddedDict_get), METH_FASTCALL, nullptr},
            {"keys", reinterpret_cast<PyCFunction>(PyAPI_EmbeddedDict_keys), METH_NOARGS, nullptr},
            {"values", reinterpret_cast<PyCFunction>(PyAPI_EmbeddedDict_values), METH_NOARGS, nullptr},
            {"items", reinterpret_cast<PyCFunction>(PyAPI_EmbeddedDict_items), METH_NOARGS, nullptr},
            {NULL}
        };
    }

    PyTypeObject EmbeddedDictType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.EmbeddedDict",
        .tp_basicsize = static_cast<Py_ssize_t>(EmbeddedDict::sizeOf()),
        .tp_itemsize = 0,
        .tp_dealloc = reinterpret_cast<destructor>(PyAPI_EmbeddedDict_del),
        .tp_repr = reinterpret_cast<reprfunc>(PyAPI_EmbeddedDict_str),
        .tp_as_sequence = &EmbeddedDict_sq,
        .tp_as_mapping = &EmbeddedDict_mp,
        .tp_str = reinterpret_cast<reprfunc>(PyAPI_EmbeddedDict_str),
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
        .tp_doc = "dbzero embedded immutable dict view",
        .tp_traverse = reinterpret_cast<traverseproc>(EmbeddedDict_traverse),
        .tp_iter = reinterpret_cast<getiterfunc>(PyAPI_EmbeddedDict_iter),
        .tp_methods = EmbeddedDict_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_free = PyObject_GC_Del,
    };

    PyTypeObject EmbeddedDictIteratorType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.EmbeddedDictIterator",
        .tp_basicsize = static_cast<Py_ssize_t>(EmbeddedDictIterator::sizeOf()),
        .tp_itemsize = 0,
        .tp_dealloc = reinterpret_cast<destructor>(PyAPI_EmbeddedDictIterator_del),
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
        .tp_doc = "dbzero embedded immutable dict iterator",
        .tp_traverse = reinterpret_cast<traverseproc>(EmbeddedDictIterator_traverse),
        .tp_iter = PyObject_SelfIter,
        .tp_iternext = reinterpret_cast<iternextfunc>(PyAPI_EmbeddedDictIterator_next),
        .tp_alloc = PyType_GenericAlloc,
        .tp_free = PyObject_GC_Del,
    };

    ObjectSharedPtr makeEmbeddedDict(PyObject *rootObject, const o_py_dict &dict)
    {
        auto *pyObject = reinterpret_cast<EmbeddedDict *>(EmbeddedDictType.tp_alloc(&EmbeddedDictType, 0));
        if (!pyObject) {
            return {};
        }
        pyObject->makeNew(rootObject, &dict);
        return Py_OWN(reinterpret_cast<PyObject *>(pyObject));
    }
}
