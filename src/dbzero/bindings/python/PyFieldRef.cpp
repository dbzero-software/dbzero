// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include "PyFieldRef.hpp"
#include "Memo.hpp"
#include "MemoTypeDecoration.hpp"
#include "PyInternalAPI.hpp"
#include "PySafeAPI.hpp"
#include <algorithm>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace db0::python
{

    namespace
    {
        constexpr const char *FIELD_NAMESPACE_ATTR = "__DBZERO_FIELD_NAMESPACE";

        bool isDunderName(const char *name)
        {
            auto len = std::strlen(name);
            return len >= 4 && name[0] == '_' && name[1] == '_' && name[len - 2] == '_' && name[len - 1] == '_';
        }

        void appendUnique(std::vector<std::string> &names, const std::string &name)
        {
            if (std::find(names.begin(), names.end(), name) == names.end()) {
                names.push_back(name);
            }
        }

        void appendDecorationNames(PyTypeObject *memo_type, std::vector<std::string> &names)
        {
            auto &decor = MemoTypeDecoration::get(memo_type);
            for (const auto &name: decor.getInitVars()) {
                appendUnique(names, name);
            }
            for (const auto &name: decor.getTagFields()) {
                appendUnique(names, name);
            }
            for (const auto &name: decor.getIndexedFields()) {
                appendUnique(names, name);
            }
        }

        bool pyTupleContainsString(PyObject *tuple, const char *name)
        {
            if (!tuple || !PyTuple_Check(tuple)) {
                return false;
            }
            auto size = PyTuple_Size(tuple);
            for (Py_ssize_t index = 0; index < size; ++index) {
                auto item = PyTuple_GetItem(tuple, index);
                if (PyUnicode_Check(item)) {
                    auto item_name = PyUnicode_AsUTF8(item);
                    if (item_name && std::strcmp(item_name, name) == 0) {
                        return true;
                    }
                }
            }
            return false;
        }

        bool hasDeclaredName(PyTypeObject *memo_type, const char *name)
        {
            auto &decor = MemoTypeDecoration::get(memo_type);
            for (const auto &field_name: decor.getInitVars()) {
                if (field_name == name) {
                    return true;
                }
            }
            for (const auto &field_name: decor.getTagFields()) {
                if (field_name == name) {
                    return true;
                }
            }
            for (const auto &field_name: decor.getIndexedFields()) {
                if (field_name == name) {
                    return true;
                }
            }

            auto py_tag_fields = Py_OWN(PyObject_GetAttrString(reinterpret_cast<PyObject*>(memo_type), "__DBZERO_TAG_FIELDS_ATTR"));
            if (!py_tag_fields) {
                PyErr_Clear();
            } else if (pyTupleContainsString(*py_tag_fields, name)) {
                return true;
            }
            auto py_indexed_fields = Py_OWN(PyObject_GetAttrString(reinterpret_cast<PyObject*>(memo_type), "__DBZERO_INDEXED_FIELDS_ATTR"));
            if (!py_indexed_fields) {
                PyErr_Clear();
            } else if (pyTupleContainsString(*py_indexed_fields, name)) {
                return true;
            }
            return false;
        }

        PyTypeObject *findDeclarationOwner(PyTypeObject *memo_type, const char *name)
        {
            auto mro = memo_type->tp_mro;
            if (mro && PyTuple_Check(mro)) {
                auto size = PyTuple_Size(mro);
                for (Py_ssize_t index = size - 1; index >= 0; --index) {
                    auto item = PyTuple_GetItem(mro, index);
                    if (PyType_Check(item) && PyAnyMemoType_Check(reinterpret_cast<PyTypeObject*>(item))) {
                        auto candidate = reinterpret_cast<PyTypeObject*>(item);
                        if (hasDeclaredName(candidate, name)) {
                            return candidate;
                        }
                    }
                }
            }
            return hasDeclaredName(memo_type, name) ? memo_type : nullptr;
        }

        PyObject *tryFieldsOf(PyObject *py_type);

        PyObject *makeFieldRef(PyTypeObject *memo_type, PyTypeObject *owner_type, const char *field_name, bool declared)
        {
            auto ref = reinterpret_cast<PyFieldRef*>(PyFieldRefType.tp_alloc(&PyFieldRefType, 0));
            if (!ref) {
                return nullptr;
            }
            Py_INCREF(memo_type);
            Py_INCREF(owner_type);
            ref->memo_type = memo_type;
            ref->owner_type = owner_type;
            ref->field_name = PyUnicode_FromString(field_name);
            ref->declared = declared;
            if (!ref->field_name) {
                Py_DECREF(ref);
                return nullptr;
            }
            return reinterpret_cast<PyObject*>(ref);
        }

        PyObject *getFieldRef(PyFieldNamespace *ns, const char *field_name, bool explicit_dynamic)
        {
            PyTypeObject *owner_type = findDeclarationOwner(ns->memo_type, field_name);
            bool declared = owner_type != nullptr;
            if (!declared && !explicit_dynamic) {
                PyErr_Format(PyExc_AttributeError, "%s has no declared memo field %s", ns->memo_type->tp_name, field_name);
                return nullptr;
            }
            if (!declared) {
                if (!isPersistentAttrName(field_name)) {
                    THROWF(db0::InputException) << "Invalid persistent field name: " << field_name;
                }
                owner_type = ns->memo_type;
            } else if (owner_type != ns->memo_type) {
                auto owner_ns = Py_OWN(tryFieldsOf(reinterpret_cast<PyObject*>(owner_type)));
                if (!owner_ns) {
                    return nullptr;
                }
                return getFieldRef(reinterpret_cast<PyFieldNamespace*>(*owner_ns), field_name, false);
            }

            auto key = Py_OWN(PySafeTuple_Pack(
                Py_OWN(PyLong_FromVoidPtr(owner_type)),
                Py_OWN(PyUnicode_FromString(field_name)),
                Py_OWN(PyBool_FromLong(declared ? 1 : 0))));
            if (!key) {
                return nullptr;
            }
            auto existing = PyDict_GetItemWithError(ns->refs, *key);
            if (existing) {
                Py_INCREF(existing);
                return existing;
            }
            if (PyErr_Occurred()) {
                return nullptr;
            }

            auto ref = Py_OWN(makeFieldRef(ns->memo_type, owner_type, field_name, declared));
            if (!ref) {
                return nullptr;
            }
            if (PyDict_SetItem(ns->refs, *key, *ref) < 0) {
                return nullptr;
            }
            return ref.steal();
        }

        PyObject *createNamespace(PyTypeObject *memo_type)
        {
            auto ns = reinterpret_cast<PyFieldNamespace*>(PyFieldNamespaceType.tp_alloc(&PyFieldNamespaceType, 0));
            if (!ns) {
                return nullptr;
            }
            Py_INCREF(memo_type);
            ns->memo_type = memo_type;
            ns->refs = PyDict_New();
            if (!ns->refs) {
                Py_DECREF(ns);
                return nullptr;
            }
            return reinterpret_cast<PyObject*>(ns);
        }

        PyObject *tryFieldsOf(PyObject *py_type)
        {
            if (!PyType_Check(py_type) || !PyAnyMemoType_Check(reinterpret_cast<PyTypeObject*>(py_type))) {
                PyErr_SetString(PyExc_TypeError, "fields_of requires a dbzero memo type");
                return nullptr;
            }
            auto memo_type = reinterpret_cast<PyTypeObject*>(py_type);
            auto existing = PyObject_GetAttrString(py_type, FIELD_NAMESPACE_ATTR);
            if (existing) {
                return existing;
            }
            PyErr_Clear();

            auto ns = Py_OWN(createNamespace(memo_type));
            if (!ns) {
                return nullptr;
            }
            if (PyObject_SetAttrString(py_type, FIELD_NAMESPACE_ATTR, *ns) < 0) {
                return nullptr;
            }
            return ns.steal();
        }

        PyObject *FieldNamespace_getattro(PyFieldNamespace *self, PyObject *attr)
        {
            auto attr_name = PyUnicode_AsUTF8(attr);
            if (!attr_name) {
                return nullptr;
            }
            if (!isDunderName(attr_name)) {
                return runSafe(getFieldRef, self, attr_name, false);
            }
            return PyObject_GenericGetAttr(reinterpret_cast<PyObject*>(self), attr);
        }

        PyObject *FieldNamespace_subscript(PyFieldNamespace *self, PyObject *key)
        {
            if (!PyUnicode_Check(key)) {
                PyErr_SetString(PyExc_TypeError, "field name must be a string");
                return nullptr;
            }
            auto field_name = PyUnicode_AsUTF8(key);
            if (!field_name) {
                return nullptr;
            }
            return runSafe(getFieldRef, self, field_name, true);
        }

        PyObject *FieldNamespace_dir(PyFieldNamespace *self, PyObject *)
        {
            std::vector<std::string> names;
            auto mro = self->memo_type->tp_mro;
            if (mro && PyTuple_Check(mro)) {
                auto size = PyTuple_Size(mro);
                for (Py_ssize_t index = size - 1; index >= 0; --index) {
                    auto item = PyTuple_GetItem(mro, index);
                    if (PyType_Check(item) && PyAnyMemoType_Check(reinterpret_cast<PyTypeObject*>(item))) {
                        appendDecorationNames(reinterpret_cast<PyTypeObject*>(item), names);
                    }
                }
            } else {
                appendDecorationNames(self->memo_type, names);
            }
            std::sort(names.begin(), names.end());

            auto result = Py_OWN(PyList_New(names.size()));
            Py_ssize_t index = 0;
            for (const auto &name: names) {
                PySafeList_SetItem(*result, index++, Py_OWN(PyUnicode_FromString(name.c_str())));
            }
            return result.steal();
        }

        PyObject *FieldNamespace_reduce(PyObject *, PyObject *)
        {
            PyErr_SetString(PyExc_TypeError, "FieldNamespace objects cannot be pickled");
            return nullptr;
        }

        void FieldNamespace_dealloc(PyFieldNamespace *self)
        {
            PY_DEALLOC_GUARD();
            Py_XDECREF(self->memo_type);
            Py_XDECREF(self->refs);
            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
        }

        PyObject *FieldNamespace_new(PyTypeObject *, PyObject *, PyObject *)
        {
            PyErr_SetString(PyExc_TypeError, "FieldNamespace objects are created by dbzero.fields_of");
            return nullptr;
        }

        void FieldRef_dealloc(PyFieldRef *self)
        {
            PY_DEALLOC_GUARD();
            Py_XDECREF(self->memo_type);
            Py_XDECREF(self->owner_type);
            Py_XDECREF(self->field_name);
            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
        }

        PyObject *FieldRef_new(PyTypeObject *, PyObject *, PyObject *)
        {
            PyErr_SetString(PyExc_TypeError, "FieldRef objects are created by dbzero.fields_of");
            return nullptr;
        }

        Py_hash_t FieldRef_hash(PyFieldRef *self)
        {
            Py_hash_t name_hash = PyObject_Hash(self->field_name);
            if (name_hash == -1) {
                return -1;
            }
            return name_hash ^ reinterpret_cast<Py_hash_t>(self->owner_type) ^ (self->declared ? 0x9e3779b97f4a7c15ULL : 0);
        }

        PyObject *FieldRef_richcompare(PyFieldRef *lhs, PyObject *rhs, int op)
        {
            if (op != Py_EQ && op != Py_NE) {
                Py_RETURN_NOTIMPLEMENTED;
            }
            bool equal = false;
            if (PyFieldRef_Check(rhs)) {
                auto other = reinterpret_cast<PyFieldRef*>(rhs);
                equal = lhs->owner_type == other->owner_type
                    && lhs->declared == other->declared
                    && PyObject_RichCompareBool(lhs->field_name, other->field_name, Py_EQ) == 1;
            }
            if (op == Py_NE) {
                equal = !equal;
            }
            return PyBool_FromLong(equal ? 1 : 0);
        }

        PyObject *FieldRef_repr(PyFieldRef *self)
        {
            auto name = PyUnicode_AsUTF8(self->field_name);
            std::stringstream repr;
            repr << "<dbzero.FieldRef " << self->owner_type->tp_name << "." << (name ? name : "<invalid>") << ">";
            return PyUnicode_FromString(repr.str().c_str());
        }

        PyObject *FieldRef_reduce(PyObject *, PyObject *)
        {
            PyErr_SetString(PyExc_TypeError, "FieldRef objects cannot be pickled");
            return nullptr;
        }

        static PyMethodDef FieldNamespace_methods[] = {
            {"__dir__", reinterpret_cast<PyCFunction>(FieldNamespace_dir), METH_NOARGS, "Return declared memo field names"},
            {"__reduce__", reinterpret_cast<PyCFunction>(FieldNamespace_reduce), METH_NOARGS, ""},
            {NULL}
        };

        static PyMappingMethods FieldNamespace_mapping = {
            .mp_length = 0,
            .mp_subscript = reinterpret_cast<binaryfunc>(FieldNamespace_subscript),
            .mp_ass_subscript = 0,
        };

        static PyMethodDef FieldRef_methods[] = {
            {"__reduce__", reinterpret_cast<PyCFunction>(FieldRef_reduce), METH_NOARGS, ""},
            {NULL}
        };
    }

    PyTypeObject PyFieldNamespaceType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.FieldNamespace",
        .tp_basicsize = sizeof(PyFieldNamespace),
        .tp_itemsize = 0,
        .tp_dealloc = reinterpret_cast<destructor>(FieldNamespace_dealloc),
        .tp_as_mapping = &FieldNamespace_mapping,
        .tp_getattro = reinterpret_cast<getattrofunc>(FieldNamespace_getattro),
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Memo field namespace",
        .tp_methods = FieldNamespace_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = FieldNamespace_new,
        .tp_free = PyObject_Free,
    };

    PyTypeObject PyFieldRefType = {
        PYVAROBJECT_HEAD_INIT_DESIGNATED,
        .tp_name = "dbzero.FieldRef",
        .tp_basicsize = sizeof(PyFieldRef),
        .tp_itemsize = 0,
        .tp_dealloc = reinterpret_cast<destructor>(FieldRef_dealloc),
        .tp_repr = reinterpret_cast<reprfunc>(FieldRef_repr),
        .tp_hash = reinterpret_cast<hashfunc>(FieldRef_hash),
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Memo field reference",
        .tp_richcompare = reinterpret_cast<richcmpfunc>(FieldRef_richcompare),
        .tp_methods = FieldRef_methods,
        .tp_alloc = PyType_GenericAlloc,
        .tp_new = FieldRef_new,
        .tp_free = PyObject_Free,
    };

    bool PyFieldRef_Check(PyObject *py_object)
    {
        return Py_TYPE(py_object) == &PyFieldRefType;
    }

    PyTypeObject *PyFieldRef_getMemoType(PyObject *py_object)
    {
        return reinterpret_cast<PyFieldRef*>(py_object)->memo_type;
    }

    const char *PyFieldRef_getFieldName(PyObject *py_object)
    {
        return PyUnicode_AsUTF8(reinterpret_cast<PyFieldRef*>(py_object)->field_name);
    }

    PyObject *PyAPI_fieldsOf(PyObject *, PyObject *const *args, Py_ssize_t nargs)
    {
        PY_API_FUNC
        if (nargs != 1) {
            PyErr_SetString(PyExc_TypeError, "fields_of requires exactly one argument");
            return nullptr;
        }
        return runSafe(tryFieldsOf, args[0]);
    }

}
