// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "o_py_tuple.hpp"

#include <Python.h>

#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::object_model
{

    o_py_tuple::o_py_tuple(PyObject *sequence)
        : o_tuple()
    {
        auto count = static_cast<std::uint32_t>(sequenceSize(sequence));
        auto elementsByteSize = static_cast<std::uint32_t>(measureElements(sequence));

        auto arranger = arrangeMembers();
        arranger = arranger(db0::packed_int32::type(), count);
        arranger = arranger(db0::packed_int32::type(), elementsByteSize);
        for (std::size_t i = 0; i < count; ++i) {
            arranger = arranger(o_tuple_item::type(), elementFromPythonObject(sequenceItem(sequence, i)));
        }
    }

    std::size_t o_py_tuple::measure(PyObject *sequence)
    {
        auto count = static_cast<std::uint32_t>(sequenceSize(sequence));
        auto elementsByteSize = measureElements(sequence);
        return measureMembers()
            (db0::packed_int32::type(), count)
            (db0::packed_int32::type(), static_cast<std::uint32_t>(elementsByteSize))
            (elementsByteSize);
    }

    o_py_tuple &o_py_tuple::__ref(void *buf)
    {
        return *reinterpret_cast<o_py_tuple *>(buf);
    }

    const o_py_tuple &o_py_tuple::__const_ref(const void *buf)
    {
        return *reinterpret_cast<const o_py_tuple *>(buf);
    }

    db0::Foundation::Type<o_py_tuple> o_py_tuple::type()
    {
        return db0::Foundation::Type<o_py_tuple>();
    }

    o_py_tuple::Element o_py_tuple::elementFromPythonObject(PyObject *object)
    {
        if (object == Py_None) {
            return Element::none();
        }
        if (PyBool_Check(object)) {
            return Element::boolean(object == Py_True);
        }
        if (PyLong_Check(object)) {
            auto value = PyLong_AsLongLong(object);
            if (PyErr_Occurred()) {
                PyErr_Clear();
                THROWF(db0::InputException) << "Python integer is out of int64 range";
            }
            return Element::integer(value);
        }
        if (PyFloat_Check(object)) {
            return Element::floating(PyFloat_AsDouble(object));
        }
        if (PyUnicode_Check(object)) {
            const char *value = PyUnicode_AsUTF8(object);
            if (!value) {
                PyErr_Clear();
                THROWF(db0::InputException) << "Unable to encode Python string as UTF-8";
            }
            return Element::string(value);
        }
        if (PyBytes_Check(object)) {
            char *data = nullptr;
            Py_ssize_t size = 0;
            if (PyBytes_AsStringAndSize(object, &data, &size) != 0) {
                PyErr_Clear();
                THROWF(db0::InputException) << "Unable to read Python bytes";
            }
            return Element::bytes(reinterpret_cast<const std::byte *>(data), static_cast<std::size_t>(size));
        }

        THROWF(db0::InputException) << "Unsupported o_py_tuple element type: " << Py_TYPE(object)->tp_name;
        return Element::none();
    }

    std::size_t o_py_tuple::sequenceSize(PyObject *sequence)
    {
        if (PyTuple_Check(sequence)) {
            return static_cast<std::size_t>(PyTuple_GET_SIZE(sequence));
        }
        if (PyList_Check(sequence)) {
            return static_cast<std::size_t>(PyList_GET_SIZE(sequence));
        }
        THROWF(db0::InputException) << "o_py_tuple expects a Python tuple or list";
        return 0;
    }

    PyObject *o_py_tuple::sequenceItem(PyObject *sequence, std::size_t index)
    {
        if (PyTuple_Check(sequence)) {
            return PyTuple_GET_ITEM(sequence, static_cast<Py_ssize_t>(index));
        }
        return PyList_GET_ITEM(sequence, static_cast<Py_ssize_t>(index));
    }

    std::size_t o_py_tuple::measureElements(PyObject *sequence)
    {
        auto count = sequenceSize(sequence);
        std::size_t size = 0;
        for (std::size_t i = 0; i < count; ++i) {
            size += o_tuple_item::measure(elementFromPythonObject(sequenceItem(sequence, i)));
        }
        return size;
    }

}
