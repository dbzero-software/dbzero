// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "o_py_tuple.hpp"

#include <Python.h>

#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/dict/o_py_dict.hpp>
#include <dbzero/object_model/set/o_py_set.hpp>

namespace db0::object_model
{
    namespace
    {
        void writePyTuple(void *buf, const void *source)
        {
            o_py_tuple::__new(buf, const_cast<PyObject *>(static_cast<const PyObject *>(source)));
        }

        void writePySet(void *buf, const void *source)
        {
            o_py_set::__new(buf, const_cast<PyObject *>(static_cast<const PyObject *>(source)));
        }

        void writePyDict(void *buf, const void *source)
        {
            o_py_dict::__new(buf, const_cast<PyObject *>(static_cast<const PyObject *>(source)));
        }
    }

    o_py_tuple::o_py_tuple(PyObject *sequence)
        : o_tuple<>()
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
        auto &typeManager = db0::python::PyToolkit::getTypeManager();
        auto typeId = typeManager.getTypeId(object);

        switch (typeId) {
        case db0::bindings::TypeId::NONE:
            return Element::none();
        case db0::bindings::TypeId::BOOLEAN:
            return Element::boolean(object == Py_True);
        case db0::bindings::TypeId::INTEGER: {
            auto value = PyLong_AsLongLong(object);
            if (PyErr_Occurred()) {
                PyErr_Clear();
                THROWF(db0::InputException) << "Python integer is out of int64 range";
            }
            return Element::integer(value);
        }
        case db0::bindings::TypeId::FLOAT:
            return Element::floating(PyFloat_AsDouble(object));
        case db0::bindings::TypeId::DATETIME:
            return Element::datetime(typeManager.extractUInt64(typeId, object));
        case db0::bindings::TypeId::DATETIME_TZ:
            return Element::datetimeTz(typeManager.extractUInt64(typeId, object));
        case db0::bindings::TypeId::DATE:
            return Element::date(typeManager.extractUInt64(typeId, object));
        case db0::bindings::TypeId::TIME:
            return Element::time(typeManager.extractUInt64(typeId, object));
        case db0::bindings::TypeId::TIME_TZ:
            return Element::timeTz(typeManager.extractUInt64(typeId, object));
        case db0::bindings::TypeId::DECIMAL:
            return Element::decimal(typeManager.extractUInt64(typeId, object));
        case db0::bindings::TypeId::STRING: {
            return Element::string(typeManager.extractString(object));
        }
        case db0::bindings::TypeId::BYTES: {
            auto bytes = typeManager.extractBytes(object);
            return Element::bytes(bytes.m_data, bytes.m_size);
        }
        case db0::bindings::TypeId::LIST:
        case db0::bindings::TypeId::TUPLE:
            return Element::embeddedTuple(o_py_tuple::measure(object), writePyTuple, object);
        case db0::bindings::TypeId::SET:
            return Element::embeddedSet(o_py_set::measure(object), writePySet, object);
        case db0::bindings::TypeId::DICT:
            return Element::embeddedDict(o_py_dict::measure(object), writePyDict, object);
        default:
            break;
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
