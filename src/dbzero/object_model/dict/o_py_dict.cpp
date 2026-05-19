// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "o_py_dict.hpp"

#include <Python.h>

#include <vector>

#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::object_model
{

    o_py_dict::o_py_dict(PyObject *dict)
        : o_dict()
    {
        auto count = dictSize(dict);
        auto pairsByteSize = checkedUint32Size(measurePairs(dict), "Python dict pairs byte size");
        auto capacity = hashIndexCapacity(count);
        auto bucketByteSize = checkedUint32Size(
            measureCollisionBuckets(dict, capacity), "Python dict bucket byte size"
        );

        auto arranger = arrangeDictMembers(count, pairsByteSize, bucketByteSize);
        auto iterator = Py_OWN(PyObject_GetIter(dict));
        if (!iterator) {
            PyErr_Clear();
            THROWF(db0::InputException) << "o_py_dict expects a Python dict";
        }

        Py_FOR(key, iterator) {
            arranger = arranger(Pair::type(), elementFromPythonObject(*key), valueFromPythonDict(dict, *key));
        }
        if (PyErr_Occurred()) {
            PyErr_Clear();
            THROWF(db0::InputException) << "Unable to iterate Python dict";
        }

        finishDictConstruction(arranger.ptr(), pairsByteSize, capacity, bucketByteSize);
    }

    std::size_t o_py_dict::measure(PyObject *dict)
    {
        auto count = dictSize(dict);
        auto pairsByteSize = measurePairs(dict);
        auto bucketByteSize = measureCollisionBuckets(dict, hashIndexCapacity(count));
        return measureMembers()
            (db0::packed_int32::type(), count)
            (db0::packed_int32::type(), checkedUint32Size(pairsByteSize, "Python dict pairs byte size"))
            (db0::packed_int32::type(), checkedUint32Size(bucketByteSize, "Python dict bucket byte size"))
            (pairsByteSize)
            (hashIndexCapacity(count) * sizeof(std::uint32_t))
            (bucketByteSize);
    }

    o_py_dict &o_py_dict::__ref(void *buf)
    {
        return *reinterpret_cast<o_py_dict *>(buf);
    }

    const o_py_dict &o_py_dict::__const_ref(const void *buf)
    {
        return *reinterpret_cast<const o_py_dict *>(buf);
    }

    db0::Foundation::Type<o_py_dict> o_py_dict::type()
    {
        return db0::Foundation::Type<o_py_dict>();
    }

    o_py_dict::Element o_py_dict::elementFromPythonObject(PyObject *object)
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
            const char *value = PyUnicode_AsUTF8(object);
            if (!value) {
                PyErr_Clear();
                THROWF(db0::InputException) << "Unable to encode Python string as UTF-8";
            }
            return Element::string(value);
        }
        case db0::bindings::TypeId::BYTES: {
            char *data = nullptr;
            Py_ssize_t size = 0;
            if (PyBytes_AsStringAndSize(object, &data, &size) != 0) {
                PyErr_Clear();
                THROWF(db0::InputException) << "Unable to read Python bytes";
            }
            return Element::bytes(reinterpret_cast<const std::byte *>(data), static_cast<std::size_t>(size));
        }
        default:
            break;
        }

        THROWF(db0::InputException) << "Unsupported o_py_dict item type: " << Py_TYPE(object)->tp_name;
        return Element::none();
    }

    o_py_dict::Element o_py_dict::valueFromPythonDict(PyObject *dict, PyObject *key)
    {
        auto *value = PyDict_GetItemWithError(dict, key);
        if (!value) {
            if (PyErr_Occurred()) {
                PyErr_Clear();
            }
            THROWF(db0::InputException) << "Unable to read Python dict value";
        }
        return elementFromPythonObject(value);
    }

    std::uint32_t o_py_dict::dictSize(PyObject *dict)
    {
        if (!PyDict_Check(dict)) {
            THROWF(db0::InputException) << "o_py_dict expects a Python dict";
        }
        auto size = PyDict_Size(dict);
        if (size < 0) {
            PyErr_Clear();
            THROWF(db0::InputException) << "Unable to read Python dict size";
        }
        return checkedUint32Size(static_cast<std::size_t>(size), "Python dict size");
    }

    std::size_t o_py_dict::measurePairs(PyObject *dict)
    {
        dictSize(dict);
        std::size_t size = 0;
        auto iterator = Py_OWN(PyObject_GetIter(dict));
        if (!iterator) {
            PyErr_Clear();
            THROWF(db0::InputException) << "o_py_dict expects a Python dict";
        }

        Py_FOR(key, iterator) {
            auto pairSize = Pair::measure(elementFromPythonObject(*key), valueFromPythonDict(dict, *key));
            if (size + pairSize < size) {
                THROWF(db0::InternalException) << "Python dict pairs byte size overflow";
            }
            size += pairSize;
        }
        if (PyErr_Occurred()) {
            PyErr_Clear();
            THROWF(db0::InputException) << "Unable to iterate Python dict";
        }

        checkedHashIndexOffset(size == 0 ? 0 : size - 1, "Python dict pair block");
        return size;
    }

    std::size_t o_py_dict::measureCollisionBuckets(PyObject *dict, std::size_t capacity)
    {
        dictSize(dict);
        if (capacity == 0) {
            return 0;
        }

        struct BucketMeasure
        {
            std::uint32_t m_count = 0;
            std::uint32_t m_keysByteSize = 0;
            std::uint32_t m_valuesByteSize = 0;
        };

        std::vector<BucketMeasure> buckets(capacity);
        std::size_t size = 0;
        auto iterator = Py_OWN(PyObject_GetIter(dict));
        if (!iterator) {
            PyErr_Clear();
            THROWF(db0::InputException) << "o_py_dict expects a Python dict";
        }

        Py_FOR(key, iterator) {
            auto keyElement = elementFromPythonObject(*key);
            auto valueElement = valueFromPythonDict(dict, *key);
            auto &bucket = buckets[elementHash(keyElement) % capacity];
            auto keySize = checkedUint32Size(Item::measure(keyElement), "Python dict bucket key byte size");
            auto valueSize = checkedUint32Size(Item::measure(valueElement), "Python dict bucket value byte size");
            auto growth = o_dict_bucket::measureGrowth(
                bucket.m_count, bucket.m_keysByteSize, bucket.m_valuesByteSize, keySize, valueSize
            );
            if (size + growth < size) {
                THROWF(db0::InternalException) << "Python dict bucket block byte size overflow";
            }
            size += growth;
            ++bucket.m_count;
            if (bucket.m_keysByteSize + keySize < bucket.m_keysByteSize
                || bucket.m_valuesByteSize + valueSize < bucket.m_valuesByteSize) {
                THROWF(db0::InternalException) << "Python dict bucket elements byte size overflow";
            }
            bucket.m_keysByteSize += keySize;
            bucket.m_valuesByteSize += valueSize;
        }
        if (PyErr_Occurred()) {
            PyErr_Clear();
            THROWF(db0::InputException) << "Unable to iterate Python dict";
        }

        checkedHashIndexOffset(size == 0 ? 0 : size - 1, "Python dict bucket block");
        return size;
    }

}
