// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "o_py_dict.hpp"

#include <Python.h>

#include <vector>

#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
#include <dbzero/object_model/set/o_py_set.hpp>
#include <dbzero/object_model/tuple/o_py_tuple.hpp>

namespace db0::object_model
{
    namespace
    {
        void writePyTuple(void *buf, const void *source, EmbeddedObjectOffsetCollector *context)
        {
            auto *pyObject = const_cast<PyObject *>(static_cast<const PyObject *>(source));
            if (context) {
                o_py_tuple::__new(buf, pyObject, *context);
            } else {
                o_py_tuple::__new(buf, pyObject);
            }
        }

        void writePySet(void *buf, const void *source, EmbeddedObjectOffsetCollector *context)
        {
            auto *pyObject = const_cast<PyObject *>(static_cast<const PyObject *>(source));
            if (context) {
                o_py_set::__new(buf, pyObject, *context);
            } else {
                o_py_set::__new(buf, pyObject);
            }
        }

        void writePyDict(void *buf, const void *source, EmbeddedObjectOffsetCollector *context)
        {
            auto *pyObject = const_cast<PyObject *>(static_cast<const PyObject *>(source));
            if (context) {
                o_py_dict::__new(buf, pyObject, *context);
            } else {
                o_py_dict::__new(buf, pyObject);
            }
        }

        const ImmutableObjectInitializer &getInitializer(PyObject *pyObject)
        {
            using MemoImmutableObject = db0::python::PyToolkit::TypeManager::MemoImmutableObject;

            assert(db0::python::PyToolkit::isMemoImmutableObject(pyObject));

            const auto &object = db0::python::PyToolkit::getTypeManager()
                .template extractObject<MemoImmutableObject>(pyObject);
            if (object.hasInstance()) {
                THROWF(db0::InputException)
                    << "Only non-materialized immutable memo objects can be embedded";
            }

            auto *initializer = dynamic_cast<ImmutableObjectInitializer *>(
                InitManager::instance.findInitializer(object)
            );
            if (!initializer) {
                THROWF(db0::InputException)
                    << "Non-materialized immutable memo object has no active initializer";
            }
            return *initializer;
        }

        void writeEmbeddedObject(void *buf, const void *source, EmbeddedObjectOffsetCollector *context)
        {
            auto *pyObject = const_cast<PyObject *>(static_cast<const PyObject *>(source));
            const auto &initializer = getInitializer(pyObject);
            if (context) {
                context->add(buf);
                o_embedded_object::__new(buf, initializer.getClassPtr()->getClassRef(), initializer, *context);
            } else {
                o_embedded_object::__new(buf, initializer.getClassPtr()->getClassRef(), initializer);
            }
        }
    }

    o_py_dict::o_py_dict(PyObject *dict)
        : o_dict()
    {
        std::uint32_t count = 0;
        std::uint32_t pairsByteSize = 0;
        std::size_t capacity = 0;
        std::uint32_t bucketByteSize = 0;
        count = dictSize(dict);
        pairsByteSize = checkedUint32Size(measurePairs(dict), "Python dict pairs byte size");
        capacity = hashIndexCapacity(count);
        bucketByteSize = checkedUint32Size(
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

    o_py_dict::o_py_dict(PyObject *dict, EmbeddedObjectOffsetCollector &offsetCollector)
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
            arranger = arranger(
                Pair::type(),
                elementFromPythonObject(*key, &offsetCollector),
                valueFromPythonDict(dict, *key, &offsetCollector)
            );
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
        return elementFromPythonObject(object, nullptr);
    }

    o_py_dict::Element o_py_dict::elementFromPythonObject(
        PyObject *object, EmbeddedObjectOffsetCollector *offsetCollector
    )
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
            return Element::embeddedTuple(o_py_tuple::measure(object), writePyTuple, object, offsetCollector);
        case db0::bindings::TypeId::SET:
            return Element::embeddedSet(o_py_set::measure(object), writePySet, object, offsetCollector);
        case db0::bindings::TypeId::DICT:
            return Element::embeddedDict(o_py_dict::measure(object), writePyDict, object, offsetCollector);
        case db0::bindings::TypeId::MEMO_IMMUTABLE_OBJECT: {
            const auto &initializer = getInitializer(object);
            auto size = o_embedded_object::measure(initializer.getClassPtr()->getClassRef(), initializer);
            return Element::embeddedObject(size, writeEmbeddedObject, object, offsetCollector);
        }
        default:
            break;
        }

        THROWF(db0::InputException) << "Unsupported o_py_dict item type: " << Py_TYPE(object)->tp_name;
        return Element::none();
    }

    o_py_dict::Element o_py_dict::valueFromPythonDict(PyObject *dict, PyObject *key)
    {
        return valueFromPythonDict(dict, key, nullptr);
    }

    o_py_dict::Element o_py_dict::valueFromPythonDict(
        PyObject *dict, PyObject *key, EmbeddedObjectOffsetCollector *offsetCollector
    )
    {
        auto *value = PyDict_GetItemWithError(dict, key);
        if (!value) {
            if (PyErr_Occurred()) {
                PyErr_Clear();
            }
            THROWF(db0::InputException) << "Unable to read Python dict value";
        }
        return elementFromPythonObject(value, offsetCollector);
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
