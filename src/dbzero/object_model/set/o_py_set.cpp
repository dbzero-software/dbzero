// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "o_py_set.hpp"

#include <Python.h>

#include <vector>

#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/object_model/dict/o_py_dict.hpp>
#include <dbzero/object_model/object/ObjectImmutableImpl.hpp>
#include <dbzero/object_model/object/o_embedded_object.hpp>
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

    o_py_set::o_py_set(PyObject *iterable)
        : o_set()
    {
        std::uint32_t count = 0;
        std::uint32_t elementsByteSize = 0;
        std::size_t capacity = 0;
        std::uint32_t bucketByteSize = 0;
        count = setSize(iterable);
        elementsByteSize = checkedUint32Size(measureElements(iterable), "Python set elements byte size");
        capacity = hashIndexCapacity(count);
        bucketByteSize = checkedUint32Size(
            measureCollisionBuckets(iterable, capacity), "Python set bucket byte size"
        );

        auto arranger = arrangeSetMembers(count, elementsByteSize, bucketByteSize);
        auto iterator = Py_OWN(PyObject_GetIter(iterable));
        if (!iterator) {
            PyErr_Clear();
            THROWF(db0::InputException) << "o_py_set expects a Python set";
        }

        Py_FOR(item, iterator) {
            arranger = arranger(Item::type(), elementFromPythonObject(*item));
        }
        if (PyErr_Occurred()) {
            PyErr_Clear();
            THROWF(db0::InputException) << "Unable to iterate Python set";
        }

        finishSetConstruction(arranger.ptr(), elementsByteSize, capacity, bucketByteSize);
    }

    o_py_set::o_py_set(PyObject *iterable, EmbeddedObjectOffsetCollector &offsetCollector)
        : o_set()
    {
        auto count = setSize(iterable);
        auto elementsByteSize = checkedUint32Size(measureElements(iterable), "Python set elements byte size");
        auto capacity = hashIndexCapacity(count);
        auto bucketByteSize = checkedUint32Size(
            measureCollisionBuckets(iterable, capacity), "Python set bucket byte size"
        );

        auto arranger = arrangeSetMembers(count, elementsByteSize, bucketByteSize);
        auto iterator = Py_OWN(PyObject_GetIter(iterable));
        if (!iterator) {
            PyErr_Clear();
            THROWF(db0::InputException) << "o_py_set expects a Python set";
        }

        Py_FOR(item, iterator) {
            arranger = arranger(Item::type(), elementFromPythonObject(*item, &offsetCollector));
        }
        if (PyErr_Occurred()) {
            PyErr_Clear();
            THROWF(db0::InputException) << "Unable to iterate Python set";
        }

        finishSetConstruction(arranger.ptr(), elementsByteSize, capacity, bucketByteSize);
    }

    std::size_t o_py_set::measure(PyObject *iterable)
    {
        auto count = setSize(iterable);
        auto elementsByteSize = measureElements(iterable);
        auto bucketByteSize = measureCollisionBuckets(iterable, hashIndexCapacity(count));
        return measureMembers()
            (db0::packed_int32::type(), count)
            (db0::packed_int32::type(), checkedUint32Size(elementsByteSize, "Python set elements byte size"))
            (db0::packed_int32::type(), checkedUint32Size(bucketByteSize, "Python set bucket byte size"))
            (elementsByteSize)
            (hashIndexCapacity(count) * sizeof(std::uint32_t))
            (bucketByteSize);
    }

    o_py_set &o_py_set::__ref(void *buf)
    {
        return *reinterpret_cast<o_py_set *>(buf);
    }

    const o_py_set &o_py_set::__const_ref(const void *buf)
    {
        return *reinterpret_cast<const o_py_set *>(buf);
    }

    db0::Foundation::Type<o_py_set> o_py_set::type()
    {
        return db0::Foundation::Type<o_py_set>();
    }

    o_py_set::Element o_py_set::elementFromPythonObject(PyObject *object)
    {
        return elementFromPythonObject(object, nullptr);
    }

    o_py_set::Element o_py_set::elementFromPythonObject(
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

        THROWF(db0::InputException) << "Unsupported o_py_set element type: " << Py_TYPE(object)->tp_name;
        return Element::none();
    }

    std::uint32_t o_py_set::setSize(PyObject *set)
    {
        if (!PySet_Check(set)) {
            THROWF(db0::InputException) << "o_py_set expects a Python set";
        }
        auto size = PySet_GET_SIZE(set);
        if (size < 0) {
            PyErr_Clear();
            THROWF(db0::InputException) << "Unable to read Python set size";
        }
        return checkedUint32Size(static_cast<std::size_t>(size), "Python set size");
    }

    std::size_t o_py_set::measureElements(PyObject *set)
    {
        setSize(set);
        std::size_t size = 0;
        auto iterator = Py_OWN(PyObject_GetIter(set));
        if (!iterator) {
            PyErr_Clear();
            THROWF(db0::InputException) << "o_py_set expects a Python set";
        }

        Py_FOR(item, iterator) {
            auto itemSize = Item::measure(elementFromPythonObject(*item));
            if (size + itemSize < size) {
                THROWF(db0::InternalException) << "Python set elements byte size overflow";
            }
            size += itemSize;
        }
        if (PyErr_Occurred()) {
            PyErr_Clear();
            THROWF(db0::InputException) << "Unable to iterate Python set";
        }

        checkedHashIndexOffset(size == 0 ? 0 : size - 1, "Python set item block");
        return size;
    }

    std::size_t o_py_set::measureCollisionBuckets(PyObject *set, std::size_t capacity)
    {
        setSize(set);
        if (capacity == 0) {
            return 0;
        }

        struct BucketMeasure
        {
            std::uint32_t m_count = 0;
            std::uint32_t m_elementsByteSize = 0;
        };

        std::vector<BucketMeasure> buckets(capacity);
        std::size_t size = 0;
        auto iterator = Py_OWN(PyObject_GetIter(set));
        if (!iterator) {
            PyErr_Clear();
            THROWF(db0::InputException) << "o_py_set expects a Python set";
        }

        Py_FOR(item, iterator) {
            auto element = elementFromPythonObject(*item);
            auto &bucket = buckets[elementHash(element) % capacity];
            auto itemSize = checkedUint32Size(Item::measure(element), "Python set bucket item byte size");
            auto growth = o_compact_tuple::Builder::measureGrowth(bucket.m_count, bucket.m_elementsByteSize, itemSize);
            if (size + growth < size) {
                THROWF(db0::InternalException) << "Python set bucket block byte size overflow";
            }
            size += growth;
            ++bucket.m_count;
            if (bucket.m_elementsByteSize + itemSize < bucket.m_elementsByteSize) {
                THROWF(db0::InternalException) << "Python set bucket elements byte size overflow";
            }
            bucket.m_elementsByteSize += itemSize;
        }
        if (PyErr_Occurred()) {
            PyErr_Clear();
            THROWF(db0::InputException) << "Unable to iterate Python set";
        }

        checkedHashIndexOffset(size == 0 ? 0 : size - 1, "Python set bucket block");
        return size;
    }

}
