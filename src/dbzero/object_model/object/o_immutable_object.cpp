// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "o_immutable_object.hpp"
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/serialization/string.hpp>
#include <dbzero/object_model/class.hpp>
#include <dbzero/object_model/dict/o_py_dict.hpp>
#include <dbzero/object_model/set/o_py_set.hpp>
#include <dbzero/object_model/tuple/o_py_tuple.hpp>
#include <dbzero/object_model/value.hpp>

#include <cassert>
#include <limits>
#include <memory>

namespace db0::object_model

{
    namespace
    {
        struct MeasureScratch
        {
            o_tuple_item::Element embeddedTuple(std::size_t size)
            {
                return makeEmbedded(size, StorageClass::EMBEDDED_TUPLE);
            }

            o_tuple_item::Element embeddedSet(std::size_t size)
            {
                return makeEmbedded(size, StorageClass::EMBEDDED_SET);
            }

            o_tuple_item::Element embeddedDict(std::size_t size)
            {
                return makeEmbedded(size, StorageClass::EMBEDDED_DICT);
            }

            o_tuple_item::Element embeddedObject(std::size_t size)
            {
                return makeEmbedded(size, StorageClass::EMBEDDED_OBJECT);
            }

        private:
            o_tuple_item::Element makeEmbedded(std::size_t size, StorageClass kind)
            {
                auto buffer = std::make_unique<std::byte[]>(size == 0 ? 1 : size);
                if (size > 0) {
                    auto salt = m_nextSalt++;
                    for (std::size_t i = 0; i < sizeof(salt) && i < size; ++i) {
                        buffer[i] = static_cast<std::byte>((salt >> (i * 8)) & 0xffU);
                    }
                }
                auto *data = buffer.get();
                m_buffers.push_back(std::move(buffer));

                switch (kind) {
                    case StorageClass::EMBEDDED_TUPLE:
                        return o_tuple_item::Element::embeddedTuple(data, size);
                    case StorageClass::EMBEDDED_SET:
                        return o_tuple_item::Element::embeddedSet(data, size);
                    case StorageClass::EMBEDDED_DICT:
                        return o_tuple_item::Element::embeddedDict(data, size);
                    case StorageClass::EMBEDDED_OBJECT:
                        return o_tuple_item::Element::embeddedObject(data, size);
                    default:
                        THROWF(db0::InternalException) << "Unsupported embedded measure kind";
                }
                return o_tuple_item::Element::none();
            }

            std::vector<std::unique_ptr<std::byte[]>> m_buffers;
            std::uint64_t m_nextSalt = 1;
        };

        struct EmbeddedObjectMeter: o_embedded_object
        {
            using o_embedded_object::measureMembers;
        };

        std::size_t countEmbeddedMemoObjects(PyObject *object);

        const ImmutableObjectInitializer *tryGetImmutableInitializer(PyObject *object)
        {
            using MemoImmutableObject = db0::python::PyToolkit::TypeManager::MemoImmutableObject;
            if (!db0::python::PyToolkit::isMemoImmutableObject(object)) {
                return nullptr;
            }
            const auto &memo = db0::python::PyToolkit::getTypeManager()
                .template extractObject<MemoImmutableObject>(object);
            return dynamic_cast<ImmutableObjectInitializer *>(InitManager::instance.findInitializer(memo));
        }

        std::size_t countEmbeddedMemoObjectsInInitializer(const ImmutableObjectInitializer &initializer)
        {
            std::size_t result = 0;
            for (const auto &value: initializer.objects()) {
                if (!value.m_object || value.m_storage_class == StorageClass::DELETED) {
                    continue;
                }
                result += countEmbeddedMemoObjects(value.m_object.get());
            }
            return result;
        }

        std::size_t countEmbeddedMemoObjects(PyObject *object)
        {
            if (!object) {
                return 0;
            }
            if (auto *initializer = tryGetImmutableInitializer(object)) {
                return 1 + countEmbeddedMemoObjectsInInitializer(*initializer);
            }
            if (PyTuple_Check(object)) {
                std::size_t result = 0;
                auto size = PyTuple_GET_SIZE(object);
                for (Py_ssize_t i = 0; i < size; ++i) {
                    result += countEmbeddedMemoObjects(PyTuple_GET_ITEM(object, i));
                }
                return result;
            }
            if (PyList_Check(object)) {
                std::size_t result = 0;
                auto size = PyList_GET_SIZE(object);
                for (Py_ssize_t i = 0; i < size; ++i) {
                    result += countEmbeddedMemoObjects(PyList_GET_ITEM(object, i));
                }
                return result;
            }
            if (PySet_Check(object)) {
                std::size_t result = 0;
                auto iterator = Py_OWN(PyObject_GetIter(object));
                if (!iterator.get()) {
                    PyErr_Clear();
                    return 0;
                }
                Py_FOR(item, iterator) {
                    result += countEmbeddedMemoObjects(*item);
                }
                PyErr_Clear();
                return result;
            }
            if (PyDict_Check(object)) {
                std::size_t result = 0;
                PyObject *key = nullptr;
                PyObject *value = nullptr;
                Py_ssize_t pos = 0;
                while (PyDict_Next(object, &pos, &key, &value)) {
                    result += countEmbeddedMemoObjects(key);
                    result += countEmbeddedMemoObjects(value);
                }
                return result;
            }
            return 0;
        }

        std::vector<std::uint64_t> worstCaseOffsetIndexValues(std::size_t count)
        {
            std::vector<std::uint64_t> result;
            result.reserve(count);
            constexpr std::uint64_t BASE = std::numeric_limits<std::uint64_t>::max() / 2;
            for (std::size_t i = 0; i < count; ++i) {
                result.push_back(BASE + i);
            }
            return result;
        }

        std::size_t measureEmbeddedObjectNoWriters(
            std::uint32_t classRef, const ImmutableObjectInitializer &initializer, MeasureScratch &scratch
        );

        o_tuple_item::Element elementFromPythonObjectNoWriters(PyObject *object, MeasureScratch &scratch);

        std::size_t measurePyTupleNoWriters(PyObject *sequence, MeasureScratch &scratch)
        {
            std::size_t count = 0;
            if (PyTuple_Check(sequence)) {
                count = static_cast<std::size_t>(PyTuple_GET_SIZE(sequence));
            } else if (PyList_Check(sequence)) {
                count = static_cast<std::size_t>(PyList_GET_SIZE(sequence));
            } else {
                THROWF(db0::InputException) << "o_py_tuple expects a Python tuple or list";
            }

            std::size_t elementsByteSize = 0;
            for (std::size_t i = 0; i < count; ++i) {
                auto *item = PyTuple_Check(sequence)
                    ? PyTuple_GET_ITEM(sequence, static_cast<Py_ssize_t>(i))
                    : PyList_GET_ITEM(sequence, static_cast<Py_ssize_t>(i));
                elementsByteSize += o_tuple_item::measure(elementFromPythonObjectNoWriters(item, scratch));
            }
            return o_tuple<>::Builder::measure(
                static_cast<std::uint32_t>(count), static_cast<std::uint32_t>(elementsByteSize)
            );
        }

        std::size_t measurePySetNoWriters(PyObject *setObject, MeasureScratch &scratch)
        {
            o_set::ElementSet elements;
            auto iterator = Py_OWN(PyObject_GetIter(setObject));
            if (!iterator.get()) {
                THROWF(db0::InputException) << "o_py_set expects an iterable";
            }
            Py_FOR(item, iterator) {
                elements.insert(elementFromPythonObjectNoWriters(*item, scratch));
            }
            if (PyErr_Occurred()) {
                THROWF(db0::InputException) << "o_py_set iteration failed";
            }
            return o_set::measure(elements);
        }

        std::size_t measurePyDictNoWriters(PyObject *dictObject, MeasureScratch &scratch)
        {
            if (!PyDict_Check(dictObject)) {
                THROWF(db0::InputException) << "o_py_dict expects a Python dict";
            }

            o_dict::ElementMap elements;
            PyObject *key = nullptr;
            PyObject *value = nullptr;
            Py_ssize_t pos = 0;
            while (PyDict_Next(dictObject, &pos, &key, &value)) {
                elements[
                    elementFromPythonObjectNoWriters(key, scratch)
                ] = elementFromPythonObjectNoWriters(value, scratch);
            }
            return o_dict::measure(elements);
        }

        o_tuple_item::Element objectValueElementNoWriters(
            StorageClass storageClass, PyObject *object, MeasureScratch &scratch
        )
        {
            auto &typeManager = db0::python::PyToolkit::getTypeManager();
            switch (storageClass) {
                case StorageClass::STRING_REF:
                case StorageClass::POOLED_STRING:
                case StorageClass::STR64:
                    return o_tuple_item::Element::string(typeManager.extractString(object));
                case StorageClass::DB0_BYTES:
                case StorageClass::DB0_BYTES_ARRAY: {
                    auto bytes = typeManager.extractBytes(object);
                    return o_tuple_item::Element::bytes(bytes.m_data, bytes.m_size);
                }
                case StorageClass::DB0_LIST:
                case StorageClass::DB0_TUPLE:
                    return scratch.embeddedTuple(measurePyTupleNoWriters(object, scratch));
                case StorageClass::DB0_SET:
                    return scratch.embeddedSet(measurePySetNoWriters(object, scratch));
                case StorageClass::DB0_DICT:
                    return scratch.embeddedDict(measurePyDictNoWriters(object, scratch));
                case StorageClass::OBJECT_REF:
                case StorageClass::EMBEDDED_OBJECT: {
                    auto *nestedInitializer = tryGetImmutableInitializer(object);
                    if (!nestedInitializer) {
                        THROWF(db0::InputException)
                            << "Only non-materialized immutable memo objects can be embedded";
                    }
                    auto classRef = nestedInitializer->getClassPtr()->getClassRef();
                    return scratch.embeddedObject(
                        measureEmbeddedObjectNoWriters(classRef, *nestedInitializer, scratch)
                    );
                }
                default:
                    THROWF(db0::InputException)
                        << "Storage class cannot be stored in embedded field map: " << storageClass;
            }
            return o_tuple_item::Element::none();
        }

        o_tuple_item::Element elementFromPythonObjectNoWriters(PyObject *object, MeasureScratch &scratch)
        {
            auto &typeManager = db0::python::PyToolkit::getTypeManager();
            auto typeId = typeManager.getTypeId(object);
            switch (typeId) {
                case db0::bindings::TypeId::NONE:
                    return o_tuple_item::Element::none();
                case db0::bindings::TypeId::BOOLEAN:
                    return o_tuple_item::Element::boolean(object == Py_True);
                case db0::bindings::TypeId::INTEGER:
                    return o_tuple_item::Element::integer(PyLong_AsLongLong(object));
                case db0::bindings::TypeId::FLOAT:
                    return o_tuple_item::Element::floating(PyFloat_AsDouble(object));
                case db0::bindings::TypeId::DATE:
                    return o_tuple_item::Element::date(typeManager.extractUInt64(typeId, object));
                case db0::bindings::TypeId::DATETIME:
                    return o_tuple_item::Element::datetime(typeManager.extractUInt64(typeId, object));
                case db0::bindings::TypeId::DATETIME_TZ:
                    return o_tuple_item::Element::datetimeTz(typeManager.extractUInt64(typeId, object));
                case db0::bindings::TypeId::TIME:
                    return o_tuple_item::Element::time(typeManager.extractUInt64(typeId, object));
                case db0::bindings::TypeId::TIME_TZ:
                    return o_tuple_item::Element::timeTz(typeManager.extractUInt64(typeId, object));
                case db0::bindings::TypeId::DECIMAL:
                    return o_tuple_item::Element::decimal(typeManager.extractUInt64(typeId, object));
                case db0::bindings::TypeId::STRING:
                    return o_tuple_item::Element::string(typeManager.extractString(object));
                case db0::bindings::TypeId::BYTES: {
                    auto bytes = typeManager.extractBytes(object);
                    return o_tuple_item::Element::bytes(bytes.m_data, bytes.m_size);
                }
                case db0::bindings::TypeId::LIST:
                case db0::bindings::TypeId::TUPLE:
                    return scratch.embeddedTuple(measurePyTupleNoWriters(object, scratch));
                case db0::bindings::TypeId::SET:
                    return scratch.embeddedSet(measurePySetNoWriters(object, scratch));
                case db0::bindings::TypeId::DICT:
                    return scratch.embeddedDict(measurePyDictNoWriters(object, scratch));
                case db0::bindings::TypeId::MEMO_IMMUTABLE_OBJECT: {
                    auto *nestedInitializer = tryGetImmutableInitializer(object);
                    if (!nestedInitializer) {
                        THROWF(db0::InputException)
                            << "Only non-materialized immutable memo objects can be embedded";
                    }
                    auto classRef = nestedInitializer->getClassPtr()->getClassRef();
                    return scratch.embeddedObject(
                        measureEmbeddedObjectNoWriters(classRef, *nestedInitializer, scratch)
                    );
                }
                default:
                    break;
            }

            THROWF(db0::InputException) << "Unsupported embedded Python object type: " << Py_TYPE(object)->tp_name;
            return o_tuple_item::Element::none();
        }

        o_dict::ElementMap buildMeasureFieldMapNoWriters(
            const ImmutableObjectInitializer &initializer, MeasureScratch &scratch
        )
        {
            o_dict::ElementMap fieldMap;
            for (const auto &value: initializer.objects()) {
                auto key = o_tuple_item::Element::integer(value.m_loc.first);
                if (!value.m_object || value.m_storage_class == StorageClass::DELETED) {
                    fieldMap.erase(key);
                    continue;
                }
                assert(value.m_loc.second == 0 && "Variable-length embedded fields must use default fidelity");
                fieldMap[key] = objectValueElementNoWriters(value.m_storage_class, value.m_object.get(), scratch);
            }
            return fieldMap;
        }

        std::size_t measureEmbeddedObjectNoWriters(
            std::uint32_t classRef, const ImmutableObjectInitializer &initializer, MeasureScratch &scratch
        )
        {
            PosVT::Data posVtData;
            unsigned int posVtOffset = 0;
            auto indexVtData = initializer.getData(posVtData, posVtOffset);
            auto fieldMap = buildMeasureFieldMapNoWriters(initializer, scratch);
            return EmbeddedObjectMeter::measureMembers()
                (db0::packed_int32::type(), classRef)
                (PosVT::type(), posVtData, posVtOffset)
                (IndexVT::type(), indexVtData.first, indexVtData.second)
                (o_dict::type(), fieldMap);
        }

    }

    o_immutable_object::o_immutable_object(std::uint32_t class_ref, 
        std::pair<std::uint32_t, std::uint32_t> ref_counts, std::uint8_t num_type_tags,
        const ImmutableObjectInitializer &initializer)
        : super_t(ref_counts)
        , m_num_type_tags(num_type_tags)
    {
        m_header.setImmutableObject();
        std::vector<std::uint64_t> offsets;
        EmbeddedObjectOffsetCollector offsetCollector{ reinterpret_cast<const std::byte *>(this), &offsets };
        auto arranger = arrangeMembers();
        arranger = arranger(o_embedded_object::type(), class_ref, initializer, offsetCollector);
        arranger(o_packed_offset_index::type(), offsets);
    }

    o_immutable_object::o_immutable_object(std::uint32_t class_ref, 
        std::pair<std::uint32_t, std::uint32_t> ref_counts, std::uint8_t num_type_tags, const PosVT::Data &pos_vt_data, 
        unsigned int pos_vt_offset, const XValue *index_vt_begin, const XValue *index_vt_end)
        : super_t(ref_counts)
        , m_num_type_tags(num_type_tags)
    {
        m_header.setImmutableObject();
        auto arranger = arrangeMembers();
        arranger = arranger(o_embedded_object::type(), class_ref, pos_vt_data, pos_vt_offset, index_vt_begin, index_vt_end);
        arranger(o_packed_offset_index::type(), std::vector<std::uint64_t>());
    }

    std::size_t o_immutable_object::measure(std::uint32_t class_ref,
        std::pair<std::uint32_t, std::uint32_t> ref_counts, std::uint8_t, const ImmutableObjectInitializer &initializer)
    {
        MeasureScratch scratch;
        auto offsets = worstCaseOffsetIndexValues(countEmbeddedMemoObjectsInInitializer(initializer));
        return super_t::measureMembersFromBase(ref_counts)
            (measureEmbeddedObjectNoWriters(class_ref, initializer, scratch))
            (o_packed_offset_index::type(), offsets);
    }

    std::size_t o_immutable_object::measure(std::uint32_t class_ref,
        std::pair<std::uint32_t, std::uint32_t> ref_counts, std::uint8_t, const PosVT::Data &pos_vt_data, unsigned int pos_vt_offset,
        const XValue *index_vt_begin, const XValue *index_vt_end)
    {
        return super_t::measureMembersFromBase(ref_counts)
            (o_embedded_object::type(), class_ref, pos_vt_data, pos_vt_offset, index_vt_begin, index_vt_end)
            (o_packed_offset_index::type(), std::vector<std::uint64_t>());
    }

    o_embedded_object &o_immutable_object::getObject()
    {
        return getDynFirst(o_embedded_object::type());
    }

    const o_embedded_object &o_immutable_object::getObject() const
    {
        return getDynFirst(o_embedded_object::type());
    }

    const o_packed_offset_index &o_immutable_object::getOffsetIndex() const
    {
        return getDynAfter(getObject(), o_packed_offset_index::type());
    }

    const PosVT &o_immutable_object::pos_vt() const {
        return getObject().pos_vt();
    }

    PosVT &o_immutable_object::pos_vt() {
        return getObject().pos_vt();
    }

    std::uint32_t o_immutable_object::getClassRef() const {
        return getObject().getClassRef();
    }
    
    const IndexVT &o_immutable_object::index_vt() const {
        return getObject().index_vt();
    }
    
    IndexVT &o_immutable_object::index_vt() {
        return getObject().index_vt();
    }

    const o_dict &o_immutable_object::field_map() const
    {
        return getObject().field_map();
    }

    std::optional<FixedValue> o_immutable_object::fixedValue(std::uint32_t index, unsigned int fidelityOffset) const
    {
        return getObject().fixedValue(index, fidelityOffset);
    }

    const o_tuple_item *o_immutable_object::variableValue(std::uint32_t index) const
    {
        return getObject().variableValue(index);
    }
    
    void o_immutable_object::incRef(bool is_tag) {
        m_header.incRef(is_tag);
    }
    
    bool o_immutable_object::hasRefs() const
    {
        // NOTE: type tags are not counted as "proper" references
        if (m_header.m_ref_counter.getFirst() > this->m_num_type_tags) {
            return true;
        }
        return m_header.m_ref_counter.getSecond() > 0;
    }
    
    bool o_immutable_object::hasAnyRefs() const {
        return m_header.hasRefs();
    }
    
}
