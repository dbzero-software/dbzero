// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "o_embedded_object.hpp"

#include <dbzero/bindings/python/PyToolkit.hpp>
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/object_model/dict/o_py_dict.hpp>
#include <dbzero/object_model/set/o_py_set.hpp>
#include <dbzero/object_model/tuple/o_py_tuple.hpp>

namespace db0::object_model
{
    namespace
    {
        constexpr std::uint64_t PACK2_MASK = 0x3;

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

        o_dict::Element fieldMapElementFromObject(
            StorageClass storageClass, ImmutableObjectInitializer::ObjectSharedPtr object
        )
        {
            auto *pyObject = object.get();
            if (!pyObject) {
                THROWF(db0::InternalException) << "Cannot store null object in embedded field map";
            }

            switch (storageClass) {
                case StorageClass::STRING_REF:
                case StorageClass::POOLED_STRING:
                case StorageClass::STR64:
                    return o_dict::Element::string(db0::python::PyToolkit::getTypeManager().extractString(pyObject));
                case StorageClass::DB0_BYTES:
                case StorageClass::DB0_BYTES_ARRAY: {
                    auto bytes = db0::python::PyToolkit::getTypeManager().extractBytes(pyObject);
                    return o_dict::Element::bytes(bytes.m_data, bytes.m_size);
                }
                case StorageClass::DB0_LIST:
                case StorageClass::DB0_TUPLE: {
                    auto size = o_py_tuple::measure(pyObject);
                    return o_dict::Element::embeddedTuple(size, writePyTuple, pyObject);
                }
                case StorageClass::DB0_SET: {
                    auto size = o_py_set::measure(pyObject);
                    return o_dict::Element::embeddedSet(size, writePySet, pyObject);
                }
                case StorageClass::DB0_DICT: {
                    auto size = o_py_dict::measure(pyObject);
                    return o_dict::Element::embeddedDict(size, writePyDict, pyObject);
                }
                default:
                    THROWF(db0::InputException)
                        << "Storage class cannot be stored in embedded field map: " << storageClass;
            }
            return o_dict::Element::none();
        }

        o_dict::ElementMap buildEmbeddedFieldMap(const ImmutableObjectInitializer &initializer)
        {
            o_dict::ElementMap fieldMap;
            for (const auto &value: initializer.objects()) {
                assert(value.m_loc.second == 0 && "Variable-length embedded fields must use default fidelity");
                auto key = o_dict::Element::integer(value.m_loc.first);
                if (!value.m_object) {
                    fieldMap.erase(key);
                } else {
                    fieldMap[key] = fieldMapElementFromObject(value.m_storage_class, value.m_object);
                }
            }
            return fieldMap;
        }
    }

    FixedValue::FixedValue(StorageClass kind, std::uint64_t value)
        : m_kind(kind)
        , m_value(value)
    {
    }

    bool FixedValue::isPack2() const
    {
        return m_kind == StorageClass::PACK_2;
    }

    std::optional<FixedValue> FixedValue::unpack2(unsigned int offset) const
    {
        if (!isPack2()) {
            return *this;
        }
        if (offset >= 32) {
            THROWF(db0::InternalException) << "2-bit embedded value offset exceeds uint64 storage";
        }
        auto value = (m_value >> (offset * 2)) & PACK2_MASK;
        switch (value) {
            case 0:
                return FixedValue(StorageClass::NONE, 0);
            case 1:
                return FixedValue(StorageClass::BOOLEAN, 0);
            case 2:
                return FixedValue(StorageClass::BOOLEAN, 1);
            default:
                return std::nullopt;
        }
    }

    o_embedded_object::o_embedded_object(
        std::uint32_t classRefValue, const ImmutableObjectInitializer &initializer
    )
    {
        PosVT::Data posVtData;
        unsigned int posVtOffset = 0;
        auto indexVtData = initializer.getData(posVtData, posVtOffset);
        auto fieldMap = buildEmbeddedFieldMap(initializer);
        arrangeMembers()
            (db0::packed_int32::type(), classRefValue)
            (PosVT::type(), posVtData, posVtOffset)
            (IndexVT::type(), indexVtData.first, indexVtData.second)
            (o_dict::type(), fieldMap);
    }

    o_embedded_object::o_embedded_object(
        std::uint32_t classRefValue, const PosVT::Data &posVtData, unsigned int posVtOffset,
        const XValue *indexVtBegin, const XValue *indexVtEnd
    )
    {
        arrangeMembers()
            (db0::packed_int32::type(), classRefValue)
            (PosVT::type(), posVtData, posVtOffset)
            (IndexVT::type(), indexVtBegin, indexVtEnd)
            (o_dict::type(), o_dict::ElementMap());
    }

    std::uint32_t o_embedded_object::getClassRef() const
    {
        return classRef().value();
    }

    const PosVT &o_embedded_object::pos_vt() const
    {
        return getDynAfter(classRef(), PosVT::type());
    }

    PosVT &o_embedded_object::pos_vt()
    {
        return getDynAfter(classRef(), PosVT::type());
    }

    const IndexVT &o_embedded_object::index_vt() const
    {
        return getDynAfter(pos_vt(), IndexVT::type());
    }

    IndexVT &o_embedded_object::index_vt()
    {
        return getDynAfter(pos_vt(), IndexVT::type());
    }

    const o_dict &o_embedded_object::field_map() const
    {
        return getDynAfter(index_vt(), o_dict::type());
    }

    std::optional<FixedValue> o_embedded_object::fixedValue(
        std::uint32_t index, unsigned int fidelityOffset
    ) const
    {
        std::pair<StorageClass, Value> posValue;
        if (pos_vt().find(index, posValue)) {
            auto value = FixedValue(posValue.first, posValue.second.m_store);
            return value.isPack2() ? value.unpack2(fidelityOffset) : std::optional<FixedValue>(value);
        }
        if (index_vt().find(index, posValue)) {
            auto value = FixedValue(posValue.first, posValue.second.m_store);
            return value.isPack2() ? value.unpack2(fidelityOffset) : std::optional<FixedValue>(value);
        }
        return std::nullopt;
    }

    const o_tuple_item *o_embedded_object::variableValue(std::uint32_t index) const
    {
        return field_map().get(o_dict::Element::integer(index));
    }

    std::size_t o_embedded_object::sizeOf() const
    {
        return safeSizeOf(reinterpret_cast<const std::byte *>(this));
    }

    std::size_t o_embedded_object::measure(
        std::uint32_t classRefValue, const ImmutableObjectInitializer &initializer
    )
    {
        PosVT::Data posVtData;
        unsigned int posVtOffset = 0;
        auto indexVtData = initializer.getData(posVtData, posVtOffset);
        auto fieldMap = buildEmbeddedFieldMap(initializer);
        return measureMembers()
            (db0::packed_int32::type(), classRefValue)
            (PosVT::type(), posVtData, posVtOffset)
            (IndexVT::type(), indexVtData.first, indexVtData.second)
            (o_dict::type(), fieldMap);
    }

    std::size_t o_embedded_object::measure(
        std::uint32_t classRefValue, const PosVT::Data &posVtData, unsigned int posVtOffset,
        const XValue *indexVtBegin, const XValue *indexVtEnd
    )
    {
        return measureMembers()
            (db0::packed_int32::type(), classRefValue)
            (PosVT::type(), posVtData, posVtOffset)
            (IndexVT::type(), indexVtBegin, indexVtEnd)
            (o_dict::type(), o_dict::ElementMap());
    }

    const db0::packed_int32 &o_embedded_object::classRef() const
    {
        return getDynFirst(db0::packed_int32::type());
    }

}
