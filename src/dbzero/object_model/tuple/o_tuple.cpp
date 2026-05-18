// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "o_tuple.hpp"

#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::object_model
{

    o_tuple_item::Element::Payload::Payload()
        : m_int_value(0)
    {
    }

    o_tuple_item::Element o_tuple_item::Element::none()
    {
        return { StorageClass::NONE };
    }

    o_tuple_item::Element o_tuple_item::Element::boolean(bool value)
    {
        Element result;
        result.m_storage_class = StorageClass::BOOLEAN;
        result.m_payload.m_bool_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::integer(std::int64_t value)
    {
        Element result;
        result.m_storage_class = StorageClass::INT64;
        result.m_payload.m_int_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::floating(double value)
    {
        Element result;
        result.m_storage_class = StorageClass::FP_NUMERIC64;
        result.m_payload.m_double_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::string(std::string_view value)
    {
        Element result;
        result.m_storage_class = StorageClass::STRING_REF;
        result.m_payload.m_string_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::bytes(const std::byte *data, std::size_t size)
    {
        Element result;
        result.m_storage_class = StorageClass::DB0_BYTES;
        result.m_payload.m_bytes_value = { data, size };
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::bytes(const std::vector<std::byte> &value)
    {
        return bytes(value.data(), value.size());
    }

    std::int64_t o_tuple_item::Element::intValue() const
    {
        return m_payload.m_int_value;
    }

    double o_tuple_item::Element::doubleValue() const
    {
        return m_payload.m_double_value;
    }

    bool o_tuple_item::Element::boolValue() const
    {
        return m_payload.m_bool_value;
    }

    std::string o_tuple_item::Element::stringValue() const
    {
        return std::string(m_payload.m_string_value);
    }

    const std::byte *o_tuple_item::Element::bytesData() const
    {
        return m_payload.m_bytes_value.m_data;
    }

    std::size_t o_tuple_item::Element::bytesSize() const
    {
        return m_payload.m_bytes_value.m_size;
    }

    o_tuple_item::o_tuple_item(const Element &element)
        : m_storage_class(element.m_storage_class)
    {
        arrangePayload(element);
    }

    StorageClass o_tuple_item::storageClass() const
    {
        return m_storage_class;
    }

    std::size_t o_tuple_item::sizeOf() const
    {
        switch (m_storage_class) {
        case StorageClass::NONE:
            return sizeOfMembers();
        case StorageClass::BOOLEAN:
            return sizeOfMembers()(o_simple<bool>::type());
        case StorageClass::INT64:
            return sizeOfMembers()(o_simple<std::int64_t>::type());
        case StorageClass::FP_NUMERIC64:
            return sizeOfMembers()(o_simple<double>::type());
        case StorageClass::STRING_REF:
            return sizeOfMembers()(o_string::type());
        case StorageClass::DB0_BYTES:
            return sizeOfMembers()(o_binary::type());
        default:
            throwUnsupportedStorageClass();
            return 0;
        }
    }

    std::size_t o_tuple_item::measure(const Element &element)
    {
        switch (element.m_storage_class) {
        case StorageClass::NONE:
            return measureMembers();
        case StorageClass::BOOLEAN:
            return measureMembers()(o_simple<bool>::type(), element.boolValue());
        case StorageClass::INT64:
            return measureMembers()(o_simple<std::int64_t>::type(), element.intValue());
        case StorageClass::FP_NUMERIC64:
            return measureMembers()(o_simple<double>::type(), element.doubleValue());
        case StorageClass::STRING_REF:
            return measureMembers()(o_string::type(), element.stringValue());
        case StorageClass::DB0_BYTES:
            return measureMembers()(o_binary::type(), element.bytesData(), element.bytesSize());
        default:
            throwUnsupportedStorageClass();
            return 0;
        }
    }

    void o_tuple_item::arrangePayload(const Element &element)
    {
        switch (element.m_storage_class) {
        case StorageClass::NONE:
            arrangeMembers();
            return;
        case StorageClass::BOOLEAN:
            arrangeMembers()(o_simple<bool>::type(), element.boolValue());
            return;
        case StorageClass::INT64:
            arrangeMembers()(o_simple<std::int64_t>::type(), element.intValue());
            return;
        case StorageClass::FP_NUMERIC64:
            arrangeMembers()(o_simple<double>::type(), element.doubleValue());
            return;
        case StorageClass::STRING_REF:
            arrangeMembers()(o_string::type(), element.stringValue());
            return;
        case StorageClass::DB0_BYTES:
            arrangeMembers()(o_binary::type(), element.bytesData(), element.bytesSize());
            return;
        default:
            throwUnsupportedStorageClass();
        }
    }

    const o_simple<bool> &o_tuple_item::boolPayload() const
    {
        return getDynFirst(o_simple<bool>::type());
    }

    const o_simple<std::int64_t> &o_tuple_item::intPayload() const
    {
        return getDynFirst(o_simple<std::int64_t>::type());
    }

    const o_simple<double> &o_tuple_item::doublePayload() const
    {
        return getDynFirst(o_simple<double>::type());
    }

    const o_string &o_tuple_item::stringPayload() const
    {
        return getDynFirst(o_string::type());
    }

    const o_binary &o_tuple_item::bytesPayload() const
    {
        return getDynFirst(o_binary::type());
    }

    void o_tuple_item::throwUnsupportedStorageClass()
    {
        THROWF(db0::InternalException) << "Unsupported tuple item storage class";
    }

    const o_tuple_item &o_tuple::const_iterator::operator*() const
    {
        return *m_item;
    }

    const o_tuple_item *o_tuple::const_iterator::operator->() const
    {
        return m_item;
    }

    o_tuple::const_iterator &o_tuple::const_iterator::operator++()
    {
        m_item = reinterpret_cast<const o_tuple_item *>(
            reinterpret_cast<const std::byte *>(m_item) + m_item->sizeOf()
        );
        return *this;
    }

    bool o_tuple::const_iterator::operator==(const const_iterator &other) const
    {
        return m_item == other.m_item;
    }

    bool o_tuple::const_iterator::operator!=(const const_iterator &other) const
    {
        return m_item != other.m_item;
    }

    o_tuple::const_iterator::const_iterator(const o_tuple_item *item)
        : m_item(item)
    {
    }

    o_tuple::o_tuple(const std::vector<Element> &elements)
    {
        auto elementsByteSize = static_cast<std::uint32_t>(measureElements(elements));
        auto arranger = arrangeMembers();
        arranger = arranger(db0::packed_int32::type(), static_cast<std::uint32_t>(elements.size()));
        arranger = arranger(db0::packed_int32::type(), elementsByteSize);
        for (const auto &element: elements) {
            arranger = arranger(o_tuple_item::type(), element);
        }
    }

    std::size_t o_tuple::size() const
    {
        return count().value();
    }

    std::size_t o_tuple::elementsByteSize() const
    {
        return elementsByteSizeMember().value();
    }

    bool o_tuple::empty() const
    {
        return size() == 0;
    }

    const o_tuple_item &o_tuple::item(std::size_t index) const
    {
        auto it = begin();
        for (std::size_t i = 0; i < index; ++i) {
            ++it;
        }
        return *it;
    }

    o_tuple::const_iterator o_tuple::begin() const
    {
        return const_iterator(reinterpret_cast<const o_tuple_item *>(beginOfItems()));
    }

    o_tuple::const_iterator o_tuple::end() const
    {
        return const_iterator(reinterpret_cast<const o_tuple_item *>(beginOfItems() + elementsByteSize()));
    }

    std::size_t o_tuple::sizeOf() const
    {
        return safeSizeOf(reinterpret_cast<const std::byte *>(this));
    }

    std::size_t o_tuple::measure(const std::vector<Element> &elements)
    {
        auto elementsByteSize = measureElements(elements);
        return measureMembers()
            (db0::packed_int32::type(), static_cast<std::uint32_t>(elements.size()))
            (db0::packed_int32::type(), static_cast<std::uint32_t>(elementsByteSize))
            (elementsByteSize);
    }

    const db0::packed_int32 &o_tuple::count() const
    {
        return getDynFirst(db0::packed_int32::type());
    }

    const db0::packed_int32 &o_tuple::elementsByteSizeMember() const
    {
        return getDynAfter(count(), db0::packed_int32::type());
    }

    const std::byte *o_tuple::beginOfItems() const
    {
        const auto &elementsByteSizeMemberRef = elementsByteSizeMember();
        return reinterpret_cast<const std::byte *>(&elementsByteSizeMemberRef) + elementsByteSizeMemberRef.sizeOf();
    }

    std::size_t o_tuple::measureElements(const std::vector<Element> &elements)
    {
        std::size_t size = 0;
        for (const auto &element: elements) {
            size += o_tuple_item::measure(element);
        }
        return size;
    }

}
