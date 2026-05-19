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
        return { TupleItemKind::NONE };
    }

    o_tuple_item::Element o_tuple_item::Element::boolean(bool value)
    {
        Element result;
        result.m_kind = TupleItemKind::BOOLEAN;
        result.m_payload.m_bool_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::integer(std::int64_t value)
    {
        Element result;
        if (value >= 0 && packed_int64::measure(static_cast<std::uint64_t>(value)) <= 6) {
            result.m_kind = TupleItemKind::PACKED_INT64;
        } else {
            result.m_kind = TupleItemKind::INT64;
        }
        result.m_payload.m_int_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::floating(double value)
    {
        Element result;
        result.m_kind = TupleItemKind::FP_NUMERIC64;
        result.m_payload.m_double_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::string(std::string_view value)
    {
        Element result;
        result.m_kind = TupleItemKind::STRING;
        result.m_payload.m_string_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::bytes(const std::byte *data, std::size_t size)
    {
        Element result;
        result.m_kind = TupleItemKind::BINARY;
        result.m_payload.m_bytes_value = { data, size };
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::bytes(const std::vector<std::byte> &value)
    {
        return bytes(value.data(), value.size());
    }

    o_tuple_item::Element o_tuple_item::Element::timestamp(std::uint64_t value)
    {
        Element result;
        result.m_kind = TupleItemKind::PTIME64;
        result.m_payload.m_uint64_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::date(std::uint64_t value)
    {
        Element result;
        result.m_kind = TupleItemKind::DATE;
        result.m_payload.m_uint64_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::datetime(std::uint64_t value)
    {
        Element result;
        result.m_kind = TupleItemKind::DATETIME;
        result.m_payload.m_uint64_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::datetimeTz(std::uint64_t value)
    {
        Element result;
        result.m_kind = TupleItemKind::DATETIME_TZ;
        result.m_payload.m_uint64_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::time(std::uint64_t value)
    {
        Element result;
        result.m_kind = TupleItemKind::TIME;
        result.m_payload.m_uint64_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::timeTz(std::uint64_t value)
    {
        Element result;
        result.m_kind = TupleItemKind::TIME_TZ;
        result.m_payload.m_uint64_value = value;
        return result;
    }

    o_tuple_item::Element o_tuple_item::Element::decimal(std::uint64_t value)
    {
        Element result;
        result.m_kind = TupleItemKind::DECIMAL;
        result.m_payload.m_uint64_value = value;
        return result;
    }

    std::int64_t o_tuple_item::Element::intValue() const
    {
        return m_payload.m_int_value;
    }

    std::uint64_t o_tuple_item::Element::uint64Value() const
    {
        return m_payload.m_uint64_value;
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
        : m_kind(element.m_kind)
    {
        arrangePayload(element);
    }

    TupleItemKind o_tuple_item::itemKind() const
    {
        return m_kind;
    }

    std::size_t o_tuple_item::sizeOf() const
    {
        switch (m_kind) {
        case TupleItemKind::NONE:
            return sizeOfMembers();
        case TupleItemKind::BOOLEAN:
            return sizeOfMembers()(o_simple<bool>::type());
        case TupleItemKind::INT64:
            return sizeOfMembers()(o_simple<std::int64_t>::type());
        case TupleItemKind::PACKED_INT64:
            return sizeOfMembers()(packed_int64::type());
        case TupleItemKind::FP_NUMERIC64:
            return sizeOfMembers()(o_simple<double>::type());
        case TupleItemKind::STRING:
            return sizeOfMembers()(o_string::type());
        case TupleItemKind::BINARY:
            return sizeOfMembers()(o_binary::type());
        case TupleItemKind::PTIME64:
        case TupleItemKind::DATE:
        case TupleItemKind::DATETIME:
        case TupleItemKind::DATETIME_TZ:
        case TupleItemKind::TIME:
        case TupleItemKind::TIME_TZ:
        case TupleItemKind::DECIMAL:
            return sizeOfMembers()(o_simple<std::uint64_t>::type());
        default:
            throwUnsupportedItemKind();
            return 0;
        }
    }

    std::size_t o_tuple_item::measure(const Element &element)
    {
        switch (element.m_kind) {
        case TupleItemKind::NONE:
            return measureMembers();
        case TupleItemKind::BOOLEAN:
            return measureMembers()(o_simple<bool>::type(), element.boolValue());
        case TupleItemKind::INT64:
            return measureMembers()(o_simple<std::int64_t>::type(), element.intValue());
        case TupleItemKind::PACKED_INT64:
            return measureMembers()(packed_int64::type(), static_cast<std::uint64_t>(element.intValue()));
        case TupleItemKind::FP_NUMERIC64:
            return measureMembers()(o_simple<double>::type(), element.doubleValue());
        case TupleItemKind::STRING:
            return measureMembers()(o_string::type(), element.stringValue());
        case TupleItemKind::BINARY:
            return measureMembers()(o_binary::type(), element.bytesData(), element.bytesSize());
        case TupleItemKind::PTIME64:
        case TupleItemKind::DATE:
        case TupleItemKind::DATETIME:
        case TupleItemKind::DATETIME_TZ:
        case TupleItemKind::TIME:
        case TupleItemKind::TIME_TZ:
        case TupleItemKind::DECIMAL:
            return measureMembers()(o_simple<std::uint64_t>::type(), element.uint64Value());
        default:
            throwUnsupportedItemKind();
            return 0;
        }
    }

    void o_tuple_item::arrangePayload(const Element &element)
    {
        switch (element.m_kind) {
        case TupleItemKind::NONE:
            arrangeMembers();
            return;
        case TupleItemKind::BOOLEAN:
            arrangeMembers()(o_simple<bool>::type(), element.boolValue());
            return;
        case TupleItemKind::INT64:
            arrangeMembers()(o_simple<std::int64_t>::type(), element.intValue());
            return;
        case TupleItemKind::PACKED_INT64:
            arrangeMembers()(packed_int64::type(), static_cast<std::uint64_t>(element.intValue()));
            return;
        case TupleItemKind::FP_NUMERIC64:
            arrangeMembers()(o_simple<double>::type(), element.doubleValue());
            return;
        case TupleItemKind::STRING:
            arrangeMembers()(o_string::type(), element.stringValue());
            return;
        case TupleItemKind::BINARY:
            arrangeMembers()(o_binary::type(), element.bytesData(), element.bytesSize());
            return;
        case TupleItemKind::PTIME64:
        case TupleItemKind::DATE:
        case TupleItemKind::DATETIME:
        case TupleItemKind::DATETIME_TZ:
        case TupleItemKind::TIME:
        case TupleItemKind::TIME_TZ:
        case TupleItemKind::DECIMAL:
            arrangeMembers()(o_simple<std::uint64_t>::type(), element.uint64Value());
            return;
        default:
            throwUnsupportedItemKind();
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

    const packed_int64 &o_tuple_item::packedIntPayload() const
    {
        return getDynFirst(packed_int64::type());
    }

    const o_simple<std::uint64_t> &o_tuple_item::uint64Payload() const
    {
        return getDynFirst(o_simple<std::uint64_t>::type());
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

    void o_tuple_item::throwUnsupportedItemKind()
    {
        THROWF(db0::InternalException) << "Unsupported tuple item kind";
    }

    template <bool compact>
    const o_tuple_item &o_tuple<compact>::const_iterator::operator*() const
    {
        return *m_item;
    }

    template <bool compact>
    const o_tuple_item *o_tuple<compact>::const_iterator::operator->() const
    {
        return m_item;
    }

    template <bool compact>
    typename o_tuple<compact>::const_iterator &o_tuple<compact>::const_iterator::operator++()
    {
        m_item = reinterpret_cast<const o_tuple_item *>(
            reinterpret_cast<const std::byte *>(m_item) + m_item->sizeOf()
        );
        return *this;
    }

    template <bool compact>
    bool o_tuple<compact>::const_iterator::operator==(const const_iterator &other) const
    {
        return m_item == other.m_item;
    }

    template <bool compact>
    bool o_tuple<compact>::const_iterator::operator!=(const const_iterator &other) const
    {
        return m_item != other.m_item;
    }

    template <bool compact>
    o_tuple<compact>::const_iterator::const_iterator(const o_tuple_item *item)
        : m_item(item)
    {
    }

    template <bool compact>
    o_tuple<compact>::o_tuple(const std::vector<Element> &elements)
    {
        auto elementsByteSize = static_cast<std::uint32_t>(measureElements(elements));
        Builder builder(*this, static_cast<std::uint32_t>(elements.size()), elementsByteSize);
        for (const auto &element: elements) {
            builder.add(element);
        }
        builder.finish();
    }

    template <bool compact>
    std::size_t o_tuple<compact>::size() const
    {
        return count().value();
    }

    template <bool compact>
    std::size_t o_tuple<compact>::elementsByteSize() const
    {
        if constexpr (!compact) {
            const auto &elementsByteSizeMember = this->getDynAfter(count(), db0::packed_int32::type());
            return elementsByteSizeMember.value();
        } else {
            std::size_t result = 0;
            auto *cursor = beginOfItems();
            for (std::uint32_t i = 0; i < size(); ++i) {
                const auto &tupleItem = o_tuple_item::__const_ref(cursor);
                auto itemSize = tupleItem.sizeOf();
                result += itemSize;
                cursor += itemSize;
            }
            return result;
        }
    }

    template <bool compact>
    bool o_tuple<compact>::empty() const
    {
        return size() == 0;
    }

    template <bool compact>
    const o_tuple_item &o_tuple<compact>::item(std::size_t index) const
    {
        auto it = begin();
        for (std::size_t i = 0; i < index; ++i) {
            ++it;
        }
        return *it;
    }

    template <bool compact>
    typename o_tuple<compact>::const_iterator o_tuple<compact>::begin() const
    {
        return const_iterator(reinterpret_cast<const o_tuple_item *>(beginOfItems()));
    }

    template <bool compact>
    typename o_tuple<compact>::const_iterator o_tuple<compact>::end() const
    {
        return const_iterator(reinterpret_cast<const o_tuple_item *>(beginOfItems() + elementsByteSize()));
    }

    template <bool compact>
    std::size_t o_tuple<compact>::sizeOf() const
    {
        return safeSizeOf(reinterpret_cast<const std::byte *>(this));
    }

    template <bool compact>
    std::size_t o_tuple<compact>::measure(const std::vector<Element> &elements)
    {
        auto elementsByteSize = measureElements(elements);
        return Builder::measure(static_cast<std::uint32_t>(elements.size()), static_cast<std::uint32_t>(elementsByteSize));
    }

    template <bool compact>
    o_tuple<compact>::Builder::Builder(void *buf, std::uint32_t count, std::uint32_t elementsByteSize)
        : m_tuple(*(new(buf) o_tuple<compact>()))
        , m_arranger(m_tuple.arrangeMembers())
        , m_expectedCount(count)
    {
        m_arranger = m_arranger(db0::packed_int32::type(), count);
        if constexpr (!compact) {
            m_arranger = m_arranger(db0::packed_int32::type(), elementsByteSize);
        }
    }

    template <bool compact>
    o_tuple<compact>::Builder::Builder(o_tuple<compact> &tuple, std::uint32_t count, std::uint32_t elementsByteSize)
        : m_tuple(tuple)
        , m_arranger(m_tuple.arrangeMembers())
        , m_expectedCount(count)
    {
        m_arranger = m_arranger(db0::packed_int32::type(), count);
        if constexpr (!compact) {
            m_arranger = m_arranger(db0::packed_int32::type(), elementsByteSize);
        }
    }

    template <bool compact>
    void o_tuple<compact>::Builder::add(const Element &element)
    {
        m_arranger = m_arranger(o_tuple_item::type(), element);
        ++m_addedCount;
    }

    template <bool compact>
    o_tuple<compact> &o_tuple<compact>::Builder::finish()
    {
        if (m_addedCount != m_expectedCount) {
            THROWF(db0::InternalException) << "Tuple builder received unexpected element count";
        }
        return m_tuple;
    }

    template <bool compact>
    std::size_t o_tuple<compact>::Builder::measure(std::uint32_t count, std::uint32_t elementsByteSize)
    {
        if constexpr (compact) {
            return super_t::measureMembers()
                (db0::packed_int32::type(), count)
                (static_cast<std::size_t>(elementsByteSize));
        } else {
            return super_t::measureMembers()
                (db0::packed_int32::type(), count)
                (db0::packed_int32::type(), elementsByteSize)
                (static_cast<std::size_t>(elementsByteSize));
        }
    }

    template <bool compact>
    std::size_t o_tuple<compact>::Builder::measureGrowth(
        std::uint32_t count, std::uint32_t elementsByteSize, std::uint32_t addedElementByteSize
    )
    {
        auto newCount = count + 1;
        auto newElementsByteSize = elementsByteSize + addedElementByteSize;
        if (newCount <= count || newElementsByteSize < elementsByteSize) {
            THROWF(db0::InternalException) << "Tuple builder growth exceeds uint32 range";
        }
        if (count == 0) {
            return 0;
        }
        auto newSize = measure(newCount, newElementsByteSize);
        if (count == 1) {
            return newSize;
        }
        return newSize - measure(count, elementsByteSize);
    }

    template <bool compact>
    const db0::packed_int32 &o_tuple<compact>::count() const
    {
        return this->getDynFirst(db0::packed_int32::type());
    }

    template <bool compact>
    const std::byte *o_tuple<compact>::beginOfItems() const
    {
        if constexpr (compact) {
            const auto &countMember = count();
            return reinterpret_cast<const std::byte *>(&countMember) + countMember.sizeOf();
        } else {
            const auto &elementsByteSizeMemberRef = this->getDynAfter(count(), db0::packed_int32::type());
            return reinterpret_cast<const std::byte *>(&elementsByteSizeMemberRef) + elementsByteSizeMemberRef.sizeOf();
        }
    }

    template <bool compact>
    std::size_t o_tuple<compact>::measureElements(const std::vector<Element> &elements)
    {
        std::size_t size = 0;
        for (const auto &element: elements) {
            size += o_tuple_item::measure(element);
        }
        return size;
    }

    template class o_tuple<false>;
    template class o_tuple<true>;

}
