// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "o_set.hpp"

#include <cstring>
#include <limits>
#include <vector>

#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/utils/hash_func.hpp>

namespace db0::object_model
{
    bool o_set::HashIndexEntry::isEmpty() const
    {
        return m_value == 0;
    }

    bool o_set::HashIndexEntry::isBucket() const
    {
        return (m_value & BUCKET_FLAG) != 0;
    }

    bool o_set::HashIndexEntry::isPendingBucket() const
    {
        return m_value == BUCKET_FLAG;
    }

    std::uint32_t o_set::HashIndexEntry::offset() const
    {
        return (m_value & OFFSET_MASK) - 1;
    }

    void o_set::HashIndexEntry::clear()
    {
        m_value = 0;
    }

    void o_set::HashIndexEntry::setPendingBucket()
    {
        m_value = BUCKET_FLAG;
    }

    void o_set::HashIndexEntry::setItem(std::uint32_t offset)
    {
        if (offset >= OFFSET_MASK) {
            THROWF(db0::InternalException) << "Set item offset exceeds hash index entry capacity";
        }
        m_value = offset + 1;
    }

    void o_set::HashIndexEntry::setBucket(std::uint32_t offset)
    {
        if (offset >= OFFSET_MASK) {
            THROWF(db0::InternalException) << "Set bucket offset exceeds hash index entry capacity";
        }
        m_value = BUCKET_FLAG | (offset + 1);
    }

    const o_set::Item &o_set::const_iterator::operator*() const
    {
        return *m_item;
    }

    const o_set::Item *o_set::const_iterator::operator->() const
    {
        return m_item;
    }

    o_set::const_iterator &o_set::const_iterator::operator++()
    {
        m_item = reinterpret_cast<const Item *>(
            reinterpret_cast<const std::byte *>(m_item) + m_item->sizeOf()
        );
        return *this;
    }

    bool o_set::const_iterator::operator==(const const_iterator &other) const
    {
        return m_item == other.m_item;
    }

    bool o_set::const_iterator::operator!=(const const_iterator &other) const
    {
        return m_item != other.m_item;
    }

    o_set::const_iterator::const_iterator(const Item *item)
        : m_item(item)
    {
    }

    std::size_t o_set::ElementHash::operator()(const Element &element) const
    {
        return elementHash(element);
    }

    bool o_set::ElementEqual::operator()(const Element &lhs, const Element &rhs) const
    {
        return elementsEqual(lhs, rhs);
    }

    o_set::o_set(const ElementSet &elements)
    {
        auto elementsByteSize = checkedUint32Size(measureElements(elements), "Set elements byte size");
        auto capacity = hashIndexCapacity(elements.size());
        auto bucketByteSize = checkedUint32Size(
            measureCollisionBuckets(elements, capacity), "Set bucket byte size"
        );

        auto arranger = arrangeSetMembers(static_cast<std::uint32_t>(elements.size()), elementsByteSize, bucketByteSize);
        for (const auto &element: elements) {
            arranger = arranger(Item::type(), element);
        }

        finishSetConstruction(arranger.ptr(), elementsByteSize, capacity, bucketByteSize);
    }

    db0::Foundation::Arranger o_set::arrangeSetMembers(
        std::uint32_t count, std::uint32_t elementsByteSize, std::uint32_t bucketByteSize
    )
    {
        return arrangeMembers()
            (db0::packed_int32::type(), count)
            (db0::packed_int32::type(), elementsByteSize)
            (db0::packed_int32::type(), bucketByteSize);
    }

    void o_set::finishSetConstruction(
        void *indexEntriesPtr, std::uint32_t elementsByteSize, std::size_t capacity, std::uint32_t bucketByteSize
    )
    {
        auto *indexEntries = reinterpret_cast<HashIndexEntry *>(indexEntriesPtr);
        auto bucketByteSizeWritten = writeCollisionBuckets(
            indexEntries, beginOfItems(), elementsByteSize, capacity, reinterpret_cast<std::byte *>(indexEntries + capacity)
        );
        if (bucketByteSizeWritten != bucketByteSize) {
            THROWF(db0::InternalException) << "Set bucket byte size changed during construction";
        }
    }

    std::size_t o_set::size() const
    {
        return count().value();
    }

    std::size_t o_set::elementsByteSize() const
    {
        return elementsByteSizeMember().value();
    }

    bool o_set::empty() const
    {
        return size() == 0;
    }

    bool o_set::contains(const Element &element) const
    {
        auto capacity = hashIndexCapacity(size());
        if (capacity == 0) {
            return false;
        }

        auto hash = elementHash(element);
        const auto *entries = beginOfHashIndex();
        auto slot = hash % capacity;
        const auto &entry = entries[slot];
        if (entry.isEmpty()) {
            return false;
        }

        auto offset = entry.offset();
        if (!entry.isBucket()) {
            return itemEqualsElement(itemAtOffset(offset), element);
        }

        const auto &bucket = bucketAtOffset(offset);
        for (const auto &bucketItem: bucket) {
            if (itemEqualsElement(bucketItem, element)) {
                return true;
            }
        }
        return false;
    }

    const o_set::Item &o_set::item(std::size_t index) const
    {
        auto it = begin();
        for (std::size_t i = 0; i < index; ++i) {
            ++it;
        }
        return *it;
    }

    o_set::const_iterator o_set::begin() const
    {
        return const_iterator(reinterpret_cast<const Item *>(beginOfItems()));
    }

    o_set::const_iterator o_set::end() const
    {
        return const_iterator(reinterpret_cast<const Item *>(beginOfItems() + elementsByteSize()));
    }

    std::size_t o_set::sizeOf() const
    {
        return safeSizeOf(reinterpret_cast<const std::byte *>(this));
    }

    std::size_t o_set::measure(const ElementSet &elements)
    {
        auto elementsByteSize = measureElements(elements);
        auto bucketByteSize = measureCollisionBuckets(elements, hashIndexCapacity(elements.size()));
        return measureMembers()
            (db0::packed_int32::type(), static_cast<std::uint32_t>(elements.size()))
            (db0::packed_int32::type(), checkedUint32Size(elementsByteSize, "Set elements byte size"))
            (db0::packed_int32::type(), checkedUint32Size(bucketByteSize, "Set bucket byte size"))
            (elementsByteSize)
            (hashIndexByteSize(elements.size()))
            (bucketByteSize);
    }

    const db0::packed_int32 &o_set::count() const
    {
        return getDynFirst(db0::packed_int32::type());
    }

    const db0::packed_int32 &o_set::elementsByteSizeMember() const
    {
        return getDynAfter(count(), db0::packed_int32::type());
    }

    const db0::packed_int32 &o_set::bucketByteSizeMember() const
    {
        return getDynAfter(elementsByteSizeMember(), db0::packed_int32::type());
    }

    const std::byte *o_set::beginOfItems() const
    {
        const auto &bucketByteSizeMemberRef = bucketByteSizeMember();
        return reinterpret_cast<const std::byte *>(&bucketByteSizeMemberRef) + bucketByteSizeMemberRef.sizeOf();
    }

    o_set::HashIndexEntry *o_set::beginOfHashIndex()
    {
        return reinterpret_cast<HashIndexEntry *>(
            const_cast<std::byte *>(static_cast<const o_set *>(this)->beginOfItems()) + elementsByteSize()
        );
    }

    const o_set::HashIndexEntry *o_set::beginOfHashIndex() const
    {
        return reinterpret_cast<const HashIndexEntry *>(beginOfItems() + elementsByteSize());
    }

    const std::byte *o_set::beginOfBuckets() const
    {
        return reinterpret_cast<const std::byte *>(beginOfHashIndex() + hashIndexCapacity(size()));
    }

    const o_compact_tuple &o_set::bucketAtOffset(std::uint32_t offset) const
    {
        return o_compact_tuple::__const_ref(beginOfBuckets() + offset);
    }

    const o_set::Item &o_set::itemAtOffset(std::uint32_t offset) const
    {
        return Item::__const_ref(beginOfItems() + offset);
    }

    bool o_set::elementsEqual(const Element &lhs, const Element &rhs)
    {
        auto lhsIsInt = lhs.m_kind == TupleItemKind::INT64 || lhs.m_kind == TupleItemKind::PACKED_INT64;
        auto rhsIsInt = rhs.m_kind == TupleItemKind::INT64 || rhs.m_kind == TupleItemKind::PACKED_INT64;
        if (lhs.m_kind != rhs.m_kind && !(lhsIsInt && rhsIsInt)) {
            return false;
        }

        switch (lhs.m_kind) {
        case TupleItemKind::NONE:
            return true;
        case TupleItemKind::BOOLEAN:
            return lhs.boolValue() == rhs.boolValue();
        case TupleItemKind::INT64:
        case TupleItemKind::PACKED_INT64:
            return lhs.intValue() == rhs.intValue();
        case TupleItemKind::FP_NUMERIC64:
            return lhs.doubleValue() == rhs.doubleValue();
        case TupleItemKind::STRING:
            return lhs.m_payload.m_string_value == rhs.m_payload.m_string_value;
        case TupleItemKind::BINARY:
            return lhs.bytesSize() == rhs.bytesSize() && bytesEqual(lhs.bytesData(), rhs.bytesData(), lhs.bytesSize());
        case TupleItemKind::PTIME64:
        case TupleItemKind::DATE:
        case TupleItemKind::DATETIME:
        case TupleItemKind::DATETIME_TZ:
        case TupleItemKind::TIME:
        case TupleItemKind::TIME_TZ:
        case TupleItemKind::DECIMAL:
            return lhs.uint64Value() == rhs.uint64Value();
        default:
            THROWF(db0::InternalException) << "Unsupported set item kind";
        }
        return false;
    }

    bool o_set::itemEqualsElement(const Item &item, const Element &element)
    {
        auto itemIsInt = item.itemKind() == TupleItemKind::INT64 || item.itemKind() == TupleItemKind::PACKED_INT64;
        auto elementIsInt = element.m_kind == TupleItemKind::INT64 || element.m_kind == TupleItemKind::PACKED_INT64;
        if (item.itemKind() != element.m_kind && !(itemIsInt && elementIsInt)) {
            return false;
        }

        switch (element.m_kind) {
            case TupleItemKind::NONE:
                return true;
            case TupleItemKind::BOOLEAN:
                return item.boolPayload().value() == element.boolValue();
            case TupleItemKind::INT64:
            case TupleItemKind::PACKED_INT64: {
                auto itemValue = item.itemKind() == TupleItemKind::PACKED_INT64
                    ? static_cast<std::int64_t>(item.packedIntPayload().value())
                    : item.intPayload().value();
                return itemValue == element.intValue();
            }
            case TupleItemKind::FP_NUMERIC64:
                return item.doublePayload().value() == element.doubleValue();
            case TupleItemKind::STRING:
                return item.stringPayload().toString() == element.stringValue();
            case TupleItemKind::BINARY:
                return item.bytesPayload().size() == element.bytesSize()
                    && bytesEqual(item.bytesPayload().begin(), element.bytesData(), element.bytesSize());
            case TupleItemKind::PTIME64:
            case TupleItemKind::DATE:
            case TupleItemKind::DATETIME:
            case TupleItemKind::DATETIME_TZ:
            case TupleItemKind::TIME:
            case TupleItemKind::TIME_TZ:
            case TupleItemKind::DECIMAL:
                return item.uint64Payload().value() == element.uint64Value();
            default:
                THROWF(db0::InternalException) << "Unsupported set item kind";
        }
        return false;
    }

    bool o_set::bytesEqual(const std::byte *lhs, const std::byte *rhs, std::size_t size)
    {
        return size == 0 || std::memcmp(lhs, rhs, size) == 0;
    }

    std::size_t o_set::measureElements(const ElementSet &elements)
    {
        std::size_t size = 0;
        for (const auto &element: elements) {
            auto itemSize = Item::measure(element);
            if (size + itemSize < size) {
                THROWF(db0::InternalException) << "Set elements byte size overflow";
            }
            size += itemSize;
        }
        checkedHashIndexOffset(size == 0 ? 0 : size - 1, "Set item block");
        return size;
    }

    std::size_t o_set::measureCollisionBuckets(const ElementSet &elements, std::size_t capacity)
    {
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
        for (const auto &element: elements) {
            auto &bucket = buckets[elementHash(element) % capacity];
            auto itemSize = checkedUint32Size(Item::measure(element), "Set bucket item byte size");
            auto growth = o_compact_tuple::Builder::measureGrowth(bucket.m_count, bucket.m_elementsByteSize, itemSize);
            if (size + growth < size) {
                THROWF(db0::InternalException) << "Set bucket block byte size overflow";
            }
            size += growth;
            ++bucket.m_count;
            if (bucket.m_elementsByteSize + itemSize < bucket.m_elementsByteSize) {
                THROWF(db0::InternalException) << "Set bucket elements byte size overflow";
            }
            bucket.m_elementsByteSize += itemSize;
        }
        checkedHashIndexOffset(size == 0 ? 0 : size - 1, "Set bucket block");
        return size;
    }

    o_set::Element o_set::elementFromItem(const Item &item)
    {
        switch (item.itemKind()) {
            case TupleItemKind::NONE:
                return Element::none();
            case TupleItemKind::BOOLEAN:
                return Element::boolean(item.boolPayload().value());
            case TupleItemKind::INT64:
                return Element::integer(item.intPayload().value());
            case TupleItemKind::PACKED_INT64:
                return Element::integer(static_cast<std::int64_t>(item.packedIntPayload().value()));
            case TupleItemKind::FP_NUMERIC64:
                return Element::floating(item.doublePayload().value());
            case TupleItemKind::STRING: {
                auto str = item.stringPayload().get();
                return Element::string(std::string_view(str.get_raw(), str.size()));
            }
            case TupleItemKind::BINARY:
                return Element::bytes(item.bytesPayload().begin(), item.bytesPayload().size());
            case TupleItemKind::PTIME64:
                return Element::timestamp(item.uint64Payload().value());
            case TupleItemKind::DATE:
                return Element::date(item.uint64Payload().value());
            case TupleItemKind::DATETIME:
                return Element::datetime(item.uint64Payload().value());
            case TupleItemKind::DATETIME_TZ:
                return Element::datetimeTz(item.uint64Payload().value());
            case TupleItemKind::TIME:
                return Element::time(item.uint64Payload().value());
            case TupleItemKind::TIME_TZ:
                return Element::timeTz(item.uint64Payload().value());
            case TupleItemKind::DECIMAL:
                return Element::decimal(item.uint64Payload().value());
            default:
                THROWF(db0::InternalException) << "Unsupported set item kind";
        }
        return Element::none();
    }

    std::uint32_t o_set::elementHash(const Element &element)
    {
        auto seedKind = element.m_kind == TupleItemKind::PACKED_INT64 ? TupleItemKind::INT64 : element.m_kind;
        auto seed = 0x9e3779b9U ^ static_cast<std::uint32_t>(seedKind);
        switch (element.m_kind) {
        case TupleItemKind::NONE:
            return hashBytes(nullptr, 0, seed);
        case TupleItemKind::BOOLEAN:
            return hashBytes(&element.m_payload.m_bool_value, sizeof(element.m_payload.m_bool_value), seed);
        case TupleItemKind::INT64:
        case TupleItemKind::PACKED_INT64:
            return hashBytes(&element.m_payload.m_int_value, sizeof(element.m_payload.m_int_value), seed);
        case TupleItemKind::FP_NUMERIC64:
            return hashBytes(&element.m_payload.m_double_value, sizeof(element.m_payload.m_double_value), seed);
        case TupleItemKind::STRING:
            return hashBytes(
                element.m_payload.m_string_value.data(), element.m_payload.m_string_value.size(), seed
            );
        case TupleItemKind::BINARY:
            return hashBytes(element.bytesData(), element.bytesSize(), seed);
        case TupleItemKind::PTIME64:
        case TupleItemKind::DATE:
        case TupleItemKind::DATETIME:
        case TupleItemKind::DATETIME_TZ:
        case TupleItemKind::TIME:
        case TupleItemKind::TIME_TZ:
        case TupleItemKind::DECIMAL:
            return hashBytes(&element.m_payload.m_uint64_value, sizeof(element.m_payload.m_uint64_value), seed);
        default:
            THROWF(db0::InternalException) << "Unsupported set item kind";
        }
        return 0;
    }

    std::uint32_t o_set::itemHash(const Item &item)
    {
        auto seedKind = item.itemKind() == TupleItemKind::PACKED_INT64 ? TupleItemKind::INT64 : item.itemKind();
        auto seed = 0x9e3779b9U ^ static_cast<std::uint32_t>(seedKind);
        switch (item.itemKind()) {
        case TupleItemKind::NONE:
            return hashBytes(nullptr, 0, seed);
        case TupleItemKind::BOOLEAN: {
            auto value = item.boolPayload().value();
            return hashBytes(&value, sizeof(value), seed);
        }
        case TupleItemKind::INT64: {
            auto value = item.intPayload().value();
            return hashBytes(&value, sizeof(value), seed);
        }
        case TupleItemKind::PACKED_INT64: {
            auto value = static_cast<std::int64_t>(item.packedIntPayload().value());
            return hashBytes(&value, sizeof(value), seed);
        }
        case TupleItemKind::FP_NUMERIC64: {
            auto value = item.doublePayload().value();
            return hashBytes(&value, sizeof(value), seed);
        }
        case TupleItemKind::STRING: {
            auto str = item.stringPayload().get();
            return hashBytes(str.get_raw(), str.size(), seed);
        }
        case TupleItemKind::BINARY:
            return hashBytes(item.bytesPayload().begin(), item.bytesPayload().size(), seed);
        case TupleItemKind::PTIME64:
        case TupleItemKind::DATE:
        case TupleItemKind::DATETIME:
        case TupleItemKind::DATETIME_TZ:
        case TupleItemKind::TIME:
        case TupleItemKind::TIME_TZ:
        case TupleItemKind::DECIMAL: {
            auto value = item.uint64Payload().value();
            return hashBytes(&value, sizeof(value), seed);
        }
        default:
            THROWF(db0::InternalException) << "Unsupported set item kind";
        }
        return 0;
    }

    std::uint32_t o_set::hashBytes(const void *data, std::size_t size, std::uint32_t seed)
    {
        static const std::byte empty = std::byte{0};
        auto hash = db0::murmurhash64A(size == 0 ? &empty : data, size, seed);
        return static_cast<std::uint32_t>(hash ^ (hash >> 32));
    }

    std::size_t o_set::hashIndexCapacity(std::size_t count)
    {
        if (count == 0) {
            return 0;
        }

        std::size_t capacity = 1;
        while (capacity < count * 2) {
            capacity <<= 1;
        }
        return capacity;
    }

    std::size_t o_set::hashIndexByteSize(std::size_t count)
    {
        return hashIndexCapacity(count) * sizeof(HashIndexEntry);
    }

    std::uint32_t o_set::buildHashIndex(
        HashIndexEntry *indexEntries, const std::byte *itemsBegin, std::size_t itemsByteSize, std::size_t capacity
    )
    {
        for (std::size_t i = 0; i < capacity; ++i) {
            indexEntries[i].clear();
        }

        if (capacity == 0) {
            return 0;
        }

        auto *cursor = itemsBegin;
        auto *itemsEnd = itemsBegin + itemsByteSize;
        while (cursor < itemsEnd) {
            const auto &item = Item::__const_ref(cursor);
            auto itemOffset = checkedHashIndexOffset(cursor - itemsBegin, "Set item");
            auto slot = itemHash(item) % capacity;
            auto &entry = indexEntries[slot];
            if (entry.isEmpty()) {
                entry.setItem(itemOffset);
            } else if (!entry.isPendingBucket()) {
                entry.setPendingBucket();
            }
            cursor += item.sizeOf();
        }
        return checkedHashIndexOffset(itemsByteSize == 0 ? 0 : itemsByteSize - 1, "Set item block");
    }

    std::uint32_t o_set::writeCollisionBuckets(
        HashIndexEntry *indexEntries, const std::byte *itemsBegin, std::size_t itemsByteSize,
        std::size_t capacity, std::byte *bucketStart
    )
    {
        buildHashIndex(indexEntries, itemsBegin, itemsByteSize, capacity);

        auto *bucketCursor = bucketStart;
        auto *itemsEnd = itemsBegin + itemsByteSize;
        for (std::size_t slot = 0; slot < capacity; ++slot) {
            if (!indexEntries[slot].isPendingBucket()) {
                continue;
            }

            std::uint32_t count = 0;
            std::size_t elementsByteSize = 0;
            auto *cursor = itemsBegin;
            while (cursor < itemsEnd) {
                const auto &item = Item::__const_ref(cursor);
                if (itemHash(item) % capacity == slot) {
                    ++count;
                    auto itemSize = item.sizeOf();
                    if (elementsByteSize + itemSize < elementsByteSize) {
                        THROWF(db0::InternalException) << "Set bucket elements byte size overflow";
                    }
                    elementsByteSize += itemSize;
                }
                cursor += item.sizeOf();
            }

            indexEntries[slot].setBucket(checkedHashIndexOffset(bucketCursor - bucketStart, "Set bucket"));
            o_compact_tuple::Builder tupleBuilder(
                bucketCursor, count, checkedUint32Size(elementsByteSize, "Set bucket elements byte size")
            );
            cursor = itemsBegin;
            while (cursor < itemsEnd) {
                const auto &item = Item::__const_ref(cursor);
                if (itemHash(item) % capacity == slot) {
                    tupleBuilder.add(elementFromItem(item));
                }
                cursor += item.sizeOf();
            }
            auto &tuple = tupleBuilder.finish();
            bucketCursor += tuple.sizeOf();
        }

        return checkedHashIndexOffset(bucketCursor - bucketStart, "Set bucket block");
    }

    std::uint32_t o_set::checkedHashIndexOffset(std::size_t offset, const char *name)
    {
        if (offset >= HashIndexEntry::OFFSET_MASK) {
            THROWF(db0::InternalException) << name << " offset exceeds hash index entry capacity";
        }
        return static_cast<std::uint32_t>(offset);
    }

    std::uint32_t o_set::checkedUint32Size(std::size_t size, const char *name)
    {
        if (size > std::numeric_limits<std::uint32_t>::max()) {
            THROWF(db0::InternalException) << name << " exceeds uint32 range";
        }
        return static_cast<std::uint32_t>(size);
    }

}
