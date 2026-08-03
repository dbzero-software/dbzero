// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "o_dict.hpp"

#include <cstring>
#include <limits>
#include <vector>

#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/utils/hash_func.hpp>

namespace db0::object_model
{
    o_dict_pair::o_dict_pair(const Element &key, const Element &value)
    {
        arrangeMembers()
            (o_tuple_item::type(), key)
            (o_tuple_item::type(), value);
    }

    const o_tuple_item &o_dict_pair::key() const
    {
        return getDynFirst(o_tuple_item::type());
    }

    const o_tuple_item &o_dict_pair::value() const
    {
        return getDynAfter(key(), o_tuple_item::type());
    }

    std::size_t o_dict_pair::sizeOf() const
    {
        return safeSizeOf(reinterpret_cast<const std::byte *>(this));
    }

    std::size_t o_dict_pair::measure(const Element &key, const Element &value)
    {
        return measureMembers()
            (o_tuple_item::type(), key)
            (o_tuple_item::type(), value);
    }

    o_dict_bucket::o_dict_bucket(const std::vector<Element> &keys, const std::vector<Element> &values)
    {
        if (keys.size() != values.size()) {
            THROWF(db0::InternalException) << "Dict bucket key/value count mismatch";
        }
        arrangeMembers()
            (o_compact_tuple::type(), keys)
            (o_compact_tuple::type(), values);
    }

    const o_compact_tuple &o_dict_bucket::keys() const
    {
        return getDynFirst(o_compact_tuple::type());
    }

    const o_compact_tuple &o_dict_bucket::values() const
    {
        return getDynAfter(keys(), o_compact_tuple::type());
    }

    std::size_t o_dict_bucket::sizeOf() const
    {
        return safeSizeOf(reinterpret_cast<const std::byte *>(this));
    }

    std::size_t o_dict_bucket::measure(const std::vector<Element> &keys, const std::vector<Element> &values)
    {
        if (keys.size() != values.size()) {
            THROWF(db0::InternalException) << "Dict bucket key/value count mismatch";
        }
        return measureMembers()
            (o_compact_tuple::type(), keys)
            (o_compact_tuple::type(), values);
    }

    std::size_t o_dict_bucket::measureForBytes(
        std::uint32_t count, std::uint32_t keysByteSize, std::uint32_t valuesByteSize
    )
    {
        return o_compact_tuple::Builder::measure(count, keysByteSize)
            + o_compact_tuple::Builder::measure(count, valuesByteSize);
    }

    std::size_t o_dict_bucket::measureGrowth(
        std::uint32_t count, std::uint32_t keysByteSize, std::uint32_t valuesByteSize,
        std::uint32_t addedKeyByteSize, std::uint32_t addedValueByteSize
    )
    {
        auto newCount = count + 1;
        auto newKeysByteSize = keysByteSize + addedKeyByteSize;
        auto newValuesByteSize = valuesByteSize + addedValueByteSize;
        if (newCount <= count || newKeysByteSize < keysByteSize || newValuesByteSize < valuesByteSize) {
            THROWF(db0::InternalException) << "Dict bucket growth exceeds uint32 range";
        }
        if (count == 0) {
            return 0;
        }
        auto newSize = measureForBytes(newCount, newKeysByteSize, newValuesByteSize);
        if (count == 1) {
            return newSize;
        }
        return newSize - measureForBytes(count, keysByteSize, valuesByteSize);
    }

    bool o_dict::HashIndexEntry::isEmpty() const
    {
        return m_value == 0;
    }

    bool o_dict::HashIndexEntry::isBucket() const
    {
        return (m_value & BUCKET_FLAG) != 0;
    }

    bool o_dict::HashIndexEntry::isPendingBucket() const
    {
        return m_value == BUCKET_FLAG;
    }

    std::uint32_t o_dict::HashIndexEntry::offset() const
    {
        return (m_value & OFFSET_MASK) - 1;
    }

    void o_dict::HashIndexEntry::clear()
    {
        m_value = 0;
    }

    void o_dict::HashIndexEntry::setPendingBucket()
    {
        m_value = BUCKET_FLAG;
    }

    void o_dict::HashIndexEntry::setPair(std::uint32_t offset)
    {
        if (offset >= OFFSET_MASK) {
            THROWF(db0::InternalException) << "Dict pair offset exceeds hash index entry capacity";
        }
        m_value = offset + 1;
    }

    void o_dict::HashIndexEntry::setBucket(std::uint32_t offset)
    {
        if (offset >= OFFSET_MASK) {
            THROWF(db0::InternalException) << "Dict bucket offset exceeds hash index entry capacity";
        }
        m_value = BUCKET_FLAG | (offset + 1);
    }

    const o_dict::Pair &o_dict::const_iterator::operator*() const
    {
        return *m_pair;
    }

    const o_dict::Pair *o_dict::const_iterator::operator->() const
    {
        return m_pair;
    }

    o_dict::const_iterator &o_dict::const_iterator::operator++()
    {
        m_pair = reinterpret_cast<const Pair *>(
            reinterpret_cast<const std::byte *>(m_pair) + m_pair->sizeOf()
        );
        return *this;
    }

    bool o_dict::const_iterator::operator==(const const_iterator &other) const
    {
        return m_pair == other.m_pair;
    }

    bool o_dict::const_iterator::operator!=(const const_iterator &other) const
    {
        return m_pair != other.m_pair;
    }

    o_dict::const_iterator::const_iterator(const Pair *pair)
        : m_pair(pair)
    {
    }

    std::size_t o_dict::ElementHash::operator()(const Element &element) const
    {
        return elementHash(element);
    }

    bool o_dict::ElementEqual::operator()(const Element &lhs, const Element &rhs) const
    {
        return elementsEqual(lhs, rhs);
    }

    o_dict::o_dict(const ElementMap &elements)
    {
        auto pairsSize = checkedUint32Size(measurePairs(elements), "Dict pairs byte size");
        auto capacity = hashIndexCapacity(elements.size());
        auto bucketSize = checkedUint32Size(
            measureCollisionBuckets(elements, capacity), "Dict bucket byte size"
        );

        auto arranger = arrangeDictMembers(static_cast<std::uint32_t>(elements.size()), pairsSize, bucketSize);
        for (const auto &[key, value]: elements) {
            arranger = arranger(Pair::type(), key, value);
        }

        finishDictConstruction(arranger.ptr(), pairsSize, capacity, bucketSize);
    }

    db0::Foundation::Arranger o_dict::arrangeDictMembers(
        std::uint32_t count, std::uint32_t pairsByteSize, std::uint32_t bucketByteSize
    )
    {
        return arrangeMembers()
            (db0::packed_int32::type(), count)
            (db0::packed_int32::type(), pairsByteSize)
            (db0::packed_int32::type(), bucketByteSize);
    }

    void o_dict::finishDictConstruction(
        void *indexEntriesPtr, std::uint32_t pairsByteSize, std::size_t capacity, std::uint32_t bucketByteSize
    )
    {
        auto *indexEntries = reinterpret_cast<HashIndexEntry *>(indexEntriesPtr);
        auto bucketByteSizeWritten = writeCollisionBuckets(
            indexEntries, beginOfPairs(), pairsByteSize, capacity, reinterpret_cast<std::byte *>(indexEntries + capacity)
        );
        if (bucketByteSizeWritten != bucketByteSize) {
            THROWF(db0::InternalException) << "Dict bucket byte size changed during construction";
        }
    }

    std::size_t o_dict::size() const
    {
        return count().value();
    }

    std::size_t o_dict::pairsByteSize() const
    {
        return pairsByteSizeMember().value();
    }

    bool o_dict::empty() const
    {
        return size() == 0;
    }

    bool o_dict::contains(const Element &key) const
    {
        return get(key) != nullptr;
    }

    const o_dict::Item *o_dict::get(const Element &key) const
    {
        auto capacity = hashIndexCapacity(size());
        if (capacity == 0) {
            return nullptr;
        }

        const auto *entries = beginOfHashIndex();
        auto slot = elementHash(key) % capacity;
        const auto &entry = entries[slot];
        if (entry.isEmpty()) {
            return nullptr;
        }

        auto offset = entry.offset();
        if (!entry.isBucket()) {
            const auto &pair = pairAtOffset(offset);
            return itemEqualsElement(pair.key(), key) ? &pair.value() : nullptr;
        }

        const auto &bucket = bucketAtOffset(offset);
        auto keyIt = bucket.keys().begin();
        auto valueIt = bucket.values().begin();
        for (; keyIt != bucket.keys().end(); ++keyIt, ++valueIt) {
            if (itemEqualsElement(*keyIt, key)) {
                return &*valueIt;
            }
        }
        return nullptr;
    }

    const o_dict::Item *o_dict::get(const Item &key) const
    {
        auto capacity = hashIndexCapacity(size());
        if (capacity == 0) {
            return nullptr;
        }

        auto element = elementFromItem(key);
        const auto *entries = beginOfHashIndex();
        auto slot = itemHash(key) % capacity;
        const auto &entry = entries[slot];
        if (entry.isEmpty()) {
            return nullptr;
        }

        auto offset = entry.offset();
        if (!entry.isBucket()) {
            const auto &pair = pairAtOffset(offset);
            return itemEqualsElement(pair.key(), element) ? &pair.value() : nullptr;
        }

        const auto &bucket = bucketAtOffset(offset);
        auto keyIt = bucket.keys().begin();
        auto valueIt = bucket.values().begin();
        for (; keyIt != bucket.keys().end(); ++keyIt, ++valueIt) {
            if (itemEqualsElement(*keyIt, element)) {
                return &*valueIt;
            }
        }
        return nullptr;
    }

    o_dict::const_iterator o_dict::begin() const
    {
        return const_iterator(reinterpret_cast<const Pair *>(beginOfPairs()));
    }

    o_dict::const_iterator o_dict::end() const
    {
        return const_iterator(reinterpret_cast<const Pair *>(beginOfPairs() + pairsByteSize()));
    }

    std::size_t o_dict::sizeOf() const
    {
        return safeSizeOf(reinterpret_cast<const std::byte *>(this));
    }

    std::size_t o_dict::measure(const ElementMap &elements)
    {
        auto pairsSize = measurePairs(elements);
        auto bucketSize = measureCollisionBuckets(elements, hashIndexCapacity(elements.size()));
        return measureMembers()
            (db0::packed_int32::type(), static_cast<std::uint32_t>(elements.size()))
            (db0::packed_int32::type(), checkedUint32Size(pairsSize, "Dict pairs byte size"))
            (db0::packed_int32::type(), checkedUint32Size(bucketSize, "Dict bucket byte size"))
            (pairsSize)
            (hashIndexByteSize(elements.size()))
            (bucketSize);
    }

    const db0::packed_int32 &o_dict::count() const
    {
        return getDynFirst(db0::packed_int32::type());
    }

    const db0::packed_int32 &o_dict::pairsByteSizeMember() const
    {
        return getDynAfter(count(), db0::packed_int32::type());
    }

    const db0::packed_int32 &o_dict::bucketByteSizeMember() const
    {
        return getDynAfter(pairsByteSizeMember(), db0::packed_int32::type());
    }

    const std::byte *o_dict::beginOfPairs() const
    {
        const auto &bucketByteSizeMemberRef = bucketByteSizeMember();
        return reinterpret_cast<const std::byte *>(&bucketByteSizeMemberRef) + bucketByteSizeMemberRef.sizeOf();
    }

    o_dict::HashIndexEntry *o_dict::beginOfHashIndex()
    {
        return reinterpret_cast<HashIndexEntry *>(
            const_cast<std::byte *>(static_cast<const o_dict *>(this)->beginOfPairs()) + pairsByteSize()
        );
    }

    const o_dict::HashIndexEntry *o_dict::beginOfHashIndex() const
    {
        return reinterpret_cast<const HashIndexEntry *>(beginOfPairs() + pairsByteSize());
    }

    const std::byte *o_dict::beginOfBuckets() const
    {
        return reinterpret_cast<const std::byte *>(beginOfHashIndex() + hashIndexCapacity(size()));
    }

    const o_dict_bucket &o_dict::bucketAtOffset(std::uint32_t offset) const
    {
        return o_dict_bucket::__const_ref(beginOfBuckets() + offset);
    }

    const o_dict::Pair &o_dict::pairAtOffset(std::uint32_t offset) const
    {
        return Pair::__const_ref(beginOfPairs() + offset);
    }

    bool o_dict::elementsEqual(const Element &lhs, const Element &rhs)
    {
        auto lhsIsInt = lhs.m_kind == StorageClass::INT64 || lhs.m_kind == StorageClass::PACKED_INT32;
        auto rhsIsInt = rhs.m_kind == StorageClass::INT64 || rhs.m_kind == StorageClass::PACKED_INT32;
        auto lhsIsString = lhs.m_kind == StorageClass::STRING_REF || lhs.m_kind == StorageClass::EMBEDDED_STRING;
        auto rhsIsString = rhs.m_kind == StorageClass::STRING_REF || rhs.m_kind == StorageClass::EMBEDDED_STRING;
        auto lhsIsBytes = lhs.m_kind == StorageClass::DB0_BYTES || lhs.m_kind == StorageClass::EMBEDDED_BYTES;
        auto rhsIsBytes = rhs.m_kind == StorageClass::DB0_BYTES || rhs.m_kind == StorageClass::EMBEDDED_BYTES;
        if (lhs.m_kind != rhs.m_kind && !(lhsIsInt && rhsIsInt) && !(lhsIsString && rhsIsString)
            && !(lhsIsBytes && rhsIsBytes)) {
            return false;
        }

        switch (lhs.m_kind) {
        case StorageClass::NONE:
            return true;
        case StorageClass::BOOLEAN:
            return lhs.boolValue() == rhs.boolValue();
        case StorageClass::INT64:
        case StorageClass::PACKED_INT32:
            return lhs.intValue() == rhs.intValue();
        case StorageClass::FP_NUMERIC64:
            return lhs.doubleValue() == rhs.doubleValue();
        case StorageClass::STRING_REF:
        case StorageClass::EMBEDDED_STRING:
            return lhs.m_payload.m_string_value == rhs.m_payload.m_string_value;
        case StorageClass::DB0_BYTES:
        case StorageClass::EMBEDDED_BYTES:
            return lhs.bytesSize() == rhs.bytesSize() && bytesEqual(lhs.bytesData(), rhs.bytesData(), lhs.bytesSize());
        case StorageClass::EMBEDDED_TUPLE:
        case StorageClass::EMBEDDED_SET:
        case StorageClass::EMBEDDED_DICT:
        case StorageClass::EMBEDDED_OBJECT:
            return lhs.bytesSize() == rhs.bytesSize() && bytesEqual(lhs.bytesData(), rhs.bytesData(), lhs.bytesSize());
        case StorageClass::PTIME64:
        case StorageClass::DATE:
        case StorageClass::DATETIME:
        case StorageClass::DATETIME_TZ:
        case StorageClass::TIME:
        case StorageClass::TIME_TZ:
        case StorageClass::DECIMAL:
            return lhs.uint64Value() == rhs.uint64Value();
        default:
            THROWF(db0::InternalException) << "Unsupported dict item kind";
        }
        return false;
    }

    bool o_dict::itemEqualsElement(const Item &item, const Element &element)
    {
        auto itemIsInt = item.itemKind() == StorageClass::INT64 || item.itemKind() == StorageClass::PACKED_INT32;
        auto elementIsInt = element.m_kind == StorageClass::INT64 || element.m_kind == StorageClass::PACKED_INT32;
        auto itemIsString = item.itemKind() == StorageClass::STRING_REF
            || item.itemKind() == StorageClass::EMBEDDED_STRING;
        auto elementIsString = element.m_kind == StorageClass::STRING_REF
            || element.m_kind == StorageClass::EMBEDDED_STRING;
        auto itemIsBytes = item.itemKind() == StorageClass::DB0_BYTES
            || item.itemKind() == StorageClass::EMBEDDED_BYTES;
        auto elementIsBytes = element.m_kind == StorageClass::DB0_BYTES
            || element.m_kind == StorageClass::EMBEDDED_BYTES;
        if (item.itemKind() != element.m_kind && !(itemIsInt && elementIsInt)
            && !(itemIsString && elementIsString) && !(itemIsBytes && elementIsBytes)) {
            return false;
        }

        switch (element.m_kind) {
            case StorageClass::NONE:
                return true;
            case StorageClass::BOOLEAN:
                return item.boolPayload().value() == element.boolValue();
            case StorageClass::INT64:
            case StorageClass::PACKED_INT32: {
                auto itemValue = item.itemKind() == StorageClass::PACKED_INT32
                    ? static_cast<std::int64_t>(item.packedIntPayload().value())
                    : item.intPayload().value();
                return itemValue == element.intValue();
            }
            case StorageClass::FP_NUMERIC64:
                return item.doublePayload().value() == element.doubleValue();
            case StorageClass::STRING_REF:
            case StorageClass::EMBEDDED_STRING:
                return item.stringPayload().toString() == element.stringValue();
            case StorageClass::DB0_BYTES:
            case StorageClass::EMBEDDED_BYTES:
                return item.bytesPayload().size() == element.bytesSize()
                    && bytesEqual(item.bytesPayload().begin(), element.bytesData(), element.bytesSize());
            case StorageClass::EMBEDDED_TUPLE:
            case StorageClass::EMBEDDED_SET:
            case StorageClass::EMBEDDED_DICT:
            case StorageClass::EMBEDDED_OBJECT:
                return item.embeddedPayload().size() == element.bytesSize()
                    && bytesEqual(item.embeddedPayload().begin(), element.bytesData(), element.bytesSize());
            case StorageClass::PTIME64:
            case StorageClass::DATE:
            case StorageClass::DATETIME:
            case StorageClass::DATETIME_TZ:
            case StorageClass::TIME:
            case StorageClass::TIME_TZ:
            case StorageClass::DECIMAL:
                return item.uint64Payload().value() == element.uint64Value();
            default:
                THROWF(db0::InternalException) << "Unsupported dict item kind";
        }
        return false;
    }

    bool o_dict::bytesEqual(const std::byte *lhs, const std::byte *rhs, std::size_t size)
    {
        return size == 0 || std::memcmp(lhs, rhs, size) == 0;
    }

    std::size_t o_dict::measurePairs(const ElementMap &elements)
    {
        std::size_t size = 0;
        for (const auto &[key, value]: elements) {
            auto pairSize = Pair::measure(key, value);
            if (size + pairSize < size) {
                THROWF(db0::InternalException) << "Dict pairs byte size overflow";
            }
            size += pairSize;
        }
        checkedHashIndexOffset(size == 0 ? 0 : size - 1, "Dict pair block");
        return size;
    }

    std::size_t o_dict::measureCollisionBuckets(const ElementMap &elements, std::size_t capacity)
    {
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
        for (const auto &[key, value]: elements) {
            auto &bucket = buckets[elementHash(key) % capacity];
            auto keySize = checkedUint32Size(Item::measure(key), "Dict bucket key byte size");
            auto valueSize = checkedUint32Size(Item::measure(value), "Dict bucket value byte size");
            auto growth = o_dict_bucket::measureGrowth(
                bucket.m_count, bucket.m_keysByteSize, bucket.m_valuesByteSize, keySize, valueSize
            );
            if (size + growth < size) {
                THROWF(db0::InternalException) << "Dict bucket block byte size overflow";
            }
            size += growth;
            ++bucket.m_count;
            if (bucket.m_keysByteSize + keySize < bucket.m_keysByteSize
                || bucket.m_valuesByteSize + valueSize < bucket.m_valuesByteSize) {
                THROWF(db0::InternalException) << "Dict bucket elements byte size overflow";
            }
            bucket.m_keysByteSize += keySize;
            bucket.m_valuesByteSize += valueSize;
        }
        checkedHashIndexOffset(size == 0 ? 0 : size - 1, "Dict bucket block");
        return size;
    }

    o_dict::Element o_dict::elementFromItem(const Item &item)
    {
        switch (item.itemKind()) {
            case StorageClass::NONE:
                return Element::none();
            case StorageClass::BOOLEAN:
                return Element::boolean(item.boolPayload().value());
            case StorageClass::INT64:
                return Element::integer(item.intPayload().value());
            case StorageClass::PACKED_INT32:
                return Element::integer(static_cast<std::int64_t>(item.packedIntPayload().value()));
            case StorageClass::FP_NUMERIC64:
                return Element::floating(item.doublePayload().value());
            case StorageClass::STRING_REF:
            case StorageClass::EMBEDDED_STRING: {
                auto str = item.stringPayload().get();
                return Element::string(std::string_view(str.get_raw(), str.size()));
            }
            case StorageClass::DB0_BYTES:
            case StorageClass::EMBEDDED_BYTES:
                return Element::bytes(item.bytesPayload().begin(), item.bytesPayload().size());
            case StorageClass::EMBEDDED_TUPLE:
                return Element::embeddedTuple(item.embeddedPayload().begin(), item.embeddedPayload().size());
            case StorageClass::EMBEDDED_SET:
                return Element::embeddedSet(item.embeddedPayload().begin(), item.embeddedPayload().size());
            case StorageClass::EMBEDDED_DICT:
                return Element::embeddedDict(item.embeddedPayload().begin(), item.embeddedPayload().size());
            case StorageClass::EMBEDDED_OBJECT:
                return Element::embeddedObject(item.embeddedPayload().begin(), item.embeddedPayload().size());
            case StorageClass::PTIME64:
                return Element::timestamp(item.uint64Payload().value());
            case StorageClass::DATE:
                return Element::date(item.uint64Payload().value());
            case StorageClass::DATETIME:
                return Element::datetime(item.uint64Payload().value());
            case StorageClass::DATETIME_TZ:
                return Element::datetimeTz(item.uint64Payload().value());
            case StorageClass::TIME:
                return Element::time(item.uint64Payload().value());
            case StorageClass::TIME_TZ:
                return Element::timeTz(item.uint64Payload().value());
            case StorageClass::DECIMAL:
                return Element::decimal(item.uint64Payload().value());
            default:
                THROWF(db0::InternalException) << "Unsupported dict item kind";
        }
        return Element::none();
    }

    std::uint32_t o_dict::elementHash(const Element &element)
    {
        auto seedKind = element.m_kind == StorageClass::PACKED_INT32 ? StorageClass::INT64 : element.m_kind;
        seedKind = seedKind == StorageClass::EMBEDDED_STRING ? StorageClass::STRING_REF : seedKind;
        seedKind = seedKind == StorageClass::EMBEDDED_BYTES ? StorageClass::DB0_BYTES : seedKind;
        auto seed = 0x9e3779b9U ^ static_cast<std::uint32_t>(seedKind);
        switch (element.m_kind) {
        case StorageClass::NONE:
            return hashBytes(nullptr, 0, seed);
        case StorageClass::BOOLEAN:
            return hashBytes(&element.m_payload.m_bool_value, sizeof(element.m_payload.m_bool_value), seed);
        case StorageClass::INT64:
        case StorageClass::PACKED_INT32:
            return hashBytes(&element.m_payload.m_int_value, sizeof(element.m_payload.m_int_value), seed);
        case StorageClass::FP_NUMERIC64:
            return hashBytes(&element.m_payload.m_double_value, sizeof(element.m_payload.m_double_value), seed);
        case StorageClass::STRING_REF:
        case StorageClass::EMBEDDED_STRING:
            return hashBytes(
                element.m_payload.m_string_value.data(), element.m_payload.m_string_value.size(), seed
            );
        case StorageClass::DB0_BYTES:
        case StorageClass::EMBEDDED_BYTES:
            return hashBytes(element.bytesData(), element.bytesSize(), seed);
        case StorageClass::EMBEDDED_TUPLE:
        case StorageClass::EMBEDDED_SET:
        case StorageClass::EMBEDDED_DICT:
        case StorageClass::EMBEDDED_OBJECT: {
            if (element.m_payload.m_bytes_value.m_writer) {
                std::vector<std::byte> payload(element.bytesSize());
                element.m_payload.m_bytes_value.m_writer(
                    payload.data(), element.m_payload.m_bytes_value.m_source,
                    element.m_payload.m_bytes_value.m_context
                );
                return hashBytes(payload.data(), payload.size(), seed);
            }
            return hashBytes(element.bytesData(), element.bytesSize(), seed);
        }
        case StorageClass::PTIME64:
        case StorageClass::DATE:
        case StorageClass::DATETIME:
        case StorageClass::DATETIME_TZ:
        case StorageClass::TIME:
        case StorageClass::TIME_TZ:
        case StorageClass::DECIMAL:
            return hashBytes(&element.m_payload.m_uint64_value, sizeof(element.m_payload.m_uint64_value), seed);
        default:
            THROWF(db0::InternalException) << "Unsupported dict item kind";
        }
        return 0;
    }

    std::uint32_t o_dict::itemHash(const Item &item)
    {
        auto seedKind = item.itemKind() == StorageClass::PACKED_INT32 ? StorageClass::INT64 : item.itemKind();
        seedKind = seedKind == StorageClass::EMBEDDED_STRING ? StorageClass::STRING_REF : seedKind;
        seedKind = seedKind == StorageClass::EMBEDDED_BYTES ? StorageClass::DB0_BYTES : seedKind;
        auto seed = 0x9e3779b9U ^ static_cast<std::uint32_t>(seedKind);
        switch (item.itemKind()) {
        case StorageClass::NONE:
            return hashBytes(nullptr, 0, seed);
        case StorageClass::BOOLEAN: {
            auto value = item.boolPayload().value();
            return hashBytes(&value, sizeof(value), seed);
        }
        case StorageClass::INT64: {
            auto value = item.intPayload().value();
            return hashBytes(&value, sizeof(value), seed);
        }
        case StorageClass::PACKED_INT32: {
            auto value = static_cast<std::int64_t>(item.packedIntPayload().value());
            return hashBytes(&value, sizeof(value), seed);
        }
        case StorageClass::FP_NUMERIC64: {
            auto value = item.doublePayload().value();
            return hashBytes(&value, sizeof(value), seed);
        }
        case StorageClass::STRING_REF:
        case StorageClass::EMBEDDED_STRING: {
            auto str = item.stringPayload().get();
            return hashBytes(str.get_raw(), str.size(), seed);
        }
        case StorageClass::DB0_BYTES:
        case StorageClass::EMBEDDED_BYTES:
            return hashBytes(item.bytesPayload().begin(), item.bytesPayload().size(), seed);
        case StorageClass::EMBEDDED_TUPLE:
        case StorageClass::EMBEDDED_SET:
        case StorageClass::EMBEDDED_DICT:
        case StorageClass::EMBEDDED_OBJECT:
            return hashBytes(item.embeddedPayload().begin(), item.embeddedPayload().size(), seed);
        case StorageClass::PTIME64:
        case StorageClass::DATE:
        case StorageClass::DATETIME:
        case StorageClass::DATETIME_TZ:
        case StorageClass::TIME:
        case StorageClass::TIME_TZ:
        case StorageClass::DECIMAL: {
            auto value = item.uint64Payload().value();
            return hashBytes(&value, sizeof(value), seed);
        }
        default:
            THROWF(db0::InternalException) << "Unsupported dict item kind";
        }
        return 0;
    }

    std::uint32_t o_dict::hashBytes(const void *data, std::size_t size, std::uint32_t seed)
    {
        static const std::byte empty = std::byte{0};
        auto hash = db0::murmurhash64A(size == 0 ? &empty : data, size, seed);
        return static_cast<std::uint32_t>(hash ^ (hash >> 32));
    }

    std::size_t o_dict::hashIndexCapacity(std::size_t count)
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

    std::size_t o_dict::hashIndexByteSize(std::size_t count)
    {
        return hashIndexCapacity(count) * sizeof(HashIndexEntry);
    }

    std::uint32_t o_dict::buildHashIndex(
        HashIndexEntry *indexEntries, const std::byte *pairsBegin, std::size_t pairsByteSize, std::size_t capacity
    )
    {
        for (std::size_t i = 0; i < capacity; ++i) {
            indexEntries[i].clear();
        }

        if (capacity == 0) {
            return 0;
        }

        auto *cursor = pairsBegin;
        auto *pairsEnd = pairsBegin + pairsByteSize;
        while (cursor < pairsEnd) {
            const auto &pair = Pair::__const_ref(cursor);
            auto pairOffset = checkedHashIndexOffset(cursor - pairsBegin, "Dict pair");
            auto slot = itemHash(pair.key()) % capacity;
            auto &entry = indexEntries[slot];
            if (entry.isEmpty()) {
                entry.setPair(pairOffset);
            } else if (!entry.isPendingBucket()) {
                entry.setPendingBucket();
            }
            cursor += pair.sizeOf();
        }
        return checkedHashIndexOffset(pairsByteSize == 0 ? 0 : pairsByteSize - 1, "Dict pair block");
    }

    std::uint32_t o_dict::writeCollisionBuckets(
        HashIndexEntry *indexEntries, const std::byte *pairsBegin, std::size_t pairsByteSize,
        std::size_t capacity, std::byte *bucketStart
    )
    {
        buildHashIndex(indexEntries, pairsBegin, pairsByteSize, capacity);

        auto *bucketCursor = bucketStart;
        auto *pairsEnd = pairsBegin + pairsByteSize;
        for (std::size_t slot = 0; slot < capacity; ++slot) {
            if (!indexEntries[slot].isPendingBucket()) {
                continue;
            }

            std::vector<Element> keys;
            std::vector<Element> values;
            auto *cursor = pairsBegin;
            while (cursor < pairsEnd) {
                const auto &pair = Pair::__const_ref(cursor);
                if (itemHash(pair.key()) % capacity == slot) {
                    keys.push_back(elementFromItem(pair.key()));
                    values.push_back(elementFromItem(pair.value()));
                }
                cursor += pair.sizeOf();
            }

            indexEntries[slot].setBucket(checkedHashIndexOffset(bucketCursor - bucketStart, "Dict bucket"));
            auto &bucket = o_dict_bucket::__new(bucketCursor, keys, values);
            bucketCursor += bucket.sizeOf();
        }

        return checkedHashIndexOffset(bucketCursor - bucketStart, "Dict bucket block");
    }

    std::uint32_t o_dict::checkedHashIndexOffset(std::size_t offset, const char *name)
    {
        if (offset >= HashIndexEntry::OFFSET_MASK) {
            THROWF(db0::InternalException) << name << " offset exceeds hash index entry capacity";
        }
        return static_cast<std::uint32_t>(offset);
    }

    std::uint32_t o_dict::checkedUint32Size(std::size_t size, const char *name)
    {
        if (size > std::numeric_limits<std::uint32_t>::max()) {
            THROWF(db0::InternalException) << name << " exceeds uint32 range";
        }
        return static_cast<std::uint32_t>(size);
    }

}
