// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/packed_int.hpp>
#include <dbzero/object_model/tuple/o_tuple.hpp>

namespace db0::object_model
{

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_dict_pair: public db0::o_base<o_dict_pair, 0, false>
    {
    protected:
        using super_t = db0::o_base<o_dict_pair, 0, false>;
        friend super_t;

    public:
        using Element = o_tuple_item::Element;

        o_dict_pair(const Element &key, const Element &value);

        const o_tuple_item &key() const;
        const o_tuple_item &value() const;
        std::size_t sizeOf() const;

        static std::size_t measure(const Element &key, const Element &value);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            auto cursor = buf;
            cursor += super_t::baseSize();
            cursor += o_tuple_item::safeSizeOf(cursor);
            cursor += o_tuple_item::safeSizeOf(cursor);
            return cursor - start;
        }

    protected:
        o_dict_pair() = default;
    };
DB0_PACKED_END

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_dict_bucket: public db0::o_base<o_dict_bucket, 0, false>
    {
    protected:
        using super_t = db0::o_base<o_dict_bucket, 0, false>;
        friend super_t;

    public:
        using Element = o_tuple_item::Element;

        o_dict_bucket(const std::vector<Element> &keys, const std::vector<Element> &values);

        const o_compact_tuple &keys() const;
        const o_compact_tuple &values() const;
        std::size_t sizeOf() const;

        static std::size_t measure(const std::vector<Element> &keys, const std::vector<Element> &values);
        static std::size_t measureForBytes(
            std::uint32_t count, std::uint32_t keysByteSize, std::uint32_t valuesByteSize
        );
        static std::size_t measureGrowth(
            std::uint32_t count, std::uint32_t keysByteSize, std::uint32_t valuesByteSize,
            std::uint32_t addedKeyByteSize, std::uint32_t addedValueByteSize
        );

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            auto cursor = buf;
            cursor += super_t::baseSize();
            cursor += o_compact_tuple::safeSizeOf(cursor);
            cursor += o_compact_tuple::safeSizeOf(cursor);
            return cursor - start;
        }

    protected:
        o_dict_bucket() = default;
    };
DB0_PACKED_END

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_dict: public db0::o_base<o_dict, 0, false>
    {
    protected:
        using super_t = db0::o_base<o_dict, 0, false>;
        friend super_t;

    public:
        using Element = o_tuple_item::Element;
        using Pair = o_dict_pair;
        using Item = o_tuple_item;

        struct ElementHash
        {
            std::size_t operator()(const Element &element) const;
        };

        struct ElementEqual
        {
            bool operator()(const Element &lhs, const Element &rhs) const;
        };

        using ElementMap = std::unordered_map<Element, Element, ElementHash, ElementEqual>;

        class const_iterator
        {
        public:
            const_iterator() = default;

            const Pair &operator*() const;
            const Pair *operator->() const;
            const_iterator &operator++();
            bool operator==(const const_iterator &other) const;
            bool operator!=(const const_iterator &other) const;

        private:
            friend class o_dict;

            explicit const_iterator(const Pair *pair);

            const Pair *m_pair = nullptr;
        };

        explicit o_dict(const ElementMap &elements);

        std::size_t size() const;
        bool empty() const;
        bool contains(const Element &key) const;
        const Item *get(const Element &key) const;
        const Item *get(const Item &key) const;
        const_iterator begin() const;
        const_iterator end() const;
        std::size_t sizeOf() const;

        static std::size_t measure(const ElementMap &elements);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            auto cursor = buf;
            cursor += super_t::baseSize();

            auto countAt = cursor;
            cursor += db0::packed_int32::safeSizeOf(cursor);
            auto pairsByteSizeAt = cursor;
            cursor += db0::packed_int32::safeSizeOf(cursor);
            auto bucketByteSizeAt = cursor;
            cursor += db0::packed_int32::safeSizeOf(cursor);

            auto pairsByteSize = db0::packed_int32::__const_ref(pairsByteSizeAt).value();
            auto bucketByteSize = db0::packed_int32::__const_ref(bucketByteSizeAt).value();
            cursor += pairsByteSize;
            auto count = db0::packed_int32::__const_ref(countAt).value();
            cursor += hashIndexByteSize(count);
            cursor += bucketByteSize;
            return cursor - start;
        }

    protected:
        o_dict() = default;

        db0::Foundation::Arranger arrangeDictMembers(
            std::uint32_t count, std::uint32_t pairsByteSize, std::uint32_t bucketByteSize
        );
        void finishDictConstruction(
            void *indexEntries, std::uint32_t pairsByteSize, std::size_t capacity, std::uint32_t bucketByteSize
        );

        static bool elementsEqual(const Element &lhs, const Element &rhs);
        static std::uint32_t elementHash(const Element &element);
        static std::size_t hashIndexCapacity(std::size_t count);
        static std::uint32_t checkedHashIndexOffset(std::size_t offset, const char *name);
        static std::uint32_t checkedUint32Size(std::size_t size, const char *name);

    private:
        struct HashIndexEntry
        {
            static constexpr std::uint32_t BUCKET_FLAG = 0x80000000U;
            static constexpr std::uint32_t OFFSET_MASK = 0x7fffffffU;

            std::uint32_t m_value = 0;

            bool isEmpty() const;
            bool isBucket() const;
            bool isPendingBucket() const;
            std::uint32_t offset() const;
            void clear();
            void setPendingBucket();
            void setPair(std::uint32_t offset);
            void setBucket(std::uint32_t offset);
        };
        static_assert(sizeof(HashIndexEntry) == sizeof(std::uint32_t));

        std::size_t pairsByteSize() const;
        const db0::packed_int32 &count() const;
        const db0::packed_int32 &pairsByteSizeMember() const;
        const db0::packed_int32 &bucketByteSizeMember() const;
        const std::byte *beginOfPairs() const;
        const std::byte *beginOfBuckets() const;
        HashIndexEntry *beginOfHashIndex();
        const HashIndexEntry *beginOfHashIndex() const;
        const o_dict_bucket &bucketAtOffset(std::uint32_t offset) const;
        const Pair &pairAtOffset(std::uint32_t offset) const;

        static bool itemEqualsElement(const Item &item, const Element &element);
        static bool bytesEqual(const std::byte *lhs, const std::byte *rhs, std::size_t size);
        static std::size_t measurePairs(const ElementMap &elements);
        static std::size_t measureCollisionBuckets(const ElementMap &elements, std::size_t capacity);
        static Element elementFromItem(const Item &item);
        static std::uint32_t itemHash(const Item &item);
        static std::uint32_t hashBytes(const void *data, std::size_t size, std::uint32_t seed);
        static std::size_t hashIndexByteSize(std::size_t count);
        static std::uint32_t buildHashIndex(
            HashIndexEntry *indexEntries, const std::byte *pairsBegin, std::size_t pairsByteSize,
            std::size_t capacity
        );
        static std::uint32_t writeCollisionBuckets(
            HashIndexEntry *indexEntries, const std::byte *pairsBegin, std::size_t pairsByteSize,
            std::size_t capacity, std::byte *bucketStart
        );
    };
DB0_PACKED_END

}
