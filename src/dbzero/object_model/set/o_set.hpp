// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_set>

#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/serialization/packed_int.hpp>
#include <dbzero/object_model/tuple/o_tuple.hpp>

namespace db0::object_model
{

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_set: public db0::o_base<o_set, 0, false>
    {
    protected:
        using super_t = db0::o_base<o_set, 0, false>;
        friend super_t;

    public:
        using Element = o_tuple_item::Element;
        using Item = o_tuple_item;

        struct ElementHash
        {
            std::size_t operator()(const Element &element) const;
        };

        struct ElementEqual
        {
            bool operator()(const Element &lhs, const Element &rhs) const;
        };

        using ElementSet = std::unordered_set<Element, ElementHash, ElementEqual>;

        class const_iterator
        {
        public:
            const_iterator() = default;

            const Item &operator*() const;
            const Item *operator->() const;
            const_iterator &operator++();
            bool operator==(const const_iterator &other) const;
            bool operator!=(const const_iterator &other) const;

        private:
            friend class o_set;

            explicit const_iterator(const Item *item);

            const Item *m_item = nullptr;
        };

        explicit o_set(const ElementSet &elements);

        std::size_t size() const;
        bool empty() const;
        bool contains(const Element &element) const;
        bool contains(const Item &item) const;
        const_iterator begin() const;
        const_iterator end() const;
        std::size_t sizeOf() const;

        static std::size_t measure(const ElementSet &elements);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            auto start = buf;
            auto cursor = buf;
            cursor += super_t::baseSize();

            auto countAt = cursor;
            cursor += db0::packed_int32::safeSizeOf(cursor);
            auto elementsByteSizeAt = cursor;
            cursor += db0::packed_int32::safeSizeOf(cursor);
            auto bucketByteSizeAt = cursor;
            cursor += db0::packed_int32::safeSizeOf(cursor);

            auto elementsByteSize = db0::packed_int32::__const_ref(elementsByteSizeAt).value();
            auto bucketByteSize = db0::packed_int32::__const_ref(bucketByteSizeAt).value();
            cursor += elementsByteSize;
            auto count = db0::packed_int32::__const_ref(countAt).value();
            cursor += hashIndexByteSize(count);
            cursor += bucketByteSize;
            return cursor - start;
        }

    protected:
        o_set() = default;

        db0::Foundation::Arranger arrangeSetMembers(
            std::uint32_t count, std::uint32_t elementsByteSize, std::uint32_t bucketByteSize
        );
        void finishSetConstruction(
            void *indexEntries, std::uint32_t elementsByteSize, std::size_t capacity, std::uint32_t bucketByteSize
        );

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
            void setItem(std::uint32_t offset);
            void setBucket(std::uint32_t offset);
        };
        static_assert(sizeof(HashIndexEntry) == sizeof(std::uint32_t));

        std::size_t elementsByteSize() const;
        const Item &item(std::size_t index) const;
        const db0::packed_int32 &count() const;
        const db0::packed_int32 &elementsByteSizeMember() const;
        const db0::packed_int32 &bucketByteSizeMember() const;
        const std::byte *beginOfItems() const;
        const std::byte *beginOfBuckets() const;
        HashIndexEntry *beginOfHashIndex();
        const HashIndexEntry *beginOfHashIndex() const;
        const o_compact_tuple &bucketAtOffset(std::uint32_t offset) const;
        const Item &itemAtOffset(std::uint32_t offset) const;

        static bool elementsEqual(const Element &lhs, const Element &rhs);
        static bool itemEqualsElement(const Item &item, const Element &element);
        static bool bytesEqual(const std::byte *lhs, const std::byte *rhs, std::size_t size);
        static std::size_t measureElements(const ElementSet &elements);
        static std::size_t measureCollisionBuckets(const ElementSet &elements, std::size_t capacity);
        static Element elementFromItem(const Item &item);
        static std::uint32_t itemHash(const Item &item);
        static std::uint32_t hashBytes(const void *data, std::size_t size, std::uint32_t seed);
        static std::size_t hashIndexByteSize(std::size_t count);
        static std::uint32_t buildHashIndex(HashIndexEntry *indexEntries, const std::byte *itemsBegin, std::size_t itemsByteSize, std::size_t capacity);
        static std::uint32_t writeCollisionBuckets(HashIndexEntry *indexEntries, const std::byte *itemsBegin, std::size_t itemsByteSize, std::size_t capacity, std::byte *bucketStart);
    };
DB0_PACKED_END

}
