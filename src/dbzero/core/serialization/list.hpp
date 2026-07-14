// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Base.hpp"
#include "packed_int.hpp"
#include <cstdint>
#include <cstring>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
DB0_PACKED_BEGIN

    template <bool compact> class DB0_PACKED_ATTR o_list_header
    {
    };

    template <> class DB0_PACKED_ATTR o_list_header<false>
    {
    public:
        std::uint32_t size_of;
        // number of list elements
        std::uint32_t count;
    };

    template <class T, bool compact = false>
    class DB0_PACKED_ATTR o_list:
        public o_base<o_list<T, compact>, 0, false>,
        public o_list_header<compact>
    {
    protected :
        using self_t = o_list<T, compact>;
        using super_t = o_base<o_list<T, compact>, 0, false>;
        friend super_t;

        /**
         * Constructs empty list instance
         */
        explicit o_list()
        {
            if constexpr (compact) {
                writeCompactSize(self_t::measure());
            } else {
                this->count = 0;
                this->size_of = self_t::arrangeMembers();
            }
        }

        o_list(const self_t &other)
            : super_t()
        {
            // copy raw bytes
            memcpy(this, &other, other.sizeOf());
        }

        template <class sequence_t, typename... Args> explicit o_list(const sequence_t &data, Args&& ...args)
        {
            if constexpr (!compact) {
                this->count = data.size();
            }

            auto arranger = makeArrangerFor(data, std::forward<Args>(args)...);
            auto it = data.begin(), end = data.end();
            while (it != end)
            {
                arranger = arranger(T::type(), *it, std::forward<Args>(args)...);
                ++it;
            }

            if constexpr (!compact) {
                this->size_of = arranger;
            }
        }

    public :
        /** Measure empty list
         *
         */
        static std::size_t measure() {
            if constexpr (compact) {
                return compactSizeFromElementBytes(0);
            } else {
                return self_t::measureMembers();
            }
        }

        static std::size_t measure(const self_t &other) {
            return other.sizeOf();
        }

        template <typename SequenceT, typename... Args> static std::size_t measure(const SequenceT &data, Args&& ...args)
        {
            if constexpr (compact) {
                return compactSizeFromElementBytes(measureElementBytes(data, std::forward<Args>(args)...));
            } else {
                auto meter = self_t::measureMembers();
                auto it = data.begin(), end = data.end();
                while (it != end)
                {
                    meter = meter(T::type(), *it, std::forward<Args>(args)...);
                    ++it;
                }
                return meter;
            }
        }

        std::size_t sizeOf () const {
            if constexpr (compact) {
                return compactSizeMember().value();
            } else {
                return static_cast<std::size_t>(this->size_of);
            }
        }

        template <typename buf_t> static std::size_t safeSizeOf(buf_t at)
        {
            if constexpr (compact) {
                const std::byte *cursor = at;
                auto result = packed_uint32::read(cursor);
                at += result;
                return result;
            } else {
                auto result = self_t::__const_ref(at).size_of;
                at += result;
                return result;
            }
        }

        inline std::uint32_t size() const {
            if constexpr (compact) {
                std::uint32_t result = 0;
                auto it = begin(), stop = end();
                while (it != stop) {
                    ++result;
                    ++it;
                }
                return result;
            } else {
                return this->count;
            }
        }

        bool empty() const {
            if constexpr (compact) {
                return begin() == end();
            } else {
                return this->count==0;
            }
        }

        class const_iterator
        {
        public :
            // as invalid
            const_iterator() = default;
            const_iterator(const T *item)
                : item(item)
            {
            }

            const T *operator->() const {
                return this->item;
            }

            const T &operator*() const {
                return *this->item;
            }

            const_iterator &operator++()
            {
                item = (const T*)((char*)item + item->sizeOf());
                return *this;
            }

            bool operator==(const const_iterator &it) const {
                return (item==it.item);
            }

            bool operator!=(const const_iterator &it) const {
                return (item!=it.item);
            }

        protected :
            const T *item = nullptr;
        };

        const_iterator begin() const {
            if constexpr (compact) {
                return const_iterator(reinterpret_cast<const T*>(beginOfItems()));
            } else {
                return const_iterator(reinterpret_cast<const T*>(self_t::beginOfDynamicArea()));
            }
        }

        const_iterator end() const {
            // past the end of data
            return const_iterator (reinterpret_cast<const T*>(self_t::beginOfMemberArea() + sizeOf()));
        }

    private:
        const packed_uint32 &compactSizeMember() const
        {
            static_assert(compact);
            return packed_uint32::__const_ref(self_t::beginOfDynamicArea());
        }

        std::byte *beginOfItems()
        {
            if constexpr (compact) {
                return self_t::beginOfDynamicArea() + packed_uint32::measure(static_cast<std::uint32_t>(sizeOf()));
            } else {
                return self_t::beginOfDynamicArea();
            }
        }

        const std::byte *beginOfItems() const
        {
            return const_cast<self_t *>(this)->beginOfItems();
        }

        void writeCompactSize(std::size_t size)
        {
            auto cursor = self_t::beginOfDynamicArea();
            packed_uint32::write(cursor, static_cast<std::uint32_t>(size));
        }

        template <typename SequenceT, typename... Args>
        Foundation::Arranger makeArrangerFor(const SequenceT &data, Args&& ...args)
        {
            if constexpr (compact) {
                auto size = self_t::measure(data, std::forward<Args>(args)...);
                writeCompactSize(size);
                return Foundation::Arranger(reinterpret_cast<const std::byte*>(this), beginOfItems());
            } else {
                return self_t::arrangeMembers();
            }
        }

        template <typename SequenceT, typename... Args>
        static std::size_t measureElementBytes(const SequenceT &data, Args&& ...args)
        {
            auto meter = Foundation::Meter(0);
            auto it = data.begin(), end = data.end();
            while (it != end)
            {
                meter = meter(T::type(), *it, std::forward<Args>(args)...);
                ++it;
            }
            return meter;
        }

        static std::size_t compactSizeFromElementBytes(std::size_t elementBytes)
        {
            auto size = elementBytes + packed_uint32::measure(static_cast<std::uint32_t>(elementBytes));
            while (true) {
                auto nextSize = elementBytes + packed_uint32::measure(static_cast<std::uint32_t>(size));
                if (nextSize == size) {
                    return size;
                }
                size = nextSize;
            }
        }
    };

DB0_PACKED_END
}
