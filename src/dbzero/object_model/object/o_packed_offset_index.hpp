// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <vector>

#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/serialization/Ext.hpp>
#include <dbzero/core/serialization/list.hpp>
#include <dbzero/core/serialization/packed_int.hpp>

namespace db0::object_model
{

DB0_PACKED_BEGIN
    struct o_packed_offset_group_range
    {
        const std::uint64_t *begin = nullptr;
        const std::uint64_t *end = nullptr;
    };

    class DB0_PACKED_ATTR o_packed_offset_group: public db0::o_base<o_packed_offset_group, 0, false>
    {
    protected:
        using super_t = db0::o_base<o_packed_offset_group, 0, false>;
        friend super_t;

    public:
        o_packed_offset_group(const std::uint64_t *begin, const std::uint64_t *end);
        explicit o_packed_offset_group(o_packed_offset_group_range range);

        std::uint32_t size() const;
        bool empty() const;
        std::uint8_t elementSize() const;
        std::uint64_t first() const;
        std::uint64_t last() const;
        std::uint64_t at(std::uint32_t index) const;
        bool contains(std::uint64_t value) const;

        std::size_t sizeOf() const;

        static std::size_t measure(const std::uint64_t *begin, const std::uint64_t *end);
        static std::size_t measure(o_packed_offset_group_range range);

        template <typename BufT> static std::size_t safeSizeOf(BufT buf)
        {
            const auto &group = __const_ref(buf);
            auto result = group.sizeOf();
            buf += result;
            return result;
        }

    protected:
        o_packed_offset_group() = default;

    private:
        const db0::packed_int32 &packedSizeMember() const;
        const db0::packed_int32 &countMember() const;
        const std::byte *members() const;
        std::byte *members();
    };
DB0_PACKED_END

DB0_PACKED_BEGIN
    class DB0_PACKED_ATTR o_packed_offset_index:
        public db0::o_ext<o_packed_offset_index, db0::o_list<o_packed_offset_group, true>, 0, false>
    {
    public:
        using list_t = db0::o_list<o_packed_offset_group, true>;
        using super_t = db0::o_ext<o_packed_offset_index, list_t, 0, false>;
        class const_iterator
        {
        public:
            using iterator_category = std::forward_iterator_tag;
            using value_type = std::uint64_t;
            using difference_type = std::ptrdiff_t;
            using pointer = const std::uint64_t *;
            using reference = std::uint64_t;

            const_iterator() = default;

            std::uint64_t operator*() const;
            const_iterator &operator++();
            const_iterator operator++(int);
            bool operator==(const const_iterator &) const;
            bool operator!=(const const_iterator &) const;

        private:
            friend class o_packed_offset_index;

            const_iterator(list_t::const_iterator group, list_t::const_iterator end);
            void skipEmptyGroups();

            list_t::const_iterator m_group;
            list_t::const_iterator m_end;
            std::uint32_t m_index = 0;
        };

        friend super_t;

        std::size_t size() const;
        bool contains(std::uint64_t value) const;
        const_iterator begin() const;
        const_iterator end() const;

        static std::size_t measure(const std::vector<std::uint64_t> &offsets);

    protected:
        explicit o_packed_offset_index(const std::vector<std::uint64_t> &offsets);
    };
DB0_PACKED_END

}
