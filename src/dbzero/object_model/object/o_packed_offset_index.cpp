// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "o_packed_offset_index.hpp"

#include <algorithm>
#include <cassert>
#include <limits>

#include <dbzero/core/exception/Exceptions.hpp>

namespace db0::object_model
{
    namespace
    {
        std::uint32_t checkedUint32(std::size_t value, const char *name)
        {
            if (value > std::numeric_limits<std::uint32_t>::max()) {
                THROWF(db0::InternalException) << name << " exceeds uint32 range";
            }
            return static_cast<std::uint32_t>(value);
        }

        std::uint8_t checkedPackedSize(std::size_t value)
        {
            if (value == 0 || value > db0::packed_int64::max_len()) {
                THROWF(db0::InternalException) << "Packed offset width is invalid";
            }
            return static_cast<std::uint8_t>(value);
        }

        std::uint64_t maxForPackedSize(std::uint8_t packedSize)
        {
            checkedPackedSize(packedSize);
            if (packedSize == db0::packed_int64::max_len()) {
                return std::numeric_limits<std::uint64_t>::max();
            }
            return (std::uint64_t { 1 } << (packedSize * 7)) - 1;
        }

        void writePacked64WithWidth(std::byte *&cursor, std::uint64_t value, std::uint8_t width)
        {
            maxForPackedSize(width);
            for (std::uint8_t byteIndex = 0; byteIndex < width; ++byteIndex) {
                auto shift = (width - byteIndex - 1) * 7;
                auto byteValue = static_cast<std::uint8_t>((value >> shift) & 0x7f);
                if (byteIndex + 1 < width) {
                    byteValue |= 0x80;
                }
                *cursor = static_cast<std::byte>(byteValue);
                ++cursor;
            }
        }

        std::uint64_t readPacked64WithWidth(
            const std::byte *members, std::uint8_t packedSize, std::uint32_t index
        )
        {
            auto *cursor = members + (static_cast<std::size_t>(packedSize) * index);
            return db0::packed_int64::read(cursor);
        }

        std::uint8_t packedSizeOfGroup(const std::uint64_t *begin, const std::uint64_t *end)
        {
            if (begin == nullptr || end == nullptr || begin >= end) {
                THROWF(db0::InternalException) << "Offset index group data range is invalid";
            }
            return checkedPackedSize(db0::packed_int64::measure(*begin));
        }

        const std::uint64_t *scanInputGroup(const std::uint64_t *&cursor, const std::uint64_t *end)
        {
            auto begin = cursor;
            auto packedSize = checkedPackedSize(db0::packed_int64::measure(*cursor));
            auto groupMax = maxForPackedSize(packedSize);
#ifndef NDEBUG
            auto previous = *cursor;
#endif
            ++cursor;
            while (cursor < end && *cursor <= groupMax) {
#ifndef NDEBUG
                assert(*cursor > previous && "Offset index input must be sorted and unique");
                previous = *cursor;
#endif
                ++cursor;
            }
#ifndef NDEBUG
            assert((cursor >= end || *cursor > previous) && "Offset index input must be sorted and unique");
#endif
            return begin;
        }

        std::uint32_t countInputGroups(const std::vector<std::uint64_t> &offsets)
        {
            std::size_t groupCount = 0;
            auto *groupBegin = offsets.data();
            auto *end = groupBegin + offsets.size();
            while (groupBegin < end) {
                scanInputGroup(groupBegin, end);
                ++groupCount;
            }
            return checkedUint32(groupCount, "Offset index group count");
        }
    }

    o_packed_offset_group::o_packed_offset_group(const std::uint64_t *begin, const std::uint64_t *end)
    {
        auto packedSize = packedSizeOfGroup(begin, end);
        auto count = checkedUint32(end - begin, "Offset index group count");
        auto arranger = arrangeMembers()
            (db0::packed_int32::type(), packedSize)
            (db0::packed_int32::type(), count);
        auto *cursor = arranger.ptr();
        for (auto *it = begin; it != end; ++it) {
            writePacked64WithWidth(cursor, *it, packedSize);
        }
    }

    std::uint32_t o_packed_offset_group::size() const
    {
        return countMember().value();
    }

    bool o_packed_offset_group::empty() const
    {
        return size() == 0;
    }

    std::uint8_t o_packed_offset_group::elementSize() const
    {
        return checkedPackedSize(packedSizeMember().value());
    }

    std::uint64_t o_packed_offset_group::first() const
    {
        if (empty()) {
            THROWF(db0::InternalException) << "Offset index group is empty";
        }
        return at(0);
    }

    std::uint64_t o_packed_offset_group::last() const
    {
        if (empty()) {
            THROWF(db0::InternalException) << "Offset index group is empty";
        }
        return at(size() - 1);
    }

    std::uint64_t o_packed_offset_group::at(std::uint32_t index) const
    {
        auto count = size();
        if (index >= count) {
            THROWF(db0::InternalException) << "Offset index group element is out of range";
        }
        auto *cursor = members() + (static_cast<std::size_t>(elementSize()) * index);
        return db0::packed_int64::read(cursor);
    }

    bool o_packed_offset_group::contains(std::uint64_t value) const
    {
        auto groupElementSize = elementSize();
        if (checkedPackedSize(db0::packed_int64::measure(value)) != groupElementSize) {
            return false;
        }

        const auto *groupMembers = members();
        auto count = size();
        std::uint32_t begin = 0;
        auto end = count;
        while (begin < end) {
            auto mid = begin + ((end - begin) / 2);
            auto candidate = readPacked64WithWidth(groupMembers, groupElementSize, mid);
            if (candidate < value) {
                begin = mid + 1;
            } else {
                end = mid;
            }
        }
        return begin < count && readPacked64WithWidth(groupMembers, groupElementSize, begin) == value;
    }

    std::size_t o_packed_offset_group::sizeOf() const
    {
        return members() - reinterpret_cast<const std::byte *>(this)
            + (static_cast<std::size_t>(elementSize()) * size());
    }

    std::size_t o_packed_offset_group::measure(const std::uint64_t *begin, const std::uint64_t *end)
    {
        auto packedSize = packedSizeOfGroup(begin, end);
        auto count = checkedUint32(end - begin, "Offset index group count");
        return measureMembers()
            (db0::packed_int32::type(), packedSize)
            (db0::packed_int32::type(), count)
            (static_cast<std::size_t>(packedSize) * count);
    }

    const db0::packed_int32 &o_packed_offset_group::packedSizeMember() const
    {
        return getDynFirst(db0::packed_int32::type());
    }

    const db0::packed_int32 &o_packed_offset_group::countMember() const
    {
        return getDynAfter(packedSizeMember(), db0::packed_int32::type());
    }

    const std::byte *o_packed_offset_group::members() const
    {
        return reinterpret_cast<const std::byte *>(&countMember()) + countMember().sizeOf();
    }

    std::byte *o_packed_offset_group::members()
    {
        return const_cast<std::byte *>(static_cast<const o_packed_offset_group *>(this)->members());
    }

    o_packed_offset_index::o_packed_offset_index(const std::vector<std::uint64_t> &offsets)
        : super_t()
    {
        auto &list = getSuper();
        list.count = countInputGroups(offsets);
        auto *listBegin = reinterpret_cast<std::byte *>(&list);
        auto arranger = db0::Foundation::Arranger(listBegin, listBegin + list_t::measure());
        auto *groupBegin = offsets.data();
        auto *end = groupBegin + offsets.size();
        while (groupBegin < end) {
            auto *groupStart = scanInputGroup(groupBegin, end);
            arranger = arranger(o_packed_offset_group::type(), groupStart, groupBegin);
        }
        list.size_of = arranger;
    }

    std::size_t o_packed_offset_index::size() const
    {
        std::size_t result = 0;
        for (auto it = begin(); it != end(); ++it) {
            result += it->size();
        }
        return result;
    }

    bool o_packed_offset_index::contains(std::uint64_t value) const
    {
        if (getSuper().empty()) {
            return false;
        }

        auto targetPackedSize = checkedPackedSize(db0::packed_int64::measure(value));
        for (auto it = begin(); it != end(); ++it) {
            if (targetPackedSize < it->elementSize()) {
                return false;
            }
            if (targetPackedSize > it->elementSize()) {
                continue;
            }
            return it->contains(value);
        }
        return false;
    }

    std::size_t o_packed_offset_index::measure(const std::vector<std::uint64_t> &offsets)
    {
        auto result = list_t::measure();
        auto *groupBegin = offsets.data();
        auto *end = groupBegin + offsets.size();
        while (groupBegin < end) {
            auto *groupStart = scanInputGroup(groupBegin, end);
            result += o_packed_offset_group::measure(groupStart, groupBegin);
        }
        return result;
    }
}
