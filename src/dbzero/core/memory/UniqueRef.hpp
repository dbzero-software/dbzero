// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <cstdint>
#include <functional>
#include <ostream>
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace db0

{

DB0_PACKED_BEGIN
    /**
     * UniqueRef stores an inverted-list reference in the same 64-bit footprint as UniqueAddress,
     * with one high bit reserved to mark passive references.
     *
     * Tag-field indexing can point at an object without owning its lifetime, while explicit tags
     * must keep the target object alive. Keeping that passive/owning bit next to the indexed value
     * lets the index choose reference-counting behavior without widening stored inverted-list
     * entries. Comparisons intentionally ignore the passive bit so both forms still identify the
     * same logical object address.
     */
    class DB0_PACKED_ATTR UniqueRef
    {
    public:
        static constexpr std::uint64_t PASSIVE_BIT = 1ULL << 63;
        static constexpr std::uint64_t VALUE_MASK = ~PASSIVE_BIT;
        static constexpr std::uint64_t OFFSET_MAX = 1ULL << 49;

        UniqueRef() = default;
        UniqueRef(UniqueAddress address, bool passive = false);

        static inline UniqueRef fromValue(std::uint64_t value);

        inline bool isValid() const;
        inline bool isPassive() const;
        inline bool isOwning() const;
        inline UniqueRef asPassive() const;
        inline UniqueRef asOwning() const;
        inline std::uint64_t getValue() const;
        inline UniqueAddress asUniqueAddress() const;
        inline Address getAddress() const;

        inline operator UniqueAddress() const;

        inline bool operator==(const UniqueRef& other) const;
        inline bool operator!=(const UniqueRef& other) const;
        inline bool operator<(const UniqueRef& other) const;
        inline bool operator<=(const UniqueRef& other) const;
        inline bool operator>(const UniqueRef& other) const;
        inline bool operator>=(const UniqueRef& other) const;

        inline friend std::ostream &operator<<(std::ostream &os, const UniqueRef &ref);

    private:
        std::uint64_t m_value = 0;

        explicit inline UniqueRef(std::uint64_t value);
    };
DB0_PACKED_END

    inline UniqueRef UniqueRef::fromValue(std::uint64_t value)
    {
        return UniqueRef(value);
    }

    inline bool UniqueRef::isValid() const
    {
        return (m_value & VALUE_MASK) != 0;
    }

    inline bool UniqueRef::isPassive() const
    {
        return (m_value & PASSIVE_BIT) != 0;
    }

    inline bool UniqueRef::isOwning() const
    {
        return !isPassive();
    }

    inline UniqueRef UniqueRef::asPassive() const
    {
        return UniqueRef(m_value | PASSIVE_BIT);
    }

    inline UniqueRef UniqueRef::asOwning() const
    {
        return UniqueRef(m_value & VALUE_MASK);
    }

    inline std::uint64_t UniqueRef::getValue() const
    {
        return m_value;
    }

    inline UniqueAddress UniqueRef::asUniqueAddress() const
    {
        return UniqueAddress::fromValue(m_value & VALUE_MASK);
    }

    inline Address UniqueRef::getAddress() const
    {
        return asUniqueAddress().getAddress();
    }

    inline UniqueRef::operator UniqueAddress() const
    {
        return asUniqueAddress();
    }

    inline bool UniqueRef::operator==(const UniqueRef& other) const
    {
        return (m_value & VALUE_MASK) == (other.m_value & VALUE_MASK);
    }

    inline bool UniqueRef::operator!=(const UniqueRef& other) const
    {
        return !(*this == other);
    }

    inline bool UniqueRef::operator<(const UniqueRef& other) const
    {
        return (m_value & VALUE_MASK) < (other.m_value & VALUE_MASK);
    }

    inline bool UniqueRef::operator<=(const UniqueRef& other) const
    {
        return (m_value & VALUE_MASK) <= (other.m_value & VALUE_MASK);
    }

    inline bool UniqueRef::operator>(const UniqueRef& other) const
    {
        return (m_value & VALUE_MASK) > (other.m_value & VALUE_MASK);
    }

    inline bool UniqueRef::operator>=(const UniqueRef& other) const
    {
        return (m_value & VALUE_MASK) >= (other.m_value & VALUE_MASK);
    }

    inline std::ostream &operator<<(std::ostream &os, const UniqueRef &ref)
    {
        os << ref.m_value;
        return os;
    }

    inline UniqueRef::UniqueRef(std::uint64_t value)
        : m_value(value)
    {
    }

}

namespace std

{

    template <> struct hash<db0::UniqueRef> {
        inline std::size_t operator()(const db0::UniqueRef &ref) const noexcept;
    };

    inline std::size_t hash<db0::UniqueRef>::operator()(const db0::UniqueRef &ref) const noexcept
    {
        return std::hash<std::uint64_t>()(ref.asUniqueAddress().getValue());
    }

}
