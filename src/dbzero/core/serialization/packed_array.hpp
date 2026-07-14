// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Base.hpp"
#include <cstdint>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
DB0_PACKED_BEGIN
    
    // packed_array is a fixed-size overlaid container for storing
    // variable number of variable-length items
    // @tparam ItemT the variable-length item type (e.g. o_packed_int)
    // @tparam SizeT the size type for the offset of the end item, determines the maximum capacity limit
    // @tparam MAX_BYTES the container's capacity / size_of
    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    class DB0_PACKED_ATTR o_packed_array: public o_fixed<o_packed_array<ItemT, SizeT, MAX_BYTES> >
    {
    public:
        o_packed_array()
            // for fundamental types (e.g. char) are 0-initialized
            : m_payload {}
        {            
        }

        class ConstIterator
        {
        public:
            ConstIterator() = default;
            ConstIterator(const unsigned char *);

            ConstIterator &operator++();

            bool operator==(const ConstIterator &other) const;
            bool operator!=(const ConstIterator &other) const;

            const ItemT &operator*() const;
            
            operator bool () const {
                return m_at != nullptr;
            }

        private:
            const unsigned char *m_at = nullptr;
        };

        ConstIterator begin() const;
        ConstIterator end() const;

        // Check if a specific element can be appended without adding it
        template <typename... Args> bool canEmplaceBack(Args&&... args) const {
            return sizeof(SizeT) + m_end_offset + ItemT::measure(args...) <= MAX_BYTES;
        }
        
        // Try appending a next element
        // @return false if there's no sufficient capacity for item to be appended
        template <typename... Args> bool tryEmplaceBack(Args&&... args)
        {
            auto size_of_item = ItemT::measure(args...);
            if (sizeof(SizeT) + m_end_offset + size_of_item > MAX_BYTES) {
                // capacity reached
                return false;
            }
            ItemT::__new(&m_payload[0] + m_end_offset, std::forward<Args>(args)...);
            m_end_offset += size_of_item;
            return true;
        }
        
        // Append next element without bounds validation
        template <typename... Args> bool emplaceBack(Args&&... args)
        {
            auto size_of_item = ItemT::measure(args...);
            ItemT::__new(&m_payload[0] + m_end_offset, std::forward<Args>(args)...);
            m_end_offset += size_of_item;
            return true;
        }

    private:
        // offset past the header
        SizeT m_end_offset = 0;
        std::array<unsigned char, MAX_BYTES - sizeof(SizeT)> m_payload;
    };
    
    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator::ConstIterator(const unsigned char *at)
        : m_at(at)
    {
    }

    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    typename o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator &o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator::operator++()
    {
        m_at += ItemT::__const_ref(m_at).sizeOf();
        return *this;
    }   
    
    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    bool o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator::operator==(const o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator &other) const {
        return m_at == other.m_at;
    }

    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    bool o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator::operator!=(const o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator &other) const {
        return m_at != other.m_at;
    }

    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    const ItemT &o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator::operator*() const {
        return ItemT::__const_ref(m_at);
    }

    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    typename o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator o_packed_array<ItemT, SizeT, MAX_BYTES>::begin() const {
        return ConstIterator(&m_payload[0]);
    }
    
    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    typename o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator o_packed_array<ItemT, SizeT, MAX_BYTES>::end() const {
        return ConstIterator(&m_payload[0] + m_end_offset);
    }

DB0_PACKED_END
};