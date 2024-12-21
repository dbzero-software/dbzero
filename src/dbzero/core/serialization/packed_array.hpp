#pragma once

#include "Base.hpp"
#include <cstdint>

namespace db0

{

    // packed_array is a fixed-size overlaid container for storing
    // variable number of variable-length items
    // @tparam ItemT the variable-length item type (e.g. o_packed_int)
    // @tparam SizeT the size type for the offset of the end item, determines the maximum capacity limit
    // @tparam MAX_BYTES the container's capacity / size_of
    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    class [[gnu::packed]] o_packed_array: public o_base<o_packed_array<ItemT, SizeT, MAX_BYTES>, 0, false>
    {
    protected:
        using self_t = o_packed_array<ItemT, SizeT, MAX_BYTES>;
        using super_t = o_base<o_packed_array<ItemT, SizeT, MAX_BYTES>, 0, false>;
        friend super_t;

        o_packed_array() = default;

    public:
        static std::size_t measure();
        
        template <typename buf_t> static std::size_t safeSizeOf(buf_t at)
        {
            at += MAX_BYTES;
            return MAX_BYTES;
        }
        
        class ConstIterator
        {
        public:
            ConstIterator(const std::byte *at);
            ConstIterator &operator++();
            bool operator!=(const ConstIterator &other) const;
            const ItemT &operator*() const;

        private:
            const std::byte *m_at;
        };

        ConstIterator begin() const;
        ConstIterator end() const;

        // Try appending a next element
        // @return false if there's no sufficient capacity for item to be appended
        template <typename... Args> bool tryEmplaceBack(Args&&... args)
        {
            auto size_of_item = ItemT::measure(args...);
            if (sizeof(self_t) + m_end_offset + size_of_item > MAX_BYTES) {
                // capacity reached
                return false;
            }
            ItemT::__new(reinterpret_cast<std::byte*>(this) + sizeof(self_t) + m_end_offset,
                std::forward<Args>(args)...);
            m_end_offset += size_of_item;
            return true;
        }

    private:
        SizeT m_end_offset = 0;
    };

    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    std::size_t o_packed_array<ItemT, SizeT, MAX_BYTES>::measure()
    {
        static_assert(MAX_BYTES <= std::numeric_limits<SizeT>::max());
        return MAX_BYTES;
    }

    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator::ConstIterator(const std::byte *at)
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
    bool o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator::operator!=(const o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator &other) const {
        return m_at != other.m_at;
    }

    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    const ItemT &o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator::operator*() const {
        return ItemT::__const_ref(m_at);
    }

    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    typename o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator o_packed_array<ItemT, SizeT, MAX_BYTES>::begin() const {
        return ConstIterator(reinterpret_cast<const std::byte*>(this) + sizeof(self_t));
    }

    template <typename ItemT, typename SizeT, std::size_t MAX_BYTES>
    typename o_packed_array<ItemT, SizeT, MAX_BYTES>::ConstIterator o_packed_array<ItemT, SizeT, MAX_BYTES>::end() const {
        return ConstIterator(reinterpret_cast<const std::byte*>(this) + sizeof(self_t) + m_end_offset);
    }

};