// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "mu_store.hpp"
#include <cassert>
#include <algorithm>
#include <limits>

namespace db0

{

    o_mu_store::o_mu_store(std::size_t max_bytes)
        : m_capacity(max_bytes)
    {
        assert(max_bytes <= 0xFFFF);
        assert(maxSize() <= std::numeric_limits<std::uint8_t>::max() - 1);
    }
    
    std::size_t o_mu_store::measure(std::size_t max_bytes) 
    {
        assert(max_bytes <= 0xFFFF);
        return max_bytes;
    }

    std::size_t o_mu_store::sizeOf() const {
        return m_capacity;
    }

    bool o_mu_store::ConstIterator::operator!=(const ConstIterator &other) const {
        return m_current != other.m_current;
    }
    
    bool o_mu_store::ConstIterator::operator==(const ConstIterator &other) const {
        return m_current == other.m_current;
    }
    
    o_mu_store::ConstIterator &o_mu_store::ConstIterator::operator++()
    {
        m_current += 3;
        return *this;
    }

    std::pair<std::uint16_t, std::uint16_t> o_mu_store::ConstIterator::operator*() const 
    {
        std::uint16_t offset = (m_current[0] << 4) | (m_current[1] >> 4);
        std::uint16_t size = ((m_current[1] & 0x0F) << 8) | m_current[2];
        return { offset, size };
    }

    o_mu_store::ConstIterator::ConstIterator(const std::uint8_t *current)
        : m_current(const_cast<std::uint8_t *>(current))
    {
    }
    
    o_mu_store::ConstIterator o_mu_store::begin() const
    {
        // no iteration possible over a full-range modification
        if (isFullRange()) {
            return this->end();
        }
        return ConstIterator((std::uint8_t*)this + sizeof(o_mu_store));
    }
    
    o_mu_store::ConstIterator o_mu_store::end() const {
        return ConstIterator((std::uint8_t*)this + sizeof(o_mu_store) + m_size * 3);
    }

    std::size_t o_mu_store::size() const {
        return isFullRange() ? 0 : m_size;
    }
    
    void o_mu_store::appendFullRange() {
        // special value to mark the whole range as modified        
        m_size = std::numeric_limits<std::uint8_t>::max();
    }
    
    bool o_mu_store::tryAppend(std::uint16_t offset, std::uint16_t size)
    {
        if (isFullRange()) {
            return false;
        }

        std::uint8_t *at;
        for (;;) {
            at = (std::uint8_t*)this + sizeof(o_mu_store) + m_size * 3;
            // try compacting once capacity limit is reached
            if (at + 3 <= (std::uint8_t*)this + m_capacity) {
                break;
            }

            auto old_size = m_size;
            this->compact();
            assert(m_size <= old_size);
            // compaction level was insufficient
            if (old_size - m_size < 3) {
                return false;
            }
            // trying again after compaction
        }
        
        db0::compress(offset, size, *reinterpret_cast<std::array<std::uint8_t, 3>*>(at));
        ++m_size;        
        return true;
    }
    
    struct CompT
    {
        using ItemT = std::array<std::uint8_t, 3>;

        inline std::uint16_t offset(const ItemT &item) const {
            return (item[0] << 4) | (item[1] >> 4);
        }

        // compare offset parts
        inline bool operator()(const ItemT &a, const ItemT &b) const {
            return offset(a) < offset(b);            
        }
    };

    // the element-compacting writer
    struct CompactWriter
    {
        using ItemT = typename CompT::ItemT;        
        ItemT *m_last = nullptr;
        std::pair<std::uint16_t, std::uint16_t> m_last_item = {0, 0};
        ItemT * const m_begin;
        ItemT *m_next;
        
        CompactWriter(std::uint8_t *next)
            : m_begin(reinterpret_cast<ItemT*>(next))
            , m_next(m_begin)
        {
        }
        
        inline bool overlap(std::pair<std::uint16_t, std::uint16_t> a,
            std::pair<std::uint16_t, std::uint16_t> b) const
        {
            return (a.first + a.second) >= b.first;
        }
        
        bool merge(std::pair<std::uint16_t, std::uint16_t> &a, std::pair<std::uint16_t, std::uint16_t> b) const
        {
            if (overlap(a, b)) {
                a.second = std::max(a.first + a.second, b.first + b.second) - a.first;
                return true;
            }
            return false;
        }

        // append consecutivea items (i.e. non-decresing offsets)
        void append(std::pair<std::uint16_t, std::uint16_t> item)
        {
            // try merging overlapping or similar items
            if (m_last && merge(m_last_item, item)) {
                // write the merged item
                compress(m_last_item.first, m_last_item.second, *m_last);
            } else {
                compress(item.first, item.second, *m_next);
                m_last_item = item;
                m_last = m_next;
                ++m_next;
            }
        }

        std::uint8_t size() const {
            return m_next - m_begin;
        }
    };
    
    void o_mu_store::compact()
    {        
        if (m_size < 2 || isFullRange()) {
            return;
        }

        // sort 3-byte items in-place
        std::array<std::uint8_t, 3> *begin_ptr = (std::array<std::uint8_t, 3>*)((std::uint8_t*)this + sizeof(o_mu_store));        
        std::sort(begin_ptr, begin_ptr + m_size, CompT());

        // copy sorted items back to the container, merging overlapping ones
        CompactWriter writer((std::uint8_t*)this + sizeof(o_mu_store));
        for (auto it = this->begin(), end = this->end(); it != end; ++it) {
            writer.append(*it);
        }
        m_size = writer.size();
    }
    
    std::size_t o_mu_store::maxSize() const {
        return (m_capacity - sizeof(o_mu_store)) / 3;
    }

    void o_mu_store::clear() {
        m_size = 0;
    }
        
    std::size_t o_mu_store::getMUSize() const
    {
        if (isFullRange()) {
            return 0;
        }
        std::size_t total = 0;
        for (auto mu_item: *this) {
            total += mu_item.second;
        }
        return total;
    }
    
}