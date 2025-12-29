// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/collections/vector/joinable_const_iterator.hpp>
#include <dbzero/core/collections/b_index/type.hpp>

namespace db0

{
    
    /**
     * The FT_MemoryIndex allows to create a full-text index over a memory-resident sorted vector
    */
    template <typename KeyT, typename CompT = std::less<KeyT> > class FT_MemoryIndex
    {
    public:
        using self_t = FT_MemoryIndex<KeyT, CompT>;
        using joinable_const_iterator = db0::joinable_const_iterator<KeyT, CompT>;

        FT_MemoryIndex(const KeyT *begin, const KeyT *end)
            : m_begin(begin)
            , m_end(end)
        {
        }

        joinable_const_iterator beginJoin(int direction) const
        {
            assert(direction == 1 || direction == -1);
            if (direction > 0 || m_begin == m_end) {
                return joinable_const_iterator(m_begin, m_end, m_begin, direction);
            } else {
                return joinable_const_iterator(m_begin, m_end, m_end - 1, direction);
            }            
        }

        bool operator==(const FT_MemoryIndex &other) const {
            return m_begin == other.m_begin && m_end == other.m_end;
        }

        std::uint64_t getAddress() const {
            return 0;
        }

        bindex::type getIndexType() const {
            return bindex::type::empty;
        }
        
        // static type ID for serializations
        static std::uint64_t getSerialTypeId()
        {
			return db0::serial::typeId<self_t>(
				(db0::serial::typeId<KeyT>() << 16) | static_cast<std::uint16_t>(db0::serial::CollectionType::FT_MemoryIndex)
            );
        }

        // Memspace for compatibility purposes (serialization)
        Memspace &getMemspace() const
        {
            static Memspace null_memspace;
            return null_memspace;
        }

    private:
        const KeyT *m_begin;
        const KeyT *m_end;
    };

}