// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FT_Iterator.hpp"
#include <list>
#include <optional>
#include <vector>
#include <memory>
#include "CP_Vector.hpp"
#include <dbzero/core/memory/Address.hpp>

namespace db0

{   

    // The IteratorGroup helps to manage a group of iterators    
    // Currently we use the IterorGroup in the FT_ANDIterator implementation    
    template <typename KeyT = std::uint64_t, typename KeyStorageT = KeyT> 
    class IteratorGroup
    {
	public:
        using self_t = IteratorGroup<KeyT, KeyStorageT>;
        using FT_IteratorT = FT_Iterator<KeyT, KeyStorageT>;
        
        IteratorGroup(std::list<std::unique_ptr<FT_IteratorT> > &&);
        // special case for the group of 2 iterators        
		IteratorGroup(std::unique_ptr<FT_IteratorT> &&, std::unique_ptr<FT_IteratorT> &&);
        
        struct GroupItem
        {
            FT_IteratorT *m_iterator;
            
            inline FT_IteratorT &operator*() { return *m_iterator; }
            inline const FT_IteratorT &operator*() const { return *m_iterator; }
            
            inline FT_IteratorT *operator->() { return m_iterator; }
            inline const FT_IteratorT *operator->() const { return m_iterator; }

            // Try advancing iterator and retrieving the next key or ...
            // @return false if end of the iterator reached
            bool nextKey(int direction, KeyStorageT *buf = nullptr);
            bool nextUniqueKey(int direction, KeyStorageT *buf = nullptr);
        };
        
        using iterator = typename std::vector<GroupItem>::iterator;
        using const_iterator = typename std::vector<GroupItem>::const_iterator;

        iterator begin() { return m_group.begin(); }
        iterator end() { return m_group.end(); }

        const_iterator begin() const { return m_group.begin(); }
        const_iterator end() const { return m_group.end(); }
        
        std::size_t size() const;

        bool empty() const;

        GroupItem &front();
        
        const GroupItem &front() const;

        // Swap front elemement with the element pointed by the iterator
        // @return iterator after the swap (front)
        iterator swapFront(iterator it);

    private:
        // persistency holder
        std::list<std::unique_ptr<FT_IteratorT> > m_iterators;
        std::vector<GroupItem> m_group;
    };

    extern template class IteratorGroup<UniqueAddress>;
    extern template class IteratorGroup<std::uint64_t>;

    // Cartesian product specific types
    extern template class IteratorGroup<const UniqueAddress*, CP_Vector<UniqueAddress> >;
    extern template class IteratorGroup<const std::uint64_t*, CP_Vector<std::uint64_t> >;

}