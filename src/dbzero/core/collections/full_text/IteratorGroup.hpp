#include "FT_Iterator.hpp"
#include <list>
#include <optional>
#include <vector>
#include <memory>
#include <dbzero/core/memory/Address.hpp>

namespace db0

{   

    // The IteratorGroup helps to manage a group of iterators    
    // Currently we use the IterorGroup in the FT_ANDIterator implementation    
    template <typename KeyT = std::uint64_t> class IteratorGroup
    {
	public:
        using self_t = IteratorGroup<KeyT>;
        
        IteratorGroup(std::list<std::unique_ptr<FT_Iterator<KeyT> > > &&);
        // special case for the group of 2 iterators        
		IteratorGroup(std::unique_ptr<FT_Iterator<KeyT> > &&, std::unique_ptr<FT_Iterator<KeyT> > &&);
        
        struct GroupItem
        {
            FT_Iterator<KeyT> *m_iterator;            
            
            inline FT_Iterator<KeyT> &operator*() { return *m_iterator; }
            inline const FT_Iterator<KeyT> &operator*() const { return *m_iterator; }
            
            inline FT_Iterator<KeyT> *operator->() { return m_iterator; }
            inline const FT_Iterator<KeyT> *operator->() const { return m_iterator; }

            // Try advancing iterator and retrieving the next key or ...
            // @return false if end of the iterator reached
            bool nextKey(int direction, KeyT *buf = nullptr);
            bool nextUniqueKey(int direction, KeyT *buf = nullptr);
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
        std::list<std::unique_ptr<FT_Iterator<KeyT> > > m_iterators;
        std::vector<GroupItem> m_group;
    };

    extern template class IteratorGroup<UniqueAddress>;
    extern template class IteratorGroup<std::uint64_t>;

}