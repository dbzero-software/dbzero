#pragma once

#include <memory>
#include <deque>
#include "SlabAllocator.hpp"

namespace db0

{
    
    class SlabRecycler
    {
    public:
        SlabRecycler(unsigned int max_size = 256);

        void append(std::shared_ptr<SlabAllocator> slab);
        
        /**
         * Get the number of slab currently begin stored
        */
        std::size_t size() const;

        /**
         * Get the maximum number of slab that could be stored
        */
        std::size_t capacity() const;
        
        /**
         * Close / remove all SlabAllocator instances that match the predicate
        */
        void close(std::function<bool(const SlabAllocator &)> predicate, bool only_first = false);

        void closeOne(std::function<bool(const SlabAllocator &)> predicate);
        
    private:
        const unsigned int m_max_size;
        std::deque<std::shared_ptr<SlabAllocator> > m_slabs;
    };

}