#pragma once

#include "FT_Iterator.hpp"

namespace db0

{

    template <typename KeyT> class FT_IteratorFactory
    {
    public:
        virtual ~FT_IteratorFactory() = default;
        
        virtual void add(std::unique_ptr<FT_Iterator<KeyT> > &&) = 0;
        
        virtual std::unique_ptr<FT_Iterator<KeyT> > release(int direction, bool lazy_init = false) = 0;

        // Invalidate / render empty
        virtual void clear() = 0;
    };
    
}