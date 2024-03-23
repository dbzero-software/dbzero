#pragma once

#include "FT_IteratorBase.hpp"
#include "FT_Iterator.hpp"

namespace db0

{
        
    /**
     * The ItertorFactory interface combines FT_IteratorBase and FT_Iterator) properties
     * It can be used to construct either of the types depending on the usage context
    */
    template <typename KeyT> class IteratorFactory
    {
    public:
        virtual ~IteratorFactory() = default;

        virtual std::unique_ptr<FT_IteratorBase> createBaseIterator() = 0;

        virtual std::unique_ptr<FT_Iterator<KeyT> > createFTIterator() = 0;
    };
    
}
