#pragma once

#include "FT_IteratorBase.hpp"
#include "FT_Iterator.hpp"
#include <dbzero/core/serialization/Serializable.hpp>

namespace db0

{

    enum class SortedIteratorTypeId: std::uint16_t
    {
        Invalid = 0,
        RT_Sort = 1
    };

    /**
     * A base for sorted full-text iterators
    */
    template <typename ValueT> class SortedIterator: public FT_IteratorBase, public Serializable
    {
    public:
        using QueryIterator = FT_Iterator<ValueT>;

        /**
         * Check if the underlying full-text query has been defined
        */
        virtual bool hasFTQuery() const = 0;

        /**
         * Retrieve the underlying full-text query
        */
        virtual std::unique_ptr<QueryIterator> beginFTQuery() const = 0;

        /**
         * Clone the iterator for starting over preserving the sorting order
         * or sort a specific inner query (ft_query) if provided       
        */
        virtual std::unique_ptr<SortedIterator<ValueT> > beginSorted(std::unique_ptr<QueryIterator> ft_query = nullptr) const = 0;

        virtual SortedIteratorTypeId getSerializationTypeId() const = 0;
    };
    
}
