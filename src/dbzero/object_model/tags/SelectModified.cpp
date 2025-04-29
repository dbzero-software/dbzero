#include "SelectModified.hpp"
#include <dbzero/core/collections/full_text/FT_SpanIterator.hpp>

namespace db0::object_model

{
    
    std::unique_ptr<QueryIterator> selectModified(std::unique_ptr<QueryIterator> &&query, const db0::BaseStorage &storage,
        StateNumType from_state, StateNumType to_state)
    {
        // The algorithm works as follows:
        // 1. collect mutated DPs within the provided scope
        // 2. construct FT_SpanIterator containing the mutated DPs as spans
        // 3. AND-join the span filter and the original query
        // 4. refine results (lazy filter) by binary comparison of pre-scope and post-scope objects to identify actual mutations
    
        return std::move(query);
    }

} 
    