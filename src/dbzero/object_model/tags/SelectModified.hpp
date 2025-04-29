#pragma once

#include <optional>
#include "ObjectIterable.hpp"
#include <dbzero/core/memory/config.hpp>

namespace db0

{

    class BaseStorage;

}

namespace db0::object_model

{

    using QueryIterator = typename ObjectIterable::QueryIterator;

    // This function allows applying the additional filter on top of an existing query iterator
    // to include in the result only objects modified wihitn a specific scope
    // @param query the input query iterator
    // @param storage the query-prefix associated storage
    // @param from_state the first state number to retrieve mutations from (scope begin)
    // @param to_state the last state number to be includes (scope end)
    std::unique_ptr<QueryIterator> selectModified(std::unique_ptr<QueryIterator> &&query, const db0::BaseStorage &,
        StateNumType from_state, StateNumType to_state
    );
    
}