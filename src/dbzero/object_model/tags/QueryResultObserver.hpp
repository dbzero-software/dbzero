#pragma once

namespace db0::object_model

{

    // QueryResultObserver is simple interface to retrieve current element's additional properties
    class QueryResultObserver
    {
    public:
        virtual ~QueryResultObserver() = default;

        // Retrieve current element's decoration - e.g. corresponding enum value
        // @return value which needs to be cast to a known type (or nullptr)
        virtual void *getDecoration() const = 0;
    };
    
}   