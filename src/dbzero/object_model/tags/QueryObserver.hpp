#pragma once

#include <dbzero/object_model/config.hpp>

namespace db0::object_model

{

    // QueryObserver is simple interface to retrieve current element's additional properties
    class QueryObserver
    {
    public:
        using LangToolkit = Config::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;

        virtual ~QueryObserver() = default;

        // Retrieve current element's decoration - e.g. corresponding tag or other assigned information
        // @return value which needs to be cast to a known type (or nullptr)
        virtual ObjectPtr getDecoration() const = 0;
    };
        
}   