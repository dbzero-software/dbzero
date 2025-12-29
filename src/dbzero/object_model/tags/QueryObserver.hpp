// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/core/collections/full_text/FT_IteratorBase.hpp>

namespace db0::object_model

{

    // QueryObserver is simple interface to retrieve current element's additional properties
    class QueryObserver
    {
    public:
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;

        virtual ~QueryObserver() = default;

        // Retrieve current element's decoration - e.g. corresponding tag or other assigned information
        // @return value which needs to be cast to a known type (or nullptr)
        virtual ObjectPtr getDecoration() const = 0;
        
        // Construct a new instance of QueryObserver - valid in a context of a different query tree        
        virtual std::unique_ptr<QueryObserver> rebase(const FT_IteratorBase &) const = 0;
    };
    
}   