// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>
#include <string>
#include <dbzero/object_model/LangConfig.hpp>

namespace db0::object_model

{

    class TagSet
    {
    public:
        using LangToolkit = LangConfig::LangToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;

        TagSet(ObjectPtr const *args, std::size_t nargs, bool is_negated);

        bool isNegated() const;
        std::size_t size() const;
        
        const std::vector<ObjectSharedPtr> &getArgs() const;
        
    private:
        std::vector<ObjectSharedPtr> m_args;
        const bool m_is_negated;
    };

}