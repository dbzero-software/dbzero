// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "TagSet.hpp"

namespace db0::object_model

{

    TagSet::TagSet(ObjectPtr const *args, std::size_t nargs, bool is_negated)
        : m_is_negated(is_negated)
    {
        for (auto arg = args; arg != args + nargs; ++arg) {
            m_args.emplace_back(*arg);
        }
    }
    
    bool TagSet::isNegated() const {
        return m_is_negated;
    }
    
    std::size_t TagSet::size() const {
        return m_args.size();
    }
    
    const std::vector<TagSet::ObjectSharedPtr> &TagSet::getArgs() const {
        return m_args;
    }
    
}