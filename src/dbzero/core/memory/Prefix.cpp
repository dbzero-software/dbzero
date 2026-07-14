// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Prefix.hpp"

namespace db0

{

    Prefix::Prefix(std::string name)
        : m_name(std::move(name))    
    {
    }
    
    const std::string &Prefix::getName() const {
        return m_name;
    }
    
    void Prefix::beginAtomic() {
        THROWF(db0::InternalException) << "Atomic operations not supported by this prefix implementation" << THROWF_END;
    }

    void Prefix::endAtomic() {
        THROWF(db0::InternalException) << "Atomic operations not supported by this prefix implementation" << THROWF_END;        
    }

    void Prefix::cancelAtomic() {
        THROWF(db0::InternalException) << "Atomic operations not supported by this prefix implementation" << THROWF_END;        
    }

    void Prefix::cleanup() const
    {
    }
    
    bool Prefix::beginRefresh()
    {
        // refresh not supported by default
        assert(false);
        return false;
    }
    
    std::uint64_t Prefix::completeRefresh() {
        return 0;
    }

    std::uint64_t Prefix::refresh() 
    {
        if (beginRefresh()) {
            return completeRefresh();
        }
        return 0;
    }
    
    void Prefix::getStats(std::function<void(const std::string &, std::uint64_t)>) const {
    }
    
}