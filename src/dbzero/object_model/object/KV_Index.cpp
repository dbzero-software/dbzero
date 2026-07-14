// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "KV_Index.hpp"
#include <iostream>

namespace db0::object_model

{

    KV_Address::KV_Address()
        : as_value()
    {
    }
    
    KV_Address::KV_Address(Address addr)
        : KV_Address()
    {
        static_assert(sizeof(as_value) >= sizeof(as_ptr));
        as_ptr.m_addr = addr.getOffset();
    }
    
    KV_Address::operator Address() const {
        return Address::fromOffset(as_ptr.m_addr);
    }
    
    KV_Address::operator bool() const {
        return as_ptr.m_addr != 0;
    }
    
    bool KV_Address::operator!=(const KV_Address &other) const {
        // byte-wise compare
        return memcmp(this, &other, sizeof(KV_Address)) != 0;
    }
    
    KV_Index::KV_Index(Memspace &memspace)
        : db0::MorphingBIndex<XValue, KV_Address>(memspace)
    {
    }
    
    KV_Index::KV_Index(Memspace &memspace, XValue value)
        : db0::MorphingBIndex<XValue, KV_Address>(memspace, value)
    {
    }
    
    KV_Index::KV_Index(std::pair<Memspace*, KV_Address> addr, bindex::type type)
        : db0::MorphingBIndex<XValue, KV_Address>(*addr.first, addr.second, type)
    {
    }
    
    bool KV_Index::operator==(const KV_Index &other) const
    {
        // the ordering and the actual key / values must be identical
        auto it = this->beginJoin(1);
        auto other_it = other.beginJoin(1);
        while (!it.is_end() && !other_it.is_end()) {
            if (!(*it).equalTo(*other_it)) {
                return false;
            }
            ++it;
            ++other_it;
        }
        return it.is_end() && other_it.is_end();
    }

}