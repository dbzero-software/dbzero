// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "StringPools.hpp"

namespace db0::pools

{

    RC_LimitedStringPool::RC_LimitedStringPool(const Memspace &pool_memspace, Memspace &memspace)
        : super_t(pool_memspace, memspace)
    {        
    }

    RC_LimitedStringPool::RC_LimitedStringPool(const Memspace &pool_memspace, mptr ptr)
        : super_t(pool_memspace, ptr)
    {
    }

    RC_LimitedStringPool::PtrT RC_LimitedStringPool::add(bool &inc_ref, const char *value) {
        return super_t::add(inc_ref, value);
    }
    
    RC_LimitedStringPool::PtrT RC_LimitedStringPool::add(bool &inc_ref, const std::string &value) {
        return super_t::add(inc_ref, value);
    }

    RC_LimitedStringPool::PtrT RC_LimitedStringPool::addRef(const char *value) {
        return super_t::addRef(value);
    }

    RC_LimitedStringPool::PtrT RC_LimitedStringPool::addRef(const std::string &value) {
        return super_t::addRef(value);
    }
    
    void RC_LimitedStringPool::unRef(PtrT ptr) {
        super_t::unRefByAddr(ptr.m_value);
    }

    RC_LimitedStringPool::PtrT RC_LimitedStringPool::get(const char *str_value) const
    {
        typename super_t::AddressT value;        
        if (super_t::find(str_value, value)) {
            return value;
        }
        
        // not found
        return {};
    }
    
    RC_LimitedStringPool::PtrT RC_LimitedStringPool::get(const std::string &value) const {
        return get(value.c_str());
    }

    std::string RC_LimitedStringPool::fetch(PtrT ptr) const
    {
        MemLock lock;
        return super_t::fetch<const ItemT&>(ptr.m_value, lock).second();
    }
    
    std::uint64_t RC_LimitedStringPool::toAddress(PtrT ptr) const {
        // FIXME: convert to address when this functionality is available
        return ptr.m_value;
    }
    
}