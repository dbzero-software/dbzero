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

    RC_LimitedStringPool::PtrT RC_LimitedStringPool::add(const char *value) {
        return super_t::add(value);
    }

    RC_LimitedStringPool::PtrT RC_LimitedStringPool::get(const char *str_value, bool create) 
    {
        using AddressT = typename super_t::AddressT;
        if (create) {
            return super_t::add(str_value);
        }
        AddressT value;
        if (super_t::find(str_value, value)) {
            return value;
        }
        
        // not found
        return {};
    }

    RC_LimitedStringPool::PtrT RC_LimitedStringPool::add(const std::string &value) {
        return super_t::add(value);
    }

    RC_LimitedStringPool::PtrT RC_LimitedStringPool::get(const std::string &value, bool create) {
        return get(value.c_str(), create);
    }

    std::string RC_LimitedStringPool::fetch(PtrT ptr) const {
        return super_t::fetch<std::string>(ptr.m_value);
    }
    
    std::uint64_t RC_LimitedStringPool::toAddress(PtrT ptr) const
    {
        // FIXME: convert to address when this functionality is available
        return ptr.m_value;
    }
    
}