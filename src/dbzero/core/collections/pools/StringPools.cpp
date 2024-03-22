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

    RC_LimitedStringPool::PtrT RC_LimitedStringPool::add(const std::string &value) {
        return super_t::add(value);
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