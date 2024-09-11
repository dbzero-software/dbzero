#include "KV_Index.hpp"
#include <iostream>

namespace db0::object_model

{

    KV_Address::KV_Address() {
        as_ptr.m_addr = 0;
    }

    KV_Address::KV_Address(std::uint64_t addr) {
        as_ptr.m_addr = addr;        
    }

    KV_Address::operator std::uint64_t() const {
        return as_ptr.m_addr;
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

}