#include "KV_Index.hpp"
#include <iostream>

namespace db0::object_model

{
    
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