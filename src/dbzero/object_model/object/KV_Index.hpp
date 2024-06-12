#pragma once

#include <dbzero/object_model/value/XValue.hpp>
#include <dbzero/core/collections/b_index/bindex_types.hpp>
#include <dbzero/core/collections/b_index/mb_index.hpp>

namespace db0::object_model

{   

    // Represents a pointer to a known b-index type
    struct [[gnu::packed]] KV_Ptr
    {
        std::uint64_t m_addr;
    };



    // Union of XValue & KV_Ptr
    template <typename ValueT>
    union [[gnu::packed]] UnionAddress
    {
        KV_Ptr as_ptr;
        ValueT as_value;
        
        UnionAddress<ValueT>(){
            as_ptr.m_addr = 0;
        }
        UnionAddress<ValueT>(std::uint64_t addr){
            as_ptr.m_addr = addr;
        }

        operator std::uint64_t() const{
            return as_ptr.m_addr;
        }
        operator bool () const{
            return as_ptr.m_addr != 0;
        }
        
        // binary compare
        bool operator!=(const UnionAddress<ValueT> &other) const{
            return memcmp(this, &other, sizeof(UnionAddress<ValueT>)) != 0;
        }
    };

    using KV_Address = UnionAddress<XValue>;
    
    // Key-Value index for field storage
    // the implementation is based on morphing-b-index
    class KV_Index: public db0::MorphingBIndex<XValue, KV_Address>
    {
    public:
        KV_Index(Memspace &);
        // construct populated with a single element
        KV_Index(Memspace &, XValue);
        KV_Index(std::pair<Memspace*, KV_Address>, bindex::type);
    };
    
}