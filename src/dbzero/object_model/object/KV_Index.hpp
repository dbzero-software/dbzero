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
    union [[gnu::packed]] KV_Address
    {
        KV_Ptr as_ptr;
        XValue as_value;
        
        KV_Address();
        KV_Address(std::uint64_t);

        operator std::uint64_t() const;
        operator bool () const;
        
        // binary compare
        bool operator!=(const KV_Address &) const;
    };
    
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