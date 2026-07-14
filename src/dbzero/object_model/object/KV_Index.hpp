// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/value/XValue.hpp>
#include <dbzero/core/collections/b_index/bindex_types.hpp>
#include <dbzero/core/collections/b_index/mb_index.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0::object_model

{   

    // Represents a pointer to a known b-index type
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR KV_Ptr
    {
        std::uint64_t m_addr = 0;
    };
DB0_PACKED_END
    
    // Union of XValue & KV_Ptr
DB0_PACKED_BEGIN
    union DB0_PACKED_ATTR KV_Address
    {
        // needs to be declared first to ensure proper default initialization
        XValue as_value;
        KV_Ptr as_ptr;
        
        KV_Address();
        KV_Address(Address);

        operator Address() const;
        operator bool () const;
        
        // binary compare
        bool operator!=(const KV_Address &) const;
    };
DB0_PACKED_END
    
    // Key-Value index for field storage
    // the implementation is based on morphing-b-index
    class KV_Index: public db0::MorphingBIndex<XValue, KV_Address>
    {
    public:
        KV_Index(Memspace &);
        // construct populated with a single element
        KV_Index(Memspace &, XValue);
        KV_Index(std::pair<Memspace*, KV_Address>, bindex::type);
        
        bool operator==(const KV_Index &other) const;
    };
    
}