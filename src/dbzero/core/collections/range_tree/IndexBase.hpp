// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <array>
#include <dbzero/core/serialization/FixedVersioned.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/object_model/object_header.hpp>
#include <dbzero/bindings/TypeId.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{

    // known index implementations
    enum class IndexType: std::uint16_t
    {
        Unknown = 0,
        RangeTree = 1
    };

    enum class IndexDataType: std::uint16_t
    {
        Unknown = 0,
        // type will be auto-assigned on first non-null element added
        Auto = 1,
        Int64 = 2,
        UInt64 = 3
    };
    
DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_index: public o_fixed_versioned<o_index>
    {
        // common object header
        o_unique_header m_header;
        IndexType m_type;
        IndexDataType m_data_type = IndexDataType::Auto;
        // address of the actual index instance
        Address m_index_addr = {};
        
        o_index(IndexType, IndexDataType);
        // header not copied
        o_index(const o_index &other);

        bool hasRefs() const {
            return m_header.hasRefs();
        }
    };
DB0_PACKED_END
    
    using IndexBase = db0::v_object<o_index>;
    
    IndexDataType getIndexDataType(db0::bindings::TypeId);

    template <typename T> std::shared_ptr<T> tryGetRangeTree(IndexBase &index)
    {
        if (!index->m_index_addr.isValid()) {
            return nullptr;
        }
        assert(index->m_type == IndexType::RangeTree);
        // pull an existing instance
        return std::make_shared<T>(index.myPtr(index->m_index_addr));
    }
    
}