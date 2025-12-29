// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "IndexBase.hpp"

namespace db0

{
    
    using TypeId = db0::bindings::TypeId;
    
    o_index::o_index(IndexType type, IndexDataType data_type)
        : m_type(type)
        , m_data_type(data_type)
    {
    }
    
    o_index::o_index(const o_index &other)
        : m_type(other.m_type)
        , m_data_type(other.m_data_type)        
    {
    }
    
    IndexDataType getIndexDataType(TypeId type_id)
    {
        switch (type_id) {
            case TypeId::INTEGER:
                return IndexDataType::Int64;
            case TypeId::DATETIME:
            case TypeId::DATETIME_TZ:
            case TypeId::DATE:
            case TypeId::TIME:
            case TypeId::TIME_TZ:
            case TypeId::DECIMAL:
                return IndexDataType::UInt64;
            default:
                THROWF(db0::InputException) << "Unsupported index key type: " 
                    << static_cast<std::uint16_t>(type_id) << THROWF_END;
        }
    }

}