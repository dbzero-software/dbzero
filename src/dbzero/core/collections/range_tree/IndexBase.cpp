// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "IndexBase.hpp"

DEFINE_ENUM_VALUES(db0::IndexOptions, "Passive", "Managed")

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
        , m_flags(other.m_flags)
    {
    }

    bool o_index::isPassive() const
    {
        return getObjVer() >= 1 && m_flags[IndexOptions::Passive];
    }

    bool o_index::isManaged() const
    {
        return getObjVer() >= 1 && m_flags[IndexOptions::Managed];
    }

    void o_index::setManaged()
    {
        if (getObjVer() < 1) {
            return;
        }
        m_flags.set(IndexOptions::Passive);
        m_flags.set(IndexOptions::Managed);
    }

    bool isSupportedIndexKeyType(TypeId type_id)
    {
        switch (type_id) {
            case TypeId::INTEGER:
            case TypeId::DATETIME:
            case TypeId::DATETIME_TZ:
            case TypeId::DATE:
            case TypeId::TIME:
            case TypeId::TIME_TZ:
            case TypeId::DECIMAL:
                return true;
            default:
                return false;
        }
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
