// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "XValue.hpp"
#include <dbzero/object_model/object/lofi_store.hpp>

namespace db0::object_model

{

    bool XValue::operator<(const XValue &other) const {
        return getIndex() < other.getIndex();
    }

    bool XValue::operator<(std::uint32_t index) const {
        return getIndex() < index;
    }
    
    bool XValue::operator==(std::uint32_t index) const {
        return getIndex() == index;
    }
    
    bool XValue::operator==(const XValue &other) const {
        return getIndex() == other.getIndex();
    }
    
    bool XValue::operator!=(const XValue &other) const {
        return getIndex() != other.getIndex();        
    }
    
    bool XValue::equalTo(const XValue &other, unsigned int offset) const
    {
        if (m_type == StorageClass::PACK_2 && other.m_type == StorageClass::PACK_2) {
            const std::uint64_t mask = lofi_store<2>::mask(offset);
            return ((m_value.m_store & mask) == (other.m_value.m_store & mask))
                && (getIndex() == other.getIndex());
        }
        return std::memcmp(this, &other, sizeof(XValue)) == 0;
    }

    bool XValue::equalTo(const XValue &other) const {
        return std::memcmp(this, &other, sizeof(XValue)) == 0;
    }
    
}