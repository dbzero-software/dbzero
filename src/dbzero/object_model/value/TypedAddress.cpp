// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "TypedAddress.hpp"

namespace db0::object_model

{

    void TypedAddress::setAddress(Address address) {
        m_value = (m_value & 0xFFFC000000000000) | address.getOffset();
    }
    
    void TypedAddress::setType(StorageClass type) {
        m_value = (m_value & 0x0003FFFFFFFFFFFF) | (static_cast<std::uint64_t>(type) << 50);
    }
        
    TypedAddress toTypedAddress(const std::pair<UniqueAddress, StorageClass> &addr_with_type) {
        return { addr_with_type.second, addr_with_type.first.getAddress() };
    }

}   