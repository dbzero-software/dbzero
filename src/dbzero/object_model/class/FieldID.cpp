// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "FieldID.hpp"

namespace db0::object_model

{

    std::uint32_t FieldID::maybeOffset() const {
        return m_value ? getOffset() : 0;            
    }
    
}

namespace std

{

    std::ostream &operator<<(std::ostream &os, const db0::object_model::FieldID &field_id)
    {
        if (field_id) {
            os << field_id.getIndex() << "/" << field_id.getOffset();
        } else {
            os << "null";
        }
        return os;
    }

}