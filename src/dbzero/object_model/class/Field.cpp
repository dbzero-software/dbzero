// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Field.hpp"

namespace db0::object_model

{
    
    o_field::o_field(RC_LimitedStringPool &string_pool, const char *name)
        : m_name(string_pool.addRef(name))
    {
    }
    
}
