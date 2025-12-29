// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Field.hpp"

namespace db0::object_model

{
    
    o_field::o_field(RC_LimitedStringPool &string_pool, const char *name)
        : m_name(string_pool.addRef(name))
    {
    }
    
}
