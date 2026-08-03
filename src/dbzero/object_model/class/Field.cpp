// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "Field.hpp"

namespace db0::object_model

{
    
    o_field::o_field(RC_LimitedStringPool &string_pool, const char *name)
        : m_name(string_pool.addRef(name))
    {
    }
    
}
