// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "mptr.hpp"
#include "Memspace.hpp"

namespace db0

{

    std::size_t mptr::getPageSize() const
    {        
        return m_memspace.get().getPageSize();
    }

}