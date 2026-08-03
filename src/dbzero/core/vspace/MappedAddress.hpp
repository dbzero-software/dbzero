// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <dbzero/core/memory/MemLock.hpp>
#include <dbzero/core/memory/Allocator.hpp>

namespace db0

{

    /// The address with the corresponding mapped range (i.e. MemLock instance)
    struct MappedAddress
    {
        Address m_address = {};
        MemLock m_mem_lock;
    };
    
}