#pragma once

#include <dbzero/core/memory/MemLock.hpp>

namespace db0

{

    /// The address with the corresponding mapped range (i.e. MemLock instance)
    struct MappedAddress
    {
        std::uint64_t m_address = 0;
        MemLock m_mem_lock;
    };

}