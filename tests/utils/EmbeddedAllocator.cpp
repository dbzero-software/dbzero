#include "EmbeddedAllocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    std::optional<std::uint64_t> EmbeddedAllocator::tryAlloc(std::size_t size)
    {
        auto new_address = 4096 * ++m_count;
        m_allocations[new_address] = size;
        return new_address;
    }
        
    void EmbeddedAllocator::free(std::uint64_t address)
    {
        auto it = m_allocations.find(address);
        if (it == m_allocations.end()) {
            THROWF(db0::InternalException) << "address not found: " << address;
        }
        m_allocations.erase(it);        
    }

    std::size_t EmbeddedAllocator::getAllocSize(std::uint64_t address) const
    {
        auto it = m_allocations.find(address);
        if (it == m_allocations.end()) {
            THROWF(db0::InternalException) << "address not found: " << address;
        }
        return it->second;
    }
        
    void EmbeddedAllocator::commit()
    {
        // nothing to do
    }
    
}