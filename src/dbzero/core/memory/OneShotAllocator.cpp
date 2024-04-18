#include "OneShotAllocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    OneShotAllocator::OneShotAllocator(std::uint64_t addr, std::size_t size)
        : m_addr(addr)        
        , m_size(size)
    {
    }

    std::optional<std::uint64_t> OneShotAllocator::tryAlloc(std::size_t size) 
    {
        if (size != m_size || m_allocated) {
            return std::nullopt;
        }
        m_allocated = true;
        return m_addr;
    }
    
    void OneShotAllocator::free(std::uint64_t address) 
    {
        if (address != m_addr || !m_allocated) {
            THROWF(db0::InternalException) << "OneShotAllocator invalid address: " << address;
        }
        m_allocated = false;
    }
    
    std::size_t OneShotAllocator::getAllocSize(std::uint64_t address) const 
    {
        if (address != m_addr || !m_allocated) {
            THROWF(db0::InternalException) << "OneShotAllocator invalid address: " << address;
        }
        return m_size;
    }

    void OneShotAllocator::commit() {
        // nothing to do
    }
    
}