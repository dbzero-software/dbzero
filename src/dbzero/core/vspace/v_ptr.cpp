#include <dbzero/core/vspace/v_ptr.hpp>

namespace db0

{
    
    inline std::uint32_t getResourceFlags(FlagSet<AccessOptions> access_mode)
    {
        return (access_mode[AccessOptions::read] ? db0::RESOURCE_AVAILABLE_FOR_READ : 0)
            | (access_mode[AccessOptions::write] ? db0::RESOURCE_AVAILABLE_FOR_WRITE : 0);
    }
    
    vtypeless::vtypeless(Memspace &memspace, std::uint64_t address, FlagSet<AccessOptions> access_mode)
        : m_address(address)        
        , m_memspace_ptr(&memspace)
        , m_access_mode(access_mode)
    {
    }

    vtypeless::vtypeless(const vtypeless &other)
        : m_memspace_ptr(other.m_memspace_ptr)
    {
        *this = other;
    }

    vtypeless::vtypeless(Memspace &memspace, std::uint64_t address, MemLock &&mem_lock, FlagSet<AccessOptions> access_mode)
        : m_address(address)
        , m_memspace_ptr(&memspace)
        // mark the resource as available
        , m_resource_flags(getResourceFlags(access_mode))
        , m_access_mode(access_mode)
        , m_mem_lock(std::move(mem_lock))
    {
        // resource must be available
        assert(m_mem_lock.m_buffer);
    }

    FlagSet<AccessOptions> vtypeless::getAccessMode() const
    {
        return m_access_mode;
    }

    vtypeless &vtypeless::operator=(const vtypeless &other) 
    {
        m_address = other.m_address;
        m_memspace_ptr = other.m_memspace_ptr;
        m_resource_flags = other.m_resource_flags.load();
        m_mem_lock = other.m_mem_lock;
        return *this;
    }

    unsigned int vtypeless::use_count() const
    {
        return m_mem_lock.use_count();
    }

    bool vtypeless::isAttached() const
    {
        return m_mem_lock.m_buffer != nullptr;
    }
    
    void vtypeless::detach()
    {
        // detaching clears the reasource available for read flag
        while (ResourceDetachMutexT::__ref(m_resource_flags).get()) {
            ResourceDetachMutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                m_mem_lock = {};
                // clear read/write flags
                lock.commit_reset();
            }
        }
    }
    
    void vtypeless::commit()
    {
        // commit clears the reasource available for write flag
        // it might still be available for read
        safeResetFlags(m_resource_flags, db0::RESOURCE_AVAILABLE_FOR_WRITE);
    }
    
}