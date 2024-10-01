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
        assert(!(m_resource_flags.load() & RESOURCE_LOCK));
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
    
    FlagSet<AccessOptions> vtypeless::getAccessMode() const {
        return m_access_mode;
    }
    
    vtypeless &vtypeless::operator=(const vtypeless &other)
    {
        m_address = other.m_address;
        m_memspace_ptr = other.m_memspace_ptr;
        m_access_mode = other.m_access_mode;

        // try locking for copy
        for (;;) {
            ResourceDetachMutexT::WriteOnlyLock lock(other.m_resource_flags);
            // NOTE: if unable to lock the resource will be retrieved from the underlying prefix
            // this is to avoid deadlocks due to possible recursive dependencies
            if (lock.isLocked()) {
                // clear the lock flag when copying
                m_resource_flags = (other.m_resource_flags.load() & ~RESOURCE_LOCK);
                m_mem_lock = other.m_mem_lock;
                assert(!(m_resource_flags.load() & db0::RESOURCE_AVAILABLE_FOR_READ) || m_mem_lock.m_buffer);
                break;
            }
        }

        return *this;
    }
    
    void vtypeless::operator=(vtypeless &&other)
    {        
        m_address = other.m_address;
        m_memspace_ptr = other.m_memspace_ptr;
        m_access_mode = other.m_access_mode;

        // try locking for copy
        for (;;) {
            ResourceDetachMutexT::WriteOnlyLock lock(other.m_resource_flags);
            // NOTE: if unable to lock the resource will be retrieved from the underlying prefix
            // this is to avoid deadlocks due to possible recursive dependencies        
            if (lock.isLocked()) {
                // clear the lock flag when copying
                m_resource_flags = (other.m_resource_flags.load() & ~RESOURCE_LOCK);
                m_mem_lock = std::move(other.m_mem_lock);
                assert(!(m_resource_flags.load() & db0::RESOURCE_AVAILABLE_FOR_READ) || m_mem_lock.m_buffer);
                break;
            }
        }
    }
    
    unsigned int vtypeless::use_count() const {
        return m_mem_lock.use_count();
    }

    bool vtypeless::isAttached() const {
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
                break;
            }
        }        
    }
    
    void vtypeless::commit()
    {
        // commit clears the reasource available for write flag
        // it might still be available for read
        atomicResetFlags(m_resource_flags, db0::RESOURCE_AVAILABLE_FOR_WRITE);
        // clear write / create flags since the following access may not be for update
        m_access_mode.set(AccessOptions::write, false);
        m_access_mode.set(AccessOptions::create, false);
    }
    
}