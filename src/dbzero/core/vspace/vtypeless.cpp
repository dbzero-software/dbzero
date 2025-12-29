// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "vtypeless.hpp"

namespace db0

{
        
    vtypeless::vtypeless(Memspace &memspace, Address address, FlagSet<AccessOptions> access_mode)
        : m_address(address)        
        , m_memspace_ptr(&memspace)
        , m_access_mode(access_mode)
    {
        assertFlags();
        assert(!(m_resource_flags.load() & RESOURCE_LOCK));
    }

    vtypeless::vtypeless(const vtypeless &other) {
        *this = other;
    }
    
    vtypeless::vtypeless(vtypeless &&other) {
        *this = std::move(other);
    }
    
    vtypeless::vtypeless(Memspace &memspace, Address address, MemLock &&mem_lock, std::uint16_t resource_flags,
        FlagSet<AccessOptions> access_mode)
        : m_address(address)
        , m_memspace_ptr(&memspace)
        // mark the resource as available
        , m_resource_flags(resource_flags)
        , m_access_mode(access_mode)
        , m_mem_lock(std::move(mem_lock))
    {
        assertFlags();
        // resource must be available
        assert(m_mem_lock.m_buffer);
    }
    
    vtypeless &vtypeless::operator=(const vtypeless &other)
    {        
        m_address = other.m_address;
        m_memspace_ptr = other.m_memspace_ptr;
        m_access_mode = other.m_access_mode;
        m_cached_size = other.m_cached_size;

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
        m_cached_size = other.m_cached_size;

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
        
        // invalidate the other instance
        other.m_address = {};
        other.m_memspace_ptr = nullptr;
    }
    
    unsigned int vtypeless::use_count() const {
        return m_mem_lock.use_count();
    }
    
    bool vtypeless::isAttached() const {
        return m_mem_lock.m_buffer != nullptr;
    }
    
    void vtypeless::detach() const
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
    
    void vtypeless::commit() const
    {
        /* FIXME:
        // NOTE: this operation assumes that only one v_object instance pointing to the same address exists
        // otherwise modifications done to one instance will not be visible to the other instances
        // this assumption holds true for dbzero objects but if unable to fulfill in the future,
        // it must be changed to "this->detach()"

        // commit clears the reasource available for write flag
        // it might still be available for read
        atomicResetFlags(m_resource_flags, db0::RESOURCE_AVAILABLE_FOR_WRITE);
        */
        detach();
    }
    
}