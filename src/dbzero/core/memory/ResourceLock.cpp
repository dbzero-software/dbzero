#include "ResourceLock.hpp"
#include <iostream>
#include <cstring>
#include <cassert>
#include <dbzero/core/storage/BaseStorage.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include "PrefixCache.hpp"

namespace db0

{   
    
#ifndef NDEBUG
    std::atomic<std::size_t> ResourceLock::rl_usage = 0;
    std::atomic<std::size_t> ResourceLock::rl_count = 0;
    std::atomic<std::size_t> ResourceLock::rl_op_count = 0;
#endif

    ResourceLock::ResourceLock(StorageContext storage_context, std::uint64_t address, std::size_t size,
        FlagSet<AccessOptions> access_mode, std::uint16_t mu_size)
        : m_context(storage_context)
        , m_address(address)
        , m_resource_flags(
            (access_mode[AccessOptions::write] ? db0::RESOURCE_DIRTY : 0) |
            (access_mode[AccessOptions::no_cache] ? db0::RESOURCE_NO_CACHE : 0) )
        , m_access_mode(access_mode)
        , m_data(size + mu_size)
        , m_mu_size(mu_size)
    {
        // intialize buffer for write-only access (create)
        if (!access_mode[AccessOptions::read]) {
            std::memset(m_data.data(), 0, this->size());
        }
        if (m_mu_size > 0) {
            createMUStore(mu_size);
        }
#ifndef NDEBUG        
        rl_usage += this->size();
        ++rl_count;
        ++rl_op_count;
#endif
    }
    
    ResourceLock::ResourceLock(const ResourceLock &lock, FlagSet<AccessOptions> access_mode)
        : m_context(lock.m_context)
        , m_address(lock.m_address)
        // copy-on-write, assume dirty, the recycled flag must be erased
        , m_resource_flags(
            ((lock.m_resource_flags | db0::RESOURCE_DIRTY) & ~db0::RESOURCE_RECYCLED) |
            (access_mode[AccessOptions::no_cache] ? db0::RESOURCE_NO_CACHE : 0) )
        , m_access_mode(access_mode)
        , m_data(lock.m_data)
        // NOTE: that a the new lock's mu-store is created initially empty
        , m_mu_size(lock.m_mu_size)
    {
        getMUStore().clear();
#ifndef NDEBUG
        rl_usage += this->size();
        ++rl_count;
        ++rl_op_count;
#endif      
    }
    
    ResourceLock::~ResourceLock()
    {
#ifndef NDEBUG        
        rl_usage -= this->size();
        --rl_count;
        ++rl_op_count;
#endif
        // make sure the dirty flag is not set
        // NOTE: to avoid triggering this assert for unused volatile locks, call "resetDirtyFlag" without flushing        
        assert(!isDirty());
    }
    
    void ResourceLock::initDirty()
    {
        if (isDirty() && !m_access_mode[AccessOptions::no_cache] && !m_access_mode[AccessOptions::no_flush]) {
            m_context.m_cache_ref.get().append(shared_from_this());
        }
    }

    bool ResourceLock::addrPageAligned(BaseStorage &storage) const {
        return m_address % storage.getPageSize() == 0;
    }
    
    void ResourceLock::setRecycled(bool is_recycled)
    {
        if (is_recycled) {
            atomicSetFlags(m_resource_flags, RESOURCE_RECYCLED);
        } else {
            atomicResetFlags(m_resource_flags, RESOURCE_RECYCLED);
        }
    }
    
    bool ResourceLock::isCached() const {
        return !(m_resource_flags & db0::RESOURCE_NO_CACHE);
    }
    
    bool ResourceLock::resetDirtyFlag()
    {
        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                // also clear the associated MU-store if it exists
                if (m_mu_size > 0) {
                    getMUStore().clear();
                }
                lock.commit_reset();
                // dirty flag successfully reset by this thread
                return true;
            }
        }

        return false;
    }
    
    void ResourceLock::discard() {
        resetDirtyFlag();
    }

    void ResourceLock::resetNoFlush()
    {
        if (m_access_mode[AccessOptions::no_flush]) {
            m_access_mode.set(AccessOptions::no_flush, false);
            // if dirty, we need to register with the dirty cache
            if (isDirty() && !m_access_mode[AccessOptions::no_cache]) {
                m_context.m_cache_ref.get().append(shared_from_this());
            }
        }
    }
     
    void ResourceLock::moveFrom(ResourceLock &other)
    {
        assert(other.size() == size());
        setDirty();
        // copy both data and the mu-store
        std::memcpy(m_data.data(), other.m_data.data(), m_data.size());        
        other.discard();
    }
    
    void ResourceLock::setDirty()
    {
        // NOTE: locks marked no_cache (e.g. BoundaryLock) or no_flush (atomic locks) are not registered with the dirty cache        
        if (atomicCheckAndSetFlags(m_resource_flags, db0::RESOURCE_DIRTY)) {
            // mark the entire range as modified
            if (m_mu_size > 0) {
                getMUStore().appendFullRange();
            }
            // register lock with the dirty cache
            if (!m_access_mode[AccessOptions::no_cache] && !m_access_mode[AccessOptions::no_flush]) {
                m_context.m_cache_ref.get().append(shared_from_this());
            }
        }
    }
    
#ifndef NDEBUG
    std::pair<std::size_t, std::size_t> ResourceLock::getTotalMemoryUsage() 
    {
        // NOTE: we subtract DRAM_Prefix utilized locks since they are reported separately
        auto dp_usage = DRAM_Prefix::getTotalMemoryUsage();
        return { rl_usage - dp_usage.first, rl_count - dp_usage.second };
    }
#endif
    
    std::ostream &showBytes(std::ostream &os, const std::byte *data, std::size_t size)
    {
        for (std::size_t i = 0; i < size; ++i) {
            os << std::hex << static_cast<int>(data[i]) << " ";
        }
        os << std::dec;
        return os;
    }

#ifndef NDEBUG
    bool ResourceLock::isVolatile() const {
        return m_access_mode[AccessOptions::no_flush];
    }
#endif        

    void ResourceLock::createMUStore(std::uint16_t mu_size) {
        o_mu_store::__new(&getMUStore(), mu_size);
    }

}