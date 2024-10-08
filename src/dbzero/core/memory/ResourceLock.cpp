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
        FlagSet<AccessOptions> access_mode, bool create_new)
        : m_context(storage_context)
        , m_address(address)
        , m_resource_flags(
            (access_mode[AccessOptions::write] ? db0::RESOURCE_DIRTY : 0) | 
            (access_mode[AccessOptions::no_cache] ? db0::RESOURCE_NO_CACHE : 0) )
        , m_access_mode(access_mode)            
        , m_data(size)
    {
        if (create_new) {
            std::memset(m_data.data(), 0, m_data.size());
        }        
#ifndef NDEBUG        
        rl_usage += m_data.size();
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
    {
#ifndef NDEBUG
        rl_usage += m_data.size();
        ++rl_count;
        ++rl_op_count;
#endif      
    }
    
    ResourceLock::ResourceLock(ResourceLock &&other, std::vector<std::byte> &&data)
        : m_context(other.m_context)
        , m_address(other.m_address)
        , m_resource_flags(other.m_resource_flags.load())
        , m_access_mode(other.m_access_mode)
        , m_data(std::move(data))
        , m_recycle_it(std::move(other.m_recycle_it))
    {
#ifndef NDEBUG
        rl_usage += m_data.size();
        ++rl_count;
        ++rl_op_count;
#endif
    }

    ResourceLock::~ResourceLock()
    {
#ifndef NDEBUG        
        rl_usage -= m_data.size();
        --rl_count;
        ++rl_op_count;
#endif
        // make sure the dirty flag is not set (unless no-flush lock)
        assert(!isDirty() || m_access_mode[AccessOptions::no_flush]);
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
                lock.commit_reset();
                // dirty flag successfully reset by this thread
                return true;
            }
        }

        return false;
    }

    void ResourceLock::resetNoFlush() {
        m_access_mode.set(AccessOptions::no_flush, false);
    }
    
    void ResourceLock::copyFrom(const ResourceLock &other)
    {
        assert(other.size() == size());
        setDirty();
        std::memcpy(m_data.data(), other.m_data.data(), m_data.size());
    }
    
    void ResourceLock::setDirty()
    {
        if (atomicCheckAndSetFlags(m_resource_flags, db0::RESOURCE_DIRTY)) {
            m_context.m_cache_ref.get().append(shared_from_this());
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
        return os;
    }

}