#include <dbzero/core/memory/ResourceLock.hpp>
#include <iostream>
#include <cstring>
#include <cassert>

namespace db0

{

    ResourceLock::ResourceLock(StorageView &storage_view, std::uint64_t address, std::size_t size,
        FlagSet<AccessOptions> access_mode, bool create_new)
        : ResourceLock(tag_derived{}, storage_view, address, size, access_mode, create_new)
    {
        assert(addrPageAligned());
    }

    ResourceLock::ResourceLock(const ResourceLock &lock, std::uint64_t src_state_num, 
        FlagSet<AccessOptions> access_mode)
        : ResourceLock(tag_derived{}, lock, src_state_num, access_mode)
    {        
        assert(addrPageAligned());
    }

    ResourceLock::ResourceLock(tag_derived, StorageView &storage_view, std::uint64_t address, std::size_t size,
        FlagSet<AccessOptions> access_mode, bool create_new)
        : m_storage_view(storage_view)
        , m_address(address)
        , m_resource_flags(create_new?(db0::RESOURCE_FETCHED | db0::RESOURCE_DIRTY):0)
        , m_access_mode(access_mode)
        , m_data(size)
    {        
        if (create_new) {
            std::memset(m_data.data(), 0, size);
        }
    }
    
    ResourceLock::ResourceLock(tag_derived, const ResourceLock &lock, std::uint64_t src_state_num, FlagSet<AccessOptions> access_mode)
        : m_storage_view(lock.m_storage_view)
        , m_address(lock.getAddress())
        // keep storage access flags from 'lock'
        // copy-on-write, assume dirty, the recycled flag must be erased
        , m_resource_flags((lock.m_resource_flags | db0::RESOURCE_DIRTY) & ~db0::RESOURCE_RECYCLED)
        , m_access_mode(lock.m_access_mode)
        // create vector from data buffer (copy-on-write)
        , m_data(lock.copyData(src_state_num))
    {        
    }
    
    ResourceLock::~ResourceLock()
    {
        // make sure the dirty flag is not set
        assert(!isDirty());
    }
    
    bool ResourceLock::addrPageAligned() const
    {
        return m_address % m_storage_view.get().first.get().getPageSize() == 0;
    }

    void ResourceLock::setRecycled(bool is_recycled) 
    {
        if (is_recycled) {
            safeSetFlags(m_resource_flags, RESOURCE_RECYCLED);
        } else {
            safeResetFlags(m_resource_flags, RESOURCE_RECYCLED);
        }
    }
    
    bool ResourceLock::isCached() const {
        return !m_access_mode[AccessOptions::no_cache];
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
    
    void *ResourceLock::getBuffer(std::uint64_t state_num) const
    {
        using MutexT = ResourceFetchMutexT;
        while (!MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                // read into the local buffer
                auto storage = m_storage_view.get().first;
                storage.get().read(m_address, state_num, m_data.size(), m_data.data(), m_access_mode);
                lock.commit_set();
            }
        }
        return m_data.data();
    }
    
    void *ResourceLock::getBuffer(std::uint64_t address, std::uint64_t state_num) const
    {
        assert(address >= m_address && address < m_address + m_data.size());
        return static_cast<std::byte*>(getBuffer(state_num)) + address - m_address;
    }

    std::vector<std::byte> ResourceLock::copyData(std::uint64_t state_num) const
    {
        auto ptr = (std::byte*)getBuffer(state_num);
        return std::vector<std::byte>(ptr, ptr + m_data.size());
    }

    void ResourceLock::flush()
    {
        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                auto [storage, state_num] = m_storage_view.get();
                // write from the local buffer
                storage.get().write(m_address, state_num, m_data.size(), m_data.data());
                // reset the dirty flag
                lock.commit_reset();
            }
        }
    }
    
    const StorageView &ResourceLock::getStorageView() const {
        return m_storage_view;
    }
    
    void ResourceLock::release()
    {
        // FIXME: log
        std::cout << "ResourceLock::release() called" << std::endl;

        assert(!isDirty());
        // clear the resource fetched flag
        safeResetFlags(m_resource_flags, RESOURCE_FETCHED);
    }
    
}