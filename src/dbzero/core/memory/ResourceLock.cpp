// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ResourceLock.hpp"
#include <iostream>
#include <cstring>
#include <cassert>
#include <algorithm>
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
    
    const std::byte ResourceLock::m_cow_zero = std::byte(0);
    
    ResourceLock::ResourceLock(StorageContext storage_context, std::uint64_t address, std::size_t size,
        FlagSet<AccessOptions> access_mode, std::shared_ptr<ResourceLock> cow_lock)
        : m_context(storage_context)
        , m_address(address)
        , m_access_mode(access_mode)
        , m_data(size, static_cast<std::byte>(0))
        , m_cow_lock(cow_lock)
    {
        assert(!m_cow_lock || m_cow_lock->size() == this->size());
#ifndef NDEBUG        
        rl_usage += this->size();
        ++rl_count;
        ++rl_op_count;
#endif
    }
    
    ResourceLock::ResourceLock(std::shared_ptr<ResourceLock> lock, FlagSet<AccessOptions> access_mode)
        : m_context(lock->m_context)
        , m_address(lock->m_address)
        // copy-on-write, the recycled flag must be erased
        , m_resource_flags(
            (lock->m_resource_flags & ~(db0::RESOURCE_RECYCLED | db0::RESOURCE_DIRTY))            
        )
        , m_access_mode(access_mode)
        , m_data(lock->m_data)
        , m_cow_lock(lock)
    {
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
        
    bool ResourceLock::resetDirtyFlag()
    {
        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                m_diffs.clear();                
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
            // if dirty, we need to register with the dirty cache (unless exliclity marked no_dirty_cache)
            if (isDirty() && !m_access_mode[AccessOptions::no_dirty_cache]) {
                m_context.m_cache_ref.get().append(shared_from_this());
            }
        }
    }
    
    void ResourceLock::moveFrom(ResourceLock &other)
    {
        assert(other.size() == size());
        setDirty();
        std::memcpy(m_data.data(), other.m_data.data(), m_data.size());
        m_diffs = other.m_diffs;    
        other.discard();
    }
    
    void ResourceLock::setDirty()
    {
        if (atomicCheckAndSetFlags(m_resource_flags, db0::RESOURCE_DIRTY)) {
            // register lock with the dirty cache
            // NOTE: locks marked no_dirty_cache (e.g. BoundaryLock) or no_flush (atomic locks) are not registered with the dirty cache
            if (!m_access_mode[AccessOptions::no_dirty_cache] && !m_access_mode[AccessOptions::no_flush]) {
                // register with the dirty cache
                m_context.m_cache_ref.get().append(shared_from_this());
            }
        }
    }
    
    void ResourceLock::freeze() {
        atomicCheckAndSetFlags(m_resource_flags, db0::RESOURCE_FREEZE);
    }
    
    void ResourceLock::setDirty(std::uint64_t at, std::uint64_t end)
    {
        assert(at >= m_address);
        assert(end <= this->m_address + this->size());
        
        if (end == at) {
            // no need to track empty ranges
            return;
        }
        
        // if unable to fit, then mark the entire lock as dirty
        if ((end - m_address) >= std::numeric_limits<std::uint16_t>::max()) {
            m_diffs.setOverflow();
            return;
        }
        
        m_diffs.insert(at - m_address, end - m_address, MAX_DIFF_RANGES);
        setDirty();
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
    
    const std::byte *ResourceLock::getCowPtr() const
    {
        if (m_cow_lock) {
            return (const std::byte*)m_cow_lock->getBuffer();
        }
        if (m_cow_data.size()) {
            return m_cow_data.data();
        }
        if (m_access_mode[AccessOptions::create]) {
            return &m_cow_zero;
        }        
        return nullptr;
    }
    
    bool ResourceLock::hasCoWData() const {
        return m_cow_lock || !m_cow_data.empty() || m_access_mode[AccessOptions::create];
    }
    
    std::size_t ResourceLock::usedMem() const
    {
        std::size_t result = m_data.size() + sizeof(*this);
        // assume potential CoW buffer
        if (!m_access_mode[AccessOptions::no_cow]) {
            result += m_data.size();
        }
        return result;
    }
    
    std::uint64_t ResourceLock::getAddressOf(const void *ptr) const
    {
        assert(ptr >= m_data.data() && ptr < m_data.data() + m_data.size());
        return m_address + static_cast<const std::byte*>(ptr) - m_data.data();
    }
    
    bool ResourceLock::getDiffs(const void *buf, std::vector<std::uint16_t> &result) const
    {
        if (m_diffs.isOverflow()) {
            // unable to diff-flush, must write the entire page
            return false;
        }
        if (buf == &m_cow_zero) {
            return db0::getDiffs(m_data.data(), this->size(), result, 0, {}, m_diffs);
        } else {
            return db0::getDiffs(buf, m_data.data(), this->size(), result, 0, {}, m_diffs);
        }
    }
    
    bool ResourceLock::getDiffs(std::vector<std::uint16_t> &result) const
    {        
        auto cow_ptr = getCowPtr();
        if (!cow_ptr) {
            // unable to diff-flush
            return false;
        }
        return this->getDiffs(cow_ptr, result);
    }

    std::size_t ResourceLock::getPageSize() const {
        return m_context.m_storage_ref.get().getPageSize();
    }

}