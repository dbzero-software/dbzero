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
        , m_resource_flags(
            (access_mode[AccessOptions::no_cache] ? db0::RESOURCE_NO_CACHE : 0) 
        )
        , m_access_mode(access_mode)
        , m_data(size)
        , m_cow_lock(cow_lock)
    {
        assert(!m_cow_lock || m_cow_lock->size() == this->size());
        // intialize buffer for write-only access (create)
        if (!access_mode[AccessOptions::read]) {
            std::memset(m_data.data(), 0, this->size());
        }
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
            (lock->m_resource_flags & ~(db0::RESOURCE_RECYCLED | db0::RESOURCE_DIRTY)) |
            (access_mode[AccessOptions::no_cache] ? db0::RESOURCE_NO_CACHE : 0) 
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
    
    bool ResourceLock::isCached() const {
        return !(m_resource_flags & db0::RESOURCE_NO_CACHE);
    }
    
    bool ResourceLock::resetDirtyFlag()
    {
        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                m_diffs.clear();
                m_diffs_overflow = false;
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
        std::memcpy(m_data.data(), other.m_data.data(), m_data.size());
        m_diffs = other.m_diffs;    
        other.discard();
    }
    
    void ResourceLock::setDirty()
    {
        if (atomicCheckAndSetFlags(m_resource_flags, db0::RESOURCE_DIRTY)) {
            // register lock with the dirty cache
            // NOTE: locks marked no_cache (e.g. BoundaryLock) or no_flush (atomic locks) are not registered with the dirty cache        
            if (!m_access_mode[AccessOptions::no_cache] && !m_access_mode[AccessOptions::no_flush]) {
                m_context.m_cache_ref.get().append(shared_from_this());
            }
        }
    }

    void ResourceLock::setDirty(std::uint64_t at, std::uint64_t end)
    {
        assert(at >= m_address);
        assert(end <= this->m_address + this->size());
        
        if (end == at) {
            // no need to track empty ranges
            return;
        }

        // check overflow conditions
        if (m_diffs.size() == MAX_DIFF_RANGES || (end - m_address) >= std::numeric_limits<std::uint16_t>::max()) {
            // do not track more diffs as the limit is reached
            m_diffs_overflow = true;
            m_diffs.clear();
            return;            
        }
        
        m_diffs.emplace_back(at - m_address, end - m_address);
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
    
    bool ResourceLock::getDiffs(const void *buf, std::vector<std::uint16_t> &result) const
    {
        if (buf == &m_cow_zero) {
            return db0::getDiffs(m_data.data(), this->size(), result);
        } else {
            return db0::getDiffs(buf, m_data.data(), this->size(), result);
        }
    }
    
    bool getDiffs(const void *buf_1, const void *buf_2,
        std::size_t size, std::vector<std::uint16_t> &result, std::size_t max_diff, std::optional<std::size_t> max_size,
        std::vector<std::pair<std::uint16_t, std::uint16_t>> *diff_ranges)
    {
        if (!max_size) {
            max_size = std::numeric_limits<std::uint16_t>::max();
        }
        if (diff_ranges && diff_ranges->empty()) {
            diff_ranges = nullptr;
        }
        if (diff_ranges && !diff_ranges->empty()) {
            prepareDiffRanges(*diff_ranges);
        }
        
        assert(size <= std::numeric_limits<std::uint16_t>::max());
        if (!max_diff) {
            // by default flush as diff if less than 75% of the data differs
            max_diff = (size * 3) >> 2;
        }
        result.clear();
        const std::uint8_t *it_1 = static_cast<const std::uint8_t *>(buf_1), *it_2 = static_cast<const std::uint8_t *>(buf_2);
        auto it_base = it_1;
        auto end = it_1 + size;
        // exact number of bytes that differ
        std::uint16_t diff_bytes = 0;
        // estimated space occupied by the diff data
        std::uint16_t diff_total = 0;

        std::vector<std::pair<std::uint16_t, std::uint16_t> >::const_iterator it_diff;
        const std::uint8_t *diff_start = nullptr;
        if (diff_ranges) {
            it_diff = diff_ranges->begin();
            diff_start = it_base + it_diff->first;
        }

        for (;;) {
            // total number of allowed diff areas exceeded
            if (result.size() >= *max_size) {
                return false;
            }
            std::uint16_t diff_len = 0;
            while (it_1 != end) {
                if (it_1 == diff_start) {
                    assert(diff_ranges);
                    auto skip = it_diff->second - it_diff->first;
                    it_1 += skip;
                    it_2 += skip;
                    diff_len += skip;
                    ++it_diff;
                    if (it_diff != diff_ranges->end()) {
                        diff_start = it_base + it_diff->first;                        
                    } else {
                        diff_start = nullptr;
                    }
                    continue;
                }
                if (*it_1 == *it_2) {
                    break;
                }
                ++it_1;
                ++it_2;
                ++diff_len;                
            }
            
            // account for the administrative space overhead (approximate)
            diff_bytes += diff_len;
            diff_total += diff_len + sizeof(std::uint16_t);
            if (diff_total > max_diff) {
                return false;
            }
            if (diff_len || it_1 != end) {
                result.push_back(diff_len);
            }
            if (it_1 == end) {
                break;
            }
            std::uint16_t sim_len = 0;
            for (; it_1 != end && *it_1 == *it_2; ++it_1, ++it_2) {
                if (it_1 == diff_start) {
                    break;
                }
                ++sim_len;
            }
            // do not include the trailing similarity area
            if (it_1 == end) {
                break;
            }
            assert(sim_len);
            result.push_back(sim_len);
        }
        // no diffs found
        if (!diff_bytes) {
            result.clear();
        }
        return true;
    }
    
    bool getDiffs(const void *buf, std::size_t size, std::vector<std::uint16_t> &result, std::size_t max_diff,
        std::optional<std::size_t> max_size, std::vector<std::pair<std::uint16_t, std::uint16_t> > *diff_ranges)
    {
        if (!max_size) {
            max_size = std::numeric_limits<std::uint16_t>::max();
        }        
        if (diff_ranges && diff_ranges->empty()) {
            diff_ranges = nullptr;
        }
        if (diff_ranges && !diff_ranges->empty()) {
            prepareDiffRanges(*diff_ranges);
        }

        assert(size <= std::numeric_limits<std::uint16_t>::max());
        if (!max_diff) {
            // by default flush as diff if less than 75% of the data differs
            max_diff = (size * 3) >> 2;
        }
        result.clear();
        const std::uint8_t *it_base = static_cast<const std::uint8_t *>(buf);
        auto it = it_base;
        auto end = it + size;
        // estimated space occupied by the diff data
        std::uint16_t diff_total = 0;
        // include the zero-fill indicator (i.e. the 0, 0 elements)
        result.push_back(0);
        result.push_back(0);
        std::vector<std::pair<std::uint16_t, std::uint16_t> >::const_iterator it_diff;
        const std::uint8_t *diff_start = nullptr;
        if (diff_ranges) {
            it_diff = diff_ranges->begin();
            diff_start = it_base + it_diff->first;
        }

        for (;;) {
            // total number of allowed diff areas exceeded
            if (result.size() >= *max_size) {
                return false;
            }
            std::uint16_t diff_len = 0;
            // identify non-zero bytes or forced-diff ranges
            while (it != end) {
                if (it == diff_start) {
                    assert(diff_ranges);
                    auto skip = it_diff->second - it_diff->first;
                    it += skip;
                    diff_len += skip;
                    ++it_diff;
                    if (it_diff != diff_ranges->end()) {
                        diff_start = it_base + it_diff->first;                        
                    } else {
                        diff_start = nullptr;
                    }
                    continue;
                }
                if (!*it) {
                    break;
                }
                ++diff_len;
                ++it;
            }
            // account for the administrative space overhead (approximate)
            diff_total += diff_len + sizeof(std::uint16_t);
            if (diff_total > max_diff) {
                return false;
            }
            if (diff_len || it != end) {
                result.push_back(diff_len);
            }
            if (it == end) {
                break;
            }
            std::uint16_t sim_len = 0;
            for (; it != end && !*it; ++it) {
                if (it == diff_start) {
                    break;
                }
                ++sim_len;
            }
            // do not include the trailing similarity area
            if (it == end) {
                break;
            }
            assert(sim_len);
            result.push_back(sim_len);
        }
        return true;
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

    void prepareDiffRanges(std::vector<std::pair<std::uint16_t, std::uint16_t>> &diff_ranges)
    {
        // sort ranges ascending
        std::sort(diff_ranges.begin(), diff_ranges.end());
        // then iterate over to merge overlapping ranges
        auto it = diff_ranges.begin(), end = diff_ranges.end();
        if (it == end) {
            return;
        }

        auto next = it;
        ++next;
        for (; next != end; ++next) {
            if (it->second >= next->first) {
                // merge the ranges
                it->second = std::max(it->second, next->second);
            } else {
                ++it;
                if (it != next) {                 
                    *it = *next;
                }                
            }
        }
        // remove the rest of the ranges
        diff_ranges.erase(++it, end);
    }

}