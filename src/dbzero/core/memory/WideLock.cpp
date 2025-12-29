// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "WideLock.hpp"
#include <iostream>
#include <cstring>
#include <cassert>
#include <dbzero/core/storage/BaseStorage.hpp>

namespace db0

{
    
    WideLock::WideLock(StorageContext context, std::uint64_t address, std::size_t size, FlagSet<AccessOptions> access_mode,
        StateNumType read_state_num, StateNumType write_state_num, std::shared_ptr<DP_Lock> res_lock, std::shared_ptr<ResourceLock> cow_lock)
        : DP_Lock(tag_derived{}, context, address, size, access_mode, read_state_num, write_state_num, cow_lock)
        , m_res_lock(res_lock)
    {
        // initialzie the local buffer
        if (access_mode[AccessOptions::read]) {
            assert(read_state_num > 0);            
            // NOTE: if res_lock (residual) is present then we can skip reading the last page from storage
            auto &storage = m_context.m_storage_ref.get();
            if (m_res_lock) {
                auto dp_size = static_cast<std::size_t>(size / storage.getPageSize()) * storage.getPageSize();
                assert(dp_size > 0);
                assert(dp_size < size);
                storage.read(m_address, read_state_num, dp_size, m_data.data(), access_mode);
                // and copy the residual contents from the res_lock
                std::memcpy(m_data.data() + dp_size, res_lock->getBuffer(), size  - dp_size);                
            } else {
                storage.read(m_address, read_state_num, this->size(), m_data.data(), access_mode);
            }
            // prepare the CoW data buffer (for a mutable lock)            
            if (!access_mode[AccessOptions::no_cow] && !m_cow_lock && !access_mode[AccessOptions::create]) {
                m_cow_data.resize(m_data.size());
                std::memcpy(m_cow_data.data(), m_data.data(), m_data.size());
            }
        }
    }
    
    WideLock::WideLock(std::shared_ptr<WideLock> lock, StateNumType write_state_num, FlagSet<AccessOptions> access_mode,
        std::shared_ptr<DP_Lock> res_lock)
        : DP_Lock(lock, write_state_num, access_mode)
        , m_res_lock(res_lock)
    {
    }
    
    bool WideLock::tryFlush(FlushMethod flush_method) {
        return _tryFlush(flush_method);
    }

    void WideLock::flush() {
        _tryFlush(FlushMethod::full);
    }
    
    void WideLock::resLockFlush()
    {
        auto &storage = m_context.m_storage_ref.get();
        // data page size without the residual part
        auto dp_size = static_cast<std::size_t>(this->size() / storage.getPageSize()) * storage.getPageSize();
        assert(dp_size > 0);
        assert(dp_size < this->size());
        
        m_res_lock->setDirty();
        // copy the residual part only
        std::memcpy(m_res_lock->getBuffer(), m_data.data() + dp_size, this->size() - dp_size);

        if (!m_diffs.empty()) {
            auto res_begin = m_res_lock->getAddress();
            if (m_diffs.isOverflow()) {
                // force-diff the entire residual part
                m_res_lock->setDirty(res_begin, res_begin + m_res_lock->size());
            } else {
                // create view of the residual part only
                DiffRangeView diff_view(m_diffs, m_data.size(), this->size());
                // and apply all ranges
                for (std::size_t i = 0; i < diff_view.size(); ++i) {
                    auto range = diff_view[i];
                    // convert to absolute addresses
                    m_res_lock->setDirty(res_begin + range.first, res_begin + range.second);
                }
            }
        }        
    }

    bool WideLock::_tryFlush(FlushMethod flush_method)
    {
        // no-flush flag is important for volatile locks (atomic operations)
        if (m_access_mode[AccessOptions::no_flush]) {
            // no need to flush, just reset the dirty flag
            return true;
        }
        
        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                auto &storage = m_context.m_storage_ref.get();
                auto dp_size = this->size();
                auto page_size = storage.getPageSize();
                if (m_res_lock) {
                    dp_size = static_cast<std::size_t>(dp_size / page_size) * page_size;
                    assert(dp_size > 0);
                    assert(dp_size < this->size());
                }
                
                if (flush_method == FlushMethod::full) {
                    // write the first part of the data from the local buffer
                    storage.write(m_address, m_state_num, dp_size, m_data.data());
                } else {
                    assert(flush_method == FlushMethod::diff);
                    auto cow_ptr = getCowPtr();
                    if (!cow_ptr || m_diffs.isOverflow()) {
                        // unable to diff-flush (CoW data not available)
                        // or the entire range must be forced to be diffed (overflow)
                        return false;
                    }

                    // write page-by-page using diff method (for DPs where diff method is applicable)
                    std::vector<std::uint16_t> diffs;
                    std::size_t unwritten_size = 0;
                    auto page_ptr = m_data.data(), end_ptr = m_data.data() + dp_size;
                    bool first_write = true;
                    for (;page_ptr != end_ptr; page_ptr += page_size) {
                        // NOTE that cow_ptr gets adjusted to the next page after this call
                        if (this->getDiffs(cow_ptr, page_ptr, page_size, diffs)) {
                            // flush unwritten data as a full-DP
                            if (unwritten_size > 0) {
                                storage.write(m_address, m_state_num, unwritten_size, m_data.data());
                                unwritten_size = 0;
                            }
                            // NOTE: DP needs not to be flushed if there are no diffs
                            if (!diffs.empty()) {
                                if (!storage.tryWriteDiffs(
                                    m_address + (page_ptr - m_data.data()), m_state_num, page_size, page_ptr, diffs)) 
                                {
                                    // write as full-DP if unable to write diffs
                                    storage.write(m_address + (page_ptr - m_data.data()), m_state_num, page_size, page_ptr);                                    
                                }
                            }
                            first_write = false;
                        } else {
                            if (first_write) {
                                unwritten_size += page_size;
                            } else {
                                // some data has already been written as diff-DP
                                assert(unwritten_size == 0);
                                // write as a full-DP
                                storage.write(m_address + (page_ptr - m_data.data()), m_state_num, page_size, page_ptr);
                                first_write = false;
                            }
                        }
                    }
                    if (unwritten_size > 0) {
                        assert(first_write);
                        assert(unwritten_size == dp_size);
                        // unable to write using the diff method
                        return false;                        
                    }
                }
                
                // and the residual part into the res_lock (which may be flushed independently)
                if (m_res_lock) {
                    resLockFlush();
                }
                
                m_diffs.clear();
                // reset the dirty flag
                lock.commit_reset();
            }
        }
        return true;
    }
    
    void WideLock::flushResidual()
    {
        if (!m_res_lock) {
            return;
        }
        
        // no-flush flag is important for volatile locks (atomic operations)
        if (m_access_mode[AccessOptions::no_flush]) {
            return;
        }
        
        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                resLockFlush();
                // note the dirty flag is not reset here
                break;
            }
        }
    }
    
    void WideLock::rebase(const std::unordered_map<const ResourceLock*, std::shared_ptr<DP_Lock> > &rebase_map)
    {
        auto it = rebase_map.find(m_res_lock.get());
        if (it != rebase_map.end()) {
            m_res_lock = it->second;
        }
    }
    
    void WideLock::updateStateNum(StateNumType state_num, bool is_volatile, std::shared_ptr<DP_Lock> res_lock)
    {
        DP_Lock::updateStateNum(state_num, is_volatile);
        // also need to update the residual lock
        m_res_lock = res_lock;        
    }

#ifndef NDEBUG
    bool WideLock::isBoundaryLock() const {
        return false;
    }
#endif
    
    bool WideLock::getDiffs(const std::byte *&cow_ptr, void *buf, std::size_t size,
        std::vector<std::uint16_t> &result) const
    {
        // unable to get diffs when overflow
        assert(!m_diffs.isOverflow());
        // NOTE: get forced diffs (views) from the specified range only
        auto offset = static_cast<std::byte*>(buf) - m_data.data();
        if (cow_ptr == &m_cow_zero) {
            return db0::getDiffs(buf, size, result, 0, {}, DiffRangeView(m_diffs, offset, offset + size));
        } else {            
            auto has_diffs = db0::getDiffs(cow_ptr, buf, size, result, 0, {}, DiffRangeView(m_diffs, offset, offset + size));
            cow_ptr += size;
            return has_diffs;
        }
    }
    
}