// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <dbzero/core/memory/DP_Lock.hpp>
#include <iostream>
#include <cstring>
#include <cassert>
#include <dbzero/core/storage/BaseStorage.hpp>
#ifndef NDEBUG
#include <dbzero/core/storage/BDevStorage.hpp>
#endif

namespace db0

{
    
    DP_Lock::DP_Lock(StorageContext context, std::uint64_t address, std::size_t size,
        FlagSet<AccessOptions> access_mode, StateNumType read_state_num, StateNumType write_state_num,
        std::shared_ptr<ResourceLock> cow_lock)
        : ResourceLock(context, address, size, access_mode, cow_lock)
        , m_state_num(std::max(read_state_num, write_state_num))
    {
        assert(addrPageAligned(m_context.m_storage_ref.get()));
        // initialzie the local buffer
        if (access_mode[AccessOptions::read]) {
            assert(read_state_num > 0);            
            // read into the local buffer
            m_context.m_storage_ref.get().read(
                m_address, read_state_num, this->size(), m_data.data(), access_mode
            );
            // prepare the CoW data buffer (for a mutable lock)            
            if (!access_mode[AccessOptions::no_cow] && !access_mode[AccessOptions::create] && !m_cow_lock) {
                m_cow_data.resize(m_data.size());
                std::memcpy(m_cow_data.data(), m_data.data(), m_data.size());
            }
        }
    }
    
    DP_Lock::DP_Lock(tag_derived, StorageContext context, std::uint64_t address, std::size_t size,
        FlagSet<AccessOptions> access_mode, StateNumType read_state_num, StateNumType write_state_num,
        std::shared_ptr<ResourceLock> cow_lock)
        : ResourceLock(context, address, size, access_mode, cow_lock)
        , m_state_num(std::max(read_state_num, write_state_num))
    {
    }
    
    DP_Lock::DP_Lock(std::shared_ptr<DP_Lock> other, StateNumType write_state_num, FlagSet<AccessOptions> access_mode)
        : ResourceLock(other, access_mode)
        , m_state_num(write_state_num)
    {
        assert(addrPageAligned(m_context.m_storage_ref.get()));
        assert(m_state_num > 0);
    }
    
    bool DP_Lock::_tryFlush(FlushMethod flush_method)
    {
        // no-flush flag is important for volatile locks (atomic operations)
        if (m_access_mode[AccessOptions::no_flush]) {
            return true;
        }
        
        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                // write from the local buffer (either as a full-DP or diff-DP)
                auto &storage = m_context.m_storage_ref.get();
                if (flush_method == FlushMethod::full) {
                    storage.write(m_address, m_state_num, this->size(), m_data.data());
                } else {
                    assert(flush_method == FlushMethod::diff);
                    auto cow_ptr = getCowPtr();                    
                    if (!cow_ptr) {
                        // unable to diff-flush                        
                        return false;
                    }

                    std::vector<std::uint16_t> diffs;
                    if (!this->getDiffs(cow_ptr, diffs)) {
                        // unable to diff-flush (too many diffs)
                        return false;
                    }
                    
                    // NOTE: DP needs not to be flushed if there are no diffs
                    if (!diffs.empty()) {
                        if (!storage.tryWriteDiffs(m_address, m_state_num, this->size(), m_data.data(), diffs)) {
                            // unable to diff-flush
                            return false;
                        }
                    }
                    
#ifndef NDEBUG                    
                    if (Settings::__storage_validation) {
                        // write full contents for validation
                        storage.asFile().writeForValidation(m_address, m_state_num, this->size(), m_data.data());
                    }
#endif                    
                }
                
                m_diffs.clear();
                // reset the dirty flag
                lock.commit_reset();
            }
        }
        return true;
    }
    
    bool DP_Lock::tryFlush(FlushMethod flush_method) {
        return _tryFlush(flush_method);
    }
    
    void DP_Lock::flush() {
        _tryFlush(FlushMethod::full);
    }
    
    std::uint64_t DP_Lock::getStateNum() const {
        return m_state_num;
    }
    
    void DP_Lock::updateStateNum(StateNumType state_num, bool is_volatile)
    {
        assert(state_num > m_state_num);
        assert(!isDirty());
        if (is_volatile) {
            // NOTE: in case of volatile locks, CoW data will be reused
            m_access_mode.set(AccessOptions::no_flush);
        } else {
            // collect the CoW's data buffer for the next transaction
            m_cow_lock = nullptr;
            if (m_access_mode[AccessOptions::create]) {
                m_access_mode.set(AccessOptions::create, false);
            }
            m_cow_data.resize(m_data.size());
            std::memcpy(m_cow_data.data(), m_data.data(), m_data.size());
        }
        m_state_num = state_num;
        setDirty();
    }
    
    void DP_Lock::merge(StateNumType final_state_num)
    {
        // for atomic operations current state num is active transaction +1
        assert(m_state_num == final_state_num + 1);
        m_state_num = final_state_num;        
    }
    
#ifndef NDEBUG
    bool DP_Lock::isBoundaryLock() const {
        return false;
    }
#endif
    
}