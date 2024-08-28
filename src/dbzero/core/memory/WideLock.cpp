#include "WideLock.hpp"
#include <iostream>
#include <cstring>
#include <cassert>
#include <dbzero/core/storage/BaseStorage.hpp>

namespace db0

{
    
    WideLock::WideLock(BaseStorage &storage, std::uint64_t address, std::size_t size, FlagSet<AccessOptions> access_mode,
        std::uint64_t read_state_num, std::uint64_t write_state_num, std::shared_ptr<DP_Lock> res_lock, 
        bool create_new)
        : DP_Lock(tag_derived{}, storage, address, size, access_mode, read_state_num, write_state_num, create_new)
        , m_res_lock(res_lock)
    {
        // initialzie the local buffer
        if (access_mode[AccessOptions::read]) {
            assert(read_state_num > 0);
            // NOTE: if res_lock (residual) is present then we can skip reading the last page from storage
            if (m_res_lock) {
                auto dp_size = static_cast<std::size_t>(size / storage.getPageSize()) * storage.getPageSize();
                assert(dp_size > 0);
                assert(dp_size < size);
                storage.read(m_address, read_state_num, dp_size, m_data.data(), access_mode);
                // and copy the residual contents from the res_lock
                std::memcpy(m_data.data() + dp_size, res_lock->getBuffer(), size  - dp_size);
            } else {
                storage.read(m_address, read_state_num, m_data.size(), m_data.data(), access_mode);
            }
        }
    }

    WideLock::WideLock(const WideLock &lock, std::uint64_t write_state_num, FlagSet<AccessOptions> access_mode,
        std::shared_ptr<DP_Lock> res_lock)
        : DP_Lock(lock, write_state_num, access_mode)
        , m_res_lock(res_lock)
    {    
    }
    
    void WideLock::flush()
    {
        // no-flush flag is important for volatile locks (atomic operations)
        if (m_access_mode[AccessOptions::no_flush]) {
            return;
        }

        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                if (m_res_lock) {
                    auto dp_size = static_cast<std::size_t>(m_data.size() / m_storage.getPageSize()) * m_storage.getPageSize();
                    assert(dp_size > 0);
                    assert(dp_size < m_data.size());
                    // write the first part of the data from the local buffer
                    m_storage.write(m_address, m_state_num, dp_size, m_data.data());
                    // and the residual part into the res_lock (which may be flushed independently)
                    m_res_lock->setDirty();
                    std::memcpy(m_res_lock->getBuffer(), m_data.data() + dp_size, m_data.size() - dp_size);
                } else {
                    // write entire contents from the local buffer
                    m_storage.write(m_address, m_state_num, m_data.size(), m_data.data());
                }
                // reset the dirty flag
                lock.commit_reset();
            }
        }
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
                auto dp_size = static_cast<std::size_t>(m_data.size() / m_storage.getPageSize()) * m_storage.getPageSize();
                assert(dp_size > 0);
                assert(dp_size < m_data.size());                    
                // and the residual part into the res_lock (which may be flushed independently)
                m_res_lock->setDirty();
                std::memcpy(m_res_lock->getBuffer(), m_data.data() + dp_size, m_data.size() - dp_size);
                // note the dirty flag is not reset here
            }
        }
    }
    
}