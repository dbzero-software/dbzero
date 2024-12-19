#include <dbzero/core/memory/DP_Lock.hpp>
#include <iostream>
#include <cstring>
#include <cassert>
#include <dbzero/core/storage/BaseStorage.hpp>

namespace db0

{
    
    DP_Lock::DP_Lock(StorageContext context, std::uint64_t address, std::size_t size,
        FlagSet<AccessOptions> access_mode, std::uint64_t read_state_num, std::uint64_t write_state_num, std::uint16_t mu_size)
        : ResourceLock(context, address, size, access_mode, mu_size)
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
        }
    }

    DP_Lock::DP_Lock(tag_derived, StorageContext context, std::uint64_t address, std::size_t size,
        FlagSet<AccessOptions> access_mode, std::uint64_t read_state_num, std::uint64_t write_state_num, std::uint16_t mu_size)
        : ResourceLock(context, address, size, access_mode, mu_size)
        , m_state_num(std::max(read_state_num, write_state_num))
    {
    }
    
    DP_Lock::DP_Lock(const DP_Lock &other, std::uint64_t write_state_num, FlagSet<AccessOptions> access_mode)
        : ResourceLock(other, access_mode)
        , m_state_num(write_state_num)
    {
        assert(addrPageAligned(m_context.m_storage_ref.get()));
        assert(m_state_num > 0);
    }

    void DP_Lock::flush()
    {
        // no-flush flag is important for volatile locks (atomic operations)
        if (m_access_mode[AccessOptions::no_flush]) {
            return;
        }

        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                // write from the local buffer
                m_context.m_storage_ref.get().write(m_address, m_state_num, this->size(), m_data.data());
                if (m_mu_size > 0) {
                    getMUStore().clear();
                }
                // reset the dirty flag
                lock.commit_reset();
            }
        }
    }

    std::uint64_t DP_Lock::getStateNum() const {
        return m_state_num;
    }

    void DP_Lock::updateStateNum(std::uint64_t state_num, bool no_flush)
    {
        assert(state_num > m_state_num);
        assert(!isDirty());        
        m_state_num = state_num;        
        if (no_flush) {
            m_access_mode.set(AccessOptions::no_flush);
        }
        setDirty();
    }
    
    void DP_Lock::merge(std::uint64_t final_state_num)
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