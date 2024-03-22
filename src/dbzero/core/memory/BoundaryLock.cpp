#include "BoundaryLock.hpp"
#include <cstring>

namespace db0 

{

    BoundaryLock::BoundaryLock(StorageView &storage_view, std::uint64_t address, std::shared_ptr<ResourceLock> lhs, std::size_t lhs_size,
        std::shared_ptr<ResourceLock> rhs, std::size_t rhs_size, FlagSet<AccessOptions> access_mode, bool create_new)
        : ResourceLock(storage_view, address, lhs_size + rhs_size, access_mode, create_new)
        , m_lhs(lhs)
        , m_lhs_size(lhs_size)
        , m_rhs(rhs)
        , m_rhs_size(rhs_size)
    {
    }

    BoundaryLock::BoundaryLock(const BoundaryLock &lock, std::uint64_t src_state_num,
        std::shared_ptr<ResourceLock> lhs, std::shared_ptr<ResourceLock> rhs,
        FlagSet<AccessOptions> access_mode)
        : ResourceLock(lock, src_state_num, access_mode)
        , m_lhs(lhs)
        , m_lhs_size(lock.m_lhs_size)
        , m_rhs(rhs)
        , m_rhs_size(lock.m_rhs_size)
    {
    }
    
    void *BoundaryLock::getBuffer(std::uint64_t state_num) const
    {        
        using MutexT = ResourceFetchMutexT;
        while (!MutexT::__ref(m_resource_flags).get())
        {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                // copy from parent locks into the local buffer
                auto lhs_buffer = m_lhs->getBuffer(m_address, state_num);
                auto rhs_buffer = m_rhs->getBuffer(m_address + m_lhs_size, state_num);
                std::memcpy(m_data.data(), lhs_buffer, m_lhs_size);
                std::memcpy(m_data.data() + m_lhs_size, rhs_buffer, m_rhs_size);
                lock.commit_set();
            }
        }
        return m_data.data();
    }
    
    void BoundaryLock::flush()
    {
        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                auto state_num = m_storage_view.get().second;
                // write back to parent locks and mark dirty
                auto lhs_buffer = m_lhs->getBuffer(m_address, state_num);
                auto rhs_buffer = m_rhs->getBuffer(m_address + m_lhs_size, state_num);
                std::memcpy(lhs_buffer, m_data.data(), m_lhs_size);
                std::memcpy(rhs_buffer, m_data.data() + m_lhs_size, m_rhs_size);
                m_lhs->setDirty();
                m_rhs->setDirty();

                // reset the dirty flag
                lock.commit_reset();
            }
        }

        // flush both parent locks
        m_lhs->flush();
        m_rhs->flush();
    }
    
    bool BoundaryLock::resetDirtyFlag()
    {
        // remove dirty flag with the dependent locks as well
        m_lhs->resetDirtyFlag();
        m_rhs->resetDirtyFlag();
        return ResourceLock::resetDirtyFlag();
    }

    void BoundaryLock::release()
    {
        m_lhs->release();
        m_lhs = nullptr;
        m_rhs->release();
        m_rhs = nullptr;
    }

}