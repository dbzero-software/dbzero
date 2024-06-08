#include "BoundaryLock.hpp"
#include <cstring>

namespace db0 

{

    BoundaryLock::BoundaryLock(StorageView &storage_view, std::uint64_t address, std::shared_ptr<ResourceLock> lhs, std::size_t lhs_size,
        std::shared_ptr<ResourceLock> rhs, std::size_t rhs_size, FlagSet<AccessOptions> access_mode, bool create_new)
        // important to use no_cache for BoundaryLock (this is to allow release/creation of a new boundary lock without collisions)
        : ResourceLock(tag_derived(), storage_view, address, lhs_size + rhs_size, access_mode | AccessOptions::no_cache, create_new)
        , m_lhs(lhs)
        , m_lhs_size(lhs_size)
        , m_rhs(rhs)
        , m_rhs_size(rhs_size)
    {
    }
    
    BoundaryLock::BoundaryLock(const BoundaryLock &lock, std::uint64_t src_state_num,
        std::shared_ptr<ResourceLock> lhs, std::shared_ptr<ResourceLock> rhs,
        FlagSet<AccessOptions> access_mode)
        // important to use no_cache for BoundaryLock        
        : ResourceLock(tag_derived(), lock.m_storage_view, lock.m_address, lock.size(), access_mode | AccessOptions::no_cache)        
        , m_lhs(lhs)
        , m_lhs_size(lock.m_lhs_size)
        , m_rhs(rhs)
        , m_rhs_size(lock.m_rhs_size)
    {
    }
    
    BoundaryLock::~BoundaryLock()
    {
        // internal BoundaryLock flush can be performed on destruction since it's a non-IO operation        
        this->_flush();
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
                std::memcpy(m_data.data(), lhs_buffer, m_lhs_size);
                auto rhs_buffer = m_rhs->getBuffer(m_address + m_lhs_size, state_num);
                std::memcpy(m_data.data() + m_lhs_size, rhs_buffer, m_rhs_size);

                lock.commit_set();
            }
        }
        return m_data.data();
    }

    void BoundaryLock::_flush()
    {
        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                // dirty resource should also be fetched
                assert(this->isFetched());
                auto state_num = m_storage_view.get().second;
                // write back to parent locks and mark dirty
                auto lhs_buffer = m_lhs->getBuffer(m_address, state_num);
                std::memcpy(lhs_buffer, m_data.data(), m_lhs_size);
                auto rhs_buffer = m_rhs->getBuffer(m_address + m_lhs_size, state_num);
                std::memcpy(rhs_buffer, m_data.data() + m_lhs_size, m_rhs_size);
                m_lhs->setDirty();
                m_rhs->setDirty();
                
                // reset the dirty flag
                lock.commit_reset();
            }
        }
    }

    void BoundaryLock::flush()
    {
        _flush();
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

    std::size_t BoundaryLock::size() const {
        return m_lhs_size + m_rhs_size;
    }

}