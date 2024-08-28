#include "BoundaryLock.hpp"
#include <cstring>
#include "utils.hpp"

namespace db0

{
    
    BoundaryLock::BoundaryLock(BaseStorage &storage, std::uint64_t address, std::shared_ptr<DP_Lock> lhs, std::size_t lhs_size,
        std::shared_ptr<DP_Lock> rhs, std::size_t rhs_size, FlagSet<AccessOptions> access_mode, bool create_new)
        // important to use no_cache for BoundaryLock (this is to allow release/creation of a new boundary lock without collisions)
        : ResourceLock(storage, address, lhs_size + rhs_size, access_mode | AccessOptions::no_cache, create_new)
        , m_lhs(lhs)
        , m_lhs_size(lhs_size)
        , m_rhs(rhs)
        , m_rhs_size(rhs_size)
    {
        if (!create_new) {
            // copy from parent locks into the local buffer
            auto lhs_buffer = lhs->getBuffer(m_address);
            std::memcpy(m_data.data(), lhs_buffer, lhs_size);
            auto rhs_buffer = rhs->getBuffer(m_address + lhs_size);
            std::memcpy(m_data.data() + lhs_size, rhs_buffer, rhs_size);
        }
    }
    
    BoundaryLock::BoundaryLock(BaseStorage &storage, std::uint64_t address, const BoundaryLock &lock,
        std::shared_ptr<DP_Lock> lhs, std::size_t lhs_size,
        std::shared_ptr<DP_Lock> rhs, std::size_t rhs_size, 
        FlagSet<AccessOptions> access_mode)
        // important to use no_cache for BoundaryLock (this is to allow release/creation of a new boundary lock without collisions)
        : ResourceLock(storage, address, lhs_size + rhs_size, access_mode | AccessOptions::no_cache, false)
        , m_lhs(lhs)
        , m_lhs_size(lhs_size)
        , m_rhs(rhs)
        , m_rhs_size(rhs_size)
    {
        // copy existing data
        std::memcpy(m_data.data(), lock.m_data.data(), lock.m_data.size());
    }
    
    BoundaryLock::~BoundaryLock()
    {
        // internal BoundaryLock flush can be performed on destruction since it's a non-IO operation
        this->_flush();
    }
    
    void BoundaryLock::_flush()
    {
        // note that boundary locks are flushed even with no_flush flag
        using MutexT = ResourceDirtyMutexT;
        while (MutexT::__ref(m_resource_flags).get()) {
            MutexT::WriteOnlyLock lock(m_resource_flags);
            if (lock.isLocked()) {
                // write back to parent locks and mark dirty
                m_lhs->setDirty();
                auto lhs_buffer = m_lhs->getBuffer(m_address);
                std::memcpy(lhs_buffer, m_data.data(), m_lhs_size);
                m_rhs->setDirty();
                auto rhs_buffer = m_rhs->getBuffer(m_address + m_lhs_size);
                std::memcpy(rhs_buffer, m_data.data() + m_lhs_size, m_rhs_size);
                                
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

}