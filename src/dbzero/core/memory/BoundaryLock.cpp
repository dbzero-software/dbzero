#include "BoundaryLock.hpp"
#include <cstring>
#include "utils.hpp"

namespace db0

{
    
    BoundaryLock::BoundaryLock(StorageContext context, std::uint64_t address, std::shared_ptr<DP_Lock> lhs, std::size_t lhs_size,
        std::shared_ptr<DP_Lock> rhs, std::size_t rhs_size, FlagSet<AccessOptions> access_mode)
        // important to use no_cache for BoundaryLock (this is to allow release/creation of a new boundary lock without collisions)
        : ResourceLock(context, address, lhs_size + rhs_size, access_mode | AccessOptions::no_cache)
        , m_lhs(lhs)
        , m_lhs_size(lhs_size)
        , m_rhs(rhs)
        , m_rhs_size(rhs_size)
    {
        if (access_mode[AccessOptions::read]) {
            // copy from parent locks into the local buffer
            auto lhs_buffer = lhs->getBuffer(m_address);
            std::memcpy(m_data.data(), lhs_buffer, lhs_size);
            auto rhs_buffer = rhs->getBuffer(m_address + lhs_size);
            std::memcpy(m_data.data() + lhs_size, rhs_buffer, rhs_size);
        }
    }
    
    BoundaryLock::BoundaryLock(StorageContext context, std::uint64_t address, const BoundaryLock &lock,
        std::shared_ptr<DP_Lock> lhs, std::size_t lhs_size,
        std::shared_ptr<DP_Lock> rhs, std::size_t rhs_size, 
        FlagSet<AccessOptions> access_mode)
        // important to use no_cache for BoundaryLock (this is to allow release/creation of a new boundary lock without collisions)
        : ResourceLock(context, address, lhs_size + rhs_size, access_mode | AccessOptions::no_cache)
        , m_lhs(lhs)
        , m_lhs_size(lhs_size)
        , m_rhs(rhs)
        , m_rhs_size(rhs_size)
    {
        // copy existing data (only data, mu-store not copied)
        std::memcpy(m_data.data(), lock.m_data.data(), lock.size());
    }
    
    BoundaryLock::~BoundaryLock()
    {        
        // internal BoundaryLock flush can be performed on destruction since it's a non-IO operation
        this->flushBoundary();
    }
    
    void BoundaryLock::flushBoundary()
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
                
                m_diffs.clear();
                m_diffs_overflow = false;
                // reset the dirty flag
                lock.commit_reset();
            }
        }
    }

    bool BoundaryLock::_tryFlush(FlushMethod flush_method)
    {
        if (flush_method == FlushMethod::diff) {
            // diff-flush not supported for BoundaryLock
            return false;
        }
        flushBoundary();
        // try flushing both parent locks
        bool result = m_lhs->tryFlush(flush_method);
        result &= m_rhs->tryFlush(flush_method);
        return result;
    }
    
    bool BoundaryLock::tryFlush(FlushMethod flush_method) {
        return _tryFlush(flush_method);
    }
    
    void BoundaryLock::flush() {
        _tryFlush(FlushMethod::full);
    }
    
    void __rebase(std::shared_ptr<DP_Lock> &lock,
        const std::unordered_map<const ResourceLock*, std::shared_ptr<DP_Lock> > &rebase_map) 
    {
        auto it = rebase_map.find(lock.get());
        if (it != rebase_map.end()) {
            lock = it->second;
        }
    }
    
    void BoundaryLock::rebase(const std::unordered_map<const ResourceLock*, std::shared_ptr<DP_Lock> > &rebase_map)
    {    
        __rebase(m_lhs, rebase_map);
        __rebase(m_rhs, rebase_map);
    }
    
#ifndef NDEBUG
    bool BoundaryLock::isBoundaryLock() const {
        return true;
    }
#endif

}