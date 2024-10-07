#pragma once

#include "ResourceLock.hpp"
#include "DP_Lock.hpp"

namespace db0

{

    /**
     * BoundaryLock is supported by the 2 underlying DP_Lock-s
    */
    class BoundaryLock: public ResourceLock
    {
    public:
        BoundaryLock(StorageContext, std::uint64_t address, std::shared_ptr<DP_Lock> lhs, std::size_t lhs_size,
            std::shared_ptr<DP_Lock> rhs, std::size_t rhs_size, FlagSet<AccessOptions>, bool create_new = false);
        // Create copy of an existing BoundaryLock (for CoW)
        BoundaryLock(StorageContext, std::uint64_t address, const BoundaryLock &lock, std::shared_ptr<DP_Lock> lhs, std::size_t lhs_size,
            std::shared_ptr<DP_Lock> rhs, std::size_t rhs_size, FlagSet<AccessOptions>);
        
        virtual ~BoundaryLock();
        
        void flush() override;

    private:
        std::shared_ptr<DP_Lock> m_lhs;
        const std::size_t m_lhs_size;
        std::shared_ptr<DP_Lock> m_rhs;
        const std::size_t m_rhs_size;
        
        // internal flush, without flushing parents
        void _flush();
    };
            
}