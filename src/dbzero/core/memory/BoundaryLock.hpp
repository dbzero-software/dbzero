#pragma once

#include "ResourceLock.hpp"

namespace db0

{
    
    /**
     * BoundaryLock is supported by the 2 underlying ResourceLock-s
    */
    class BoundaryLock: public ResourceLock
    {
    public:
        BoundaryLock(StorageView &, std::uint64_t address, std::shared_ptr<ResourceLock> lhs, std::size_t lhs_size,
            std::shared_ptr<ResourceLock> rhs, std::size_t rhs_size, FlagSet<AccessOptions>, bool create_new = false);

        BoundaryLock(const BoundaryLock &, std::uint64_t src_state_num, std::shared_ptr<ResourceLock> lhs, 
            std::shared_ptr<ResourceLock> rhs, FlagSet<AccessOptions>);
        
        virtual ~BoundaryLock();

        void *getBuffer(std::uint64_t state_num) const override;
        
        void flush() override;
        
        bool resetDirtyFlag() override;

        std::pair<std::shared_ptr<ResourceLock>, std::shared_ptr<ResourceLock> > getParentLocks() const;

        void release() override;
        
    private:
        std::shared_ptr<ResourceLock> m_lhs;
        const std::size_t m_lhs_size;
        std::shared_ptr<ResourceLock> m_rhs;
        const std::size_t m_rhs_size;

        // internal flush, without flushing parents
        void _flush();
    };

}