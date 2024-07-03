#pragma once

#include "BaseLock.hpp"
#include "ResourceLock.hpp"

namespace db0

{

    /**
     * BoundaryLock is supported by the 2 underlying ResourceLock-s
    */
    class BoundaryLock: public BaseLock
    {
    public:
        BoundaryLock(BaseStorage &, std::uint64_t address, std::shared_ptr<ResourceLock> lhs, std::size_t lhs_size,
            std::shared_ptr<ResourceLock> rhs, std::size_t rhs_size, FlagSet<AccessOptions>, bool create_new = false);        
        virtual ~BoundaryLock();
        
        void flush() override;
                
    private:
        std::shared_ptr<ResourceLock> m_lhs;
        const std::size_t m_lhs_size;
        std::shared_ptr<ResourceLock> m_rhs;
        const std::size_t m_rhs_size;

        // internal flush, without flushing parents
        void _flush();
    };

}