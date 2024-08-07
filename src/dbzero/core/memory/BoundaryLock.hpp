#pragma once

#include "BaseLock.hpp"
#include "ResourceLock.hpp"

namespace db0

{

    // BoundaryLockMembers are stored indirectly to be able to convert ResourceLock into 
    // a BoundaryLock in-place. See documentation chapter "Handling conflicting access patterns"
    struct BoundaryLockMembers
    {
        std::shared_ptr<ResourceLock> m_lhs;
        const std::size_t m_lhs_size;
        std::shared_ptr<ResourceLock> m_rhs;
        const std::size_t m_rhs_size;

        BoundaryLockMembers(std::shared_ptr<ResourceLock> lhs, std::size_t lhs_size, 
            std::shared_ptr<ResourceLock> rhs, std::size_t rhs_size);

        // Calculate padding such that the sizeof(ResourceLock) == sizeof(BoundaryLock)
        // padding should be 0 for 64-bit architecutures
        static constexpr std::size_t padding() {
            return sizeof(ResourceLock) - sizeof(std::unique_ptr<BoundaryLockMembers>);
        }
    };

    /**
     * BoundaryLock is supported by the 2 underlying ResourceLock-s
    */
    class BoundaryLock: public BaseLock
    {
    public:
        BoundaryLock(BaseStorage &, std::uint64_t address, std::shared_ptr<ResourceLock> lhs, std::size_t lhs_size,
            std::shared_ptr<ResourceLock> rhs, std::size_t rhs_size, FlagSet<AccessOptions>, bool create_new = false);
        // Create copy of an existing BoundaryLock (for CoW)
        BoundaryLock(BaseStorage &, std::uint64_t address, const BoundaryLock &lock, std::shared_ptr<ResourceLock> lhs, std::size_t lhs_size,
            std::shared_ptr<ResourceLock> rhs, std::size_t rhs_size, FlagSet<AccessOptions>);
        
        virtual ~BoundaryLock();
        
        void flush() override;
        
    private:
        std::unique_ptr<BoundaryLockMembers> m_members;
        std::array<std::byte, BoundaryLockMembers::padding()> m_padding;

        // internal flush, without flushing parents
        void _flush();        
    };

}