#pragma once

#include "BaseLock.hpp"
#include "ResourceLock.hpp"

namespace db0

{

    // BoundaryLockMembers are stored indirectly to be able to convert ResourceLock into 
    // a BoundaryLock in-place. See documentation chapter "Handling conflicting access patterns"
    struct BoundaryLockMembers
    {
        std::shared_ptr<BaseLock> m_lhs;
        const std::size_t m_lhs_size;
        std::shared_ptr<BaseLock> m_rhs;
        const std::size_t m_rhs_size;

        BoundaryLockMembers(std::shared_ptr<BaseLock> lhs, std::size_t lhs_size, std::shared_ptr<BaseLock> rhs,
            std::size_t rhs_size);

        // Calculate padding such that the sizeof(ResourceLock) == sizeof(BoundaryLock)
        // padding should be 0 for 64-bit architecutures
        static constexpr std::size_t padding() {
            return sizeof(ResourceLock) - (sizeof(BaseLock) + sizeof(std::unique_ptr<BoundaryLockMembers>));
        }
    };

    /**
     * BoundaryLock is supported by the 2 underlying ResourceLock-s
    */
    class BoundaryLock: public BaseLock
    {
    public:
        BoundaryLock(BaseStorage &, std::uint64_t address, std::shared_ptr<BaseLock> lhs, std::size_t lhs_size,
            std::shared_ptr<BaseLock> rhs, std::size_t rhs_size, FlagSet<AccessOptions>, bool create_new = false);
        // Create copy of an existing BoundaryLock (for CoW)
        BoundaryLock(BaseStorage &, std::uint64_t address, const BoundaryLock &lock, std::shared_ptr<BaseLock> lhs, std::size_t lhs_size,
            std::shared_ptr<BaseLock> rhs, std::size_t rhs_size, FlagSet<AccessOptions>);

        virtual ~BoundaryLock();
        
        void flush() override;

        // Converts the underlying ResourceLock instance in-place into a BoundaryLock
        // as a result the new (foundational) ResourceLock is created and returned
        // for a rationale of this operation see "Handling conflicting access patterns" in the documentation
        static std::shared_ptr<BoundaryLock> convertToBoundaryLock(std::shared_ptr<ResourceLock>,
            std::shared_ptr<ResourceLock> lhs);

    private:
        std::unique_ptr<BoundaryLockMembers> m_members;
        std::byte m_padding[BoundaryLockMembers::padding()];

        // Create a boundary lock converted from a ResourceLock, use just one parent lock (lhs)
        BoundaryLock(ResourceLock &&, std::vector<std::byte> &&data, std::shared_ptr<ResourceLock> lhs);
        
        // internal flush, without flushing parents
        void _flush();        
    };

    static_assert(sizeof(BoundaryLock) == sizeof(ResourceLock), "BoundaryLock size mismatch");
        
}