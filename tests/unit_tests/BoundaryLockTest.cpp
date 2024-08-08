#include <gtest/gtest.h>
#include <dbzero/core/memory/BoundaryLock.hpp>
#include <dbzero/core/memory/ResourceLock.hpp>
#include <dbzero/core/storage/Storage0.hpp>
#include <cstring>

using namespace std;
using namespace db0;

namespace tests

{

    TEST( BoundaryLockTest, testResourceLockInPlaceConversionToBoundaryLock )
    {
        Storage0 null_storage;
        auto page_size = null_storage.getPageSize();
        auto lock = std::make_shared<ResourceLock>(
            null_storage, page_size, page_size, FlagSet<AccessOptions> { AccessOptions::read }, 1, 1, false
        );
        auto lhs = std::make_shared<ResourceLock>(
            null_storage, 0, page_size * 2, FlagSet<AccessOptions> { AccessOptions::read }, 1, 1, false
        );
        auto boundary_lock = BoundaryLock::convertToBoundaryLock(lock, lhs);
        ASSERT_EQ(boundary_lock->getAddress(), page_size);
        ASSERT_EQ(boundary_lock->size(), page_size);
    }

    TEST( BoundaryLockTest, testUpdateConvertedBoundaryLock )
    {
        Storage0 null_storage;
        auto page_size = null_storage.getPageSize();
        auto lock = std::make_shared<ResourceLock>(
            null_storage, page_size, page_size, FlagSet<AccessOptions> { AccessOptions::read }, 1, 1, true
        );
        auto lhs = std::make_shared<ResourceLock>(
            null_storage, 0, page_size * 2, FlagSet<AccessOptions> { AccessOptions::read }, 1, 1, true
        );
        auto boundary_lock = BoundaryLock::convertToBoundaryLock(lock, lhs);
        std::memcpy((char*)(boundary_lock->getBuffer(page_size + 16)), "0123KKKK", 8);
        boundary_lock->setDirty();
        boundary_lock->flush();

        // make sure changes were written to lhs
        ASSERT_EQ(std::string((char*)lhs->getBuffer(page_size + 16), 8), "0123KKKK");
    }

    TEST( BoundaryLockTest, testUpdateConvertedBoundaryLockViaExistingBaseLock )
    {
        Storage0 null_storage;
        auto page_size = null_storage.getPageSize();
        auto lock = std::make_shared<ResourceLock>(
            null_storage, page_size, page_size, FlagSet<AccessOptions> { AccessOptions::read }, 1, 1, true
        );
        auto lhs = std::make_shared<ResourceLock>(
            null_storage, 0, page_size * 2, FlagSet<AccessOptions> { AccessOptions::read }, 1, 1, true
        );

        std::shared_ptr<BaseLock> base_lock = lock;
        auto buffer = (char*)base_lock->getBuffer(page_size);
        // convert to BoundaryLock but keep the base lock        
        BoundaryLock::convertToBoundaryLock(lock, lhs);

        // update via already fetched buffer's pointer
        std::memcpy((char*)(&buffer[16]), "0123KKKK", 8);
        // perform operations on BaseLock (which now is BoundaryLock)
        base_lock->setDirty();
        base_lock->flush();
        
        // make sure changes were written to lhs
        ASSERT_EQ(std::string((char*)lhs->getBuffer(page_size + 16), 8), "0123KKKK");
    }

}