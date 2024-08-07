#include <gtest/gtest.h>
#include <dbzero/core/memory/BoundaryLock.hpp>
#include <dbzero/core/memory/ResourceLock.hpp>
#include <dbzero/core/storage/Storage0.hpp>

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
        auto boundary_lock = convertToBoundaryLock(lock, lhs);
        assert(boundary_lock->getAddress() == page_size);
    }
    
}