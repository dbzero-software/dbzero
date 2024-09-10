#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/memory/SlabAllocator.hpp>

using namespace std;

namespace tests

{

    class SlabAllocatorTests: public testing::Test 
    {
    public:

        SlabAllocatorTests() 
            : m_memspace(m_workspace.getMemspace("test_slab_allocator")) 
        {
        }

        virtual void SetUp() override {            
        }

        virtual void TearDown() override {
        }

    protected:
        db0::TestWorkspace m_workspace;
        db0::Memspace m_memspace;
        static constexpr std::size_t page_size = 4096;
        static constexpr std::size_t slab_size = 64 * 1024 * 1024;
    };
    
    TEST_F( SlabAllocatorTests , testNewlyFormattedSlabAllocatorCanBeOpened )
    {   
        // initialize a new slab under the address = 0 
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), 0, slab_size, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), 0, slab_size, page_size);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocationsAreTakenFromSlabFront )
    {   
        // initialize a new slab under the address = 0 
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), 0, slab_size, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), 0, slab_size, page_size);

        auto addr_1 = cut.alloc(100);
        auto addr_2 = cut.alloc(71);
        ASSERT_TRUE (addr_2 > addr_1);
    }
    
    TEST_F( SlabAllocatorTests , testSlabAllocatorGetAllocSize )
    {   
        // initialize a new slab under the address = 0 
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), 0, slab_size, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), 0, slab_size, page_size);

        std::vector<std::size_t> sizes = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 31, 63, 64, 65, 66, 67, 68, 69, 70, 71 };
        std::vector<std::uint64_t> addresses;

        for (auto size: sizes) {
            addresses.push_back(cut.alloc(size));
        }

        // validate sizes
        for (std::size_t i = 0; i < sizes.size(); ++i) {
            ASSERT_EQ(sizes[i], cut.getAllocSize(addresses[i]));
        }
    }

    TEST_F( SlabAllocatorTests , testCalculateAdminSpaceSize )
    {   
        auto calculated_size = db0::SlabAllocator::calculateAdminSpaceSize(page_size);
        // construct acutal SlabAllocator with identical parameters
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), 0, page_size * 32, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), 0, page_size * 32, page_size);
        
        ASSERT_EQ(cut.getAdminSpaceSize(false), calculated_size);
    }
    
    TEST_F( SlabAllocatorTests , testSlabAllocatorCannotBeCreatedIfSizeTooSmall )
    {  
        // throws because size of the administrative space exceeds the size of the slab 
        ASSERT_ANY_THROW(
            db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), 0, page_size * 3, page_size);
        );
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorCanDynamicallyManageAvailableSize )
    {   
        // this test if to check if the available data space is adjusted according to changing administrative space
        // create allocator over the 4-pages slab
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), 0, page_size * 32, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), 0, page_size * 32, page_size);
                
        std::vector<std::uint64_t> addresses;
        // make allocations until the entire slab is occupied
        std::uint64_t max_addr = 0;
        int size = 1;
        int max_size = 0;
        std::size_t total_size = 0;
        while (true) {
            auto addr = cut.tryAlloc(size);
            if (!addr)
                break;
            addresses.push_back(*addr);
            if (*addr > max_addr) {
                max_addr = *addr;
                max_size = size;
            }
            total_size += size;
            ++size;
        }
        
        // make sure the max address is not conflicting with the admin space
        ASSERT_TRUE(max_addr + max_size + cut.getAdminSpaceSize(true) <= cut.getSlabSize());
        
        size = 1;
        for (auto &addr: addresses) {
            ASSERT_EQ(size, cut.getAllocSize(addr));
            ++size;
        }
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorAdminOverhead )
    {
        srand(123622u);
        // 4MB slab
        auto size_ = 4 * 1024 * 1024;
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), 0, size_, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), 0, size_, page_size);

        // perform random allocations until full
        for (;;) {
            // random size alloc
            auto addr = cut.tryAlloc(rand() % 1024 + 1);
            if (!addr)
                break;
        }

        // measure the administrative overhead
        auto admin_overhead = cut.getAdminSpaceSize(true) / (double)size_;
        ASSERT_TRUE(admin_overhead < 0.1);        
    }
    
    TEST_F( SlabAllocatorTests , testSlabAllocatorCanFillAvailableCapacity )
    {
        // 64kb slab
        auto size_ = 16 * 4096;
        auto init_capacity = db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), 0, size_, page_size);        
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), 0, size_, page_size, init_capacity);
                
        std::size_t total_allocated = 0;
        // perform 100b allocations until full
        for (;;) {            
            auto addr = cut.tryAlloc(100);
            if (!addr)
                break;

            total_allocated += 100;    
        }

        ASSERT_TRUE(cut.getRemainingCapacity() < 100);
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorEmptyMethod )
    {
        srand(123622u);
        // 4MB slab
        auto size_ = 4 * 1024 * 1024;
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), 0, size_, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), 0, size_, page_size);
        
        ASSERT_TRUE(cut.empty());
        // perform random allocations until full
        std::vector<std::uint64_t> addresses;    
        for (int i = 0;i < 100; ++i) {
            // random size alloc
            addresses.push_back(cut.alloc(rand() % 1024 + 1));
        }

        ASSERT_FALSE(cut.empty());
        // free all allocations
        for (auto addr: addresses) {
            cut.free(addr);
        }
        ASSERT_TRUE(cut.empty());
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorAllocSpeed )
    {        
        auto size_ = 64 * 1024 * 1024;
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), 0, size_, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), 0, size_, page_size);

        // measure speed
        auto start = std::chrono::high_resolution_clock::now();
        std::size_t total_bytes = 0;
        for (int i = 0; i < 100000; ++i) {
            cut.alloc(100);
            total_bytes += 100;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "SlabAllocator alloc speed: " << elapsed.count() << "ms" << std::endl;
        std::cout << "Total bytes: " << total_bytes << std::endl;
        std::cout << "MB / sec : " << (total_bytes / 1024.0 / 1024.0) * 1000.0 / elapsed.count() << std::endl;
    }

    TEST_F( SlabAllocatorTests , testSlabAllocatorCanMakeAddressUnique )
    {        
        auto size_ = 64 * 1024 * 1024;
        db0::SlabAllocator::formatSlab(m_memspace.getPrefixPtr(), 0, size_, page_size);
        db0::SlabAllocator cut(m_memspace.getPrefixPtr(), 0, size_, page_size);

        auto addr = cut.alloc(100);
        auto addr1 = addr;
        ASSERT_TRUE(cut.makeAddressUnique(addr1));
        auto addr2 = addr;
        ASSERT_TRUE(cut.makeAddressUnique(addr2));
        ASSERT_NE(addr, addr1);
        ASSERT_NE(addr1, addr2);
    }
    
}