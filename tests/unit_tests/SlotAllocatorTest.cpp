#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <utils/EmbeddedAllocator.hpp>
#include <dbzero/core/memory/SlotAllocator.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;

namespace tests

{

    class SlotAllocatorTest: public testing::Test
    {
    public:
    };
    
    TEST_F( SlotAllocatorTest , testSlotAllocatorDisallowUseSlot0 )
    {
        auto alloc = std::make_shared<EmbeddedAllocator>();
        SlotAllocator cut(alloc);
        ASSERT_ANY_THROW((cut.setSlot(0, std::make_shared<EmbeddedAllocator>())));
    }

    TEST_F( SlotAllocatorTest , testSlotAllocatorCanAllocateFromSpecificSlot )
    {
        auto alloc = std::make_shared<EmbeddedAllocator>();
        auto slot_alloc = std::make_shared<EmbeddedAllocator>();
        std::stringstream _str;
        slot_alloc->setAllocCallback([&](std::size_t, std::uint32_t slot_num, bool, bool, std::optional<std::uint64_t>) {
            _str << "slot_num:" << slot_num;
        });
        SlotAllocator cut(alloc);
        cut.setSlot(1, slot_alloc);
        // allocated with the general allocator
        cut.tryAlloc(100);
        // allocated with the slot allocator (as slot 0 since it's a forwarded call)
        cut.tryAlloc(100, 1);
        ASSERT_EQ(_str.str(), "slot_num:0");
    }
    
}
