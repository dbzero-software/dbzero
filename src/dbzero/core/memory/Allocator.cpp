#include "Allocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    std::uint64_t Allocator::alloc(std::size_t size, std::uint32_t slot_num, bool aligned, bool unique)
    {
        auto result = tryAlloc(size, slot_num, aligned, unique);
        if (!result) {
            THROWF(InternalException) << "Allocator: out of memory" << THROWF_END;
        }
        return *result;
    }
    
    void Allocator::flush() const {
    }
    
    bool Allocator::inRange(std::uint64_t address) const {
        return true;
    }

    std::uint64_t makeLogicalAddress(std::uint64_t address, std::uint16_t instance_id)
    {
        assert(isPhysicalAddress(address));
        return (static_cast<std::uint64_t>(instance_id) << 50) | address;
    }
    
}