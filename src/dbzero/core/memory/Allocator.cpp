#include "Allocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{
    
    std::optional<UniqueAddress> Allocator::tryAllocUnique(std::size_t size, std::uint32_t slot_num, bool aligned) {
        THROWF(InternalException) << "Allocator: unique allocation not supported by: " << typeid(*this).name() << THROWF_END;
    }
    
    Address Allocator::alloc(std::size_t size, std::uint32_t slot_num, bool aligned)
    {
        auto result = tryAlloc(size, slot_num, aligned);
        if (!result) {
            THROWF(InternalException) << "Allocator: out of memory" << THROWF_END;
        }
        return *result;
    }
    
    UniqueAddress Allocator::allocUnique(std::size_t size, std::uint32_t slot_num, bool aligned)
    {
        auto result = tryAllocUnique(size, slot_num, aligned);
        if (!result) {
            THROWF(InternalException) << "Allocator: out of memory" << THROWF_END;
        }
        return *result;
    }
    
    void Allocator::flush() const {
    }
    
    bool Allocator::inRange(Address) const {
        return true;
    }
    
}