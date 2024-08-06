#include "Allocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    std::uint64_t Allocator::alloc(std::size_t size, std::uint32_t slot_num, bool aligned)
    {
        auto result = tryAlloc(size, slot_num, aligned);
        if (!result) {
            THROWF(InternalException) << "Allocator: out of memory" << THROWF_END;
        }
        return *result;
    }

}