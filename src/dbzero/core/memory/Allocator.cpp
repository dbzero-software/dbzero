#include "Allocator.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{

    std::uint64_t Allocator::alloc(std::size_t size) {
        auto result = tryAlloc(size);
        if (!result) {
            THROWF(InternalException) << "Allocator: out of memory" << THROWF_END;
        }
        return *result;
    }

}