#include "Address.hpp"

namespace db0

{

    UniqueAddress makeUniqueAddr(std::uint64_t offset, std::uint16_t id) {
        return UniqueAddress(Address::fromOffset(offset), id);
    }
    
}