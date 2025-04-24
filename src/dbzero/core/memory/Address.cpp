#include "Address.hpp"

namespace db0

{

    Address Address::operator+(std::uint64_t offset) const {
        return Address(this->getOffset() + offset);
    }
    
}
