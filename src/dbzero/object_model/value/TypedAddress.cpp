#include "TypedAddress.hpp"

namespace db0::object_model

{

    bool TypedAddress::operator==(const TypedAddress &other) const {
        return m_value == other.m_value;
    }

    void TypedAddress::setAddress(Address address) {
        m_value = (m_value & 0xFFFC000000000000) | address.getOffset();
    }
    
    void TypedAddress::setType(StorageClass type) {
        m_value = (m_value & 0x0003FFFFFFFFFFFF) | (static_cast<std::uint64_t>(type) << 50);
    }
    
    bool TypedAddress::operator<(const TypedAddress &other) const {
        return m_value < other.m_value;
    }
    
    TypedAddress toTypedAddress(const std::pair<UniqueAddress, StorageClass> &addr_with_type) {
        return { addr_with_type.second, addr_with_type.first.getAddress() };
    }

}   