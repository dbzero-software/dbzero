#include "TypedAddress.hpp"

namespace db0::object_model

{

    bool TypedAddress::operator==(const TypedAddress &other) const {
        return m_value == other.m_value;
    }

    void TypedAddress::setAddress(std::uint64_t address)
    {
        assert((db0::getPhysicalAddress(address) & 0xFF00000000000000) == 0);
        m_value = (m_value & 0xFF00000000000000) | db0::getPhysicalAddress(address);
    }
    
    void TypedAddress::setType(StorageClass type) {
        m_value = (m_value & 0x00FFFFFFFFFFFFFF) | ((std::uint64_t)type << 56);
    }
    
    bool TypedAddress::operator<(const TypedAddress &other) const {
        return m_value < other.m_value;
    }
    
}   