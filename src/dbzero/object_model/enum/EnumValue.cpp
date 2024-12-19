#include "EnumValue.hpp"

namespace db0::object_model

{

    EnumValue_UID::EnumValue_UID(std::uint32_t enum_uid, LP_String value)
        : m_enum_uid(enum_uid)  
        , m_value(value)
    {
    }

    EnumValue_UID::EnumValue_UID(std::uint64_t uid)
        : m_enum_uid(static_cast<std::uint32_t>(uid >> 32) & 0x7FFFFFFF)
        , m_value(static_cast<std::uint32_t>(uid & 0xFFFFFFFF))
    {
        assert((uid >> 32) & 0x80000000);
    }
    
    std::uint64_t EnumValue_UID::asULong() const {
        // set the highest bit to 1 to distinguish from a regular address
        return (static_cast<std::uint64_t>(m_enum_uid | 0x80000000) << 32) | m_value.m_value;
    }
    
    EnumValue_UID EnumValue::getUID() const {
        return { m_enum_uid, m_value };
    }

    EnumValueRepr::EnumValueRepr(std::shared_ptr<EnumTypeDef> enum_type_def, const std::string &str_repr)
        : m_enum_type_def(enum_type_def)
        , m_str_repr(str_repr)
    {    
    }

    EnumValueRepr::~EnumValueRepr()
    {        
    }
    
    const EnumTypeDef &EnumValueRepr::getEnumTypeDef() const
    {
        assert(m_enum_type_def);
        return *m_enum_type_def;
    }

    EnumValueRepr &EnumValueRepr::makeNew(
        void *at, std::shared_ptr<EnumTypeDef> enum_type_def, const std::string &str_repr)
    {
        return *new(at) EnumValueRepr(enum_type_def, str_repr);
    }
    
} 

namespace std

{

    ostream &operator<<(ostream &os, const db0::object_model::EnumValue &enum_value) {
        return os << "EnumValue(" << enum_value.m_str_repr << ")";
    }
    
}