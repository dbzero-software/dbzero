#pragma once

#include <string>
#include <dbzero/core/collections/pools/StringPools.hpp>

namespace db0::object_model

{
    
    using LP_String = db0::LP_String;
    struct EnumTypeDef;
    
    struct EnumValue_UID
    {
        // the associated enum's UID (i.e. its address from the dedicated address pool)
        std::uint32_t m_enum_uid;
        LP_String m_value;

        EnumValue_UID(std::uint32_t enum_uid, LP_String value);
        EnumValue_UID(std::uint64_t);

        std::uint64_t asULong() const;
    };
    
    // EnumValue placeholder when EnumValue could not be created
    // e.g. due to read-only access
    struct EnumValueRepr
    {
        // the associated type definition
        std::shared_ptr<EnumTypeDef> m_enum_type_def;
        // the string representation
        std::string m_str_repr;

        EnumValueRepr(std::shared_ptr<EnumTypeDef>, const std::string &str_repr);
        ~EnumValueRepr();
        
        const EnumTypeDef &getEnumTypeDef() const;
        
        static EnumValueRepr &makeNew(void *at, std::shared_ptr<EnumTypeDef>, const std::string &str_repr);
    };
    
    struct EnumValue
    {
        // associated fixture UUID (for context validation purposes)
        std::uint64_t m_fixture_uuid = 0;
        // the associated enum's UID (i.e. its address from the dedicated address pool)
        std::uint32_t m_enum_uid = 0;
        LP_String m_value;
        // the string representation
        std::string m_str_repr;
        
        // get unique tag identifier (unique within its prefix)
        EnumValue_UID getUID() const;
        
        operator bool() const {
            return m_fixture_uuid && m_enum_uid;
        }
    };
    
} 

namespace std

{
    
    ostream &operator<<(ostream &, const db0::object_model::EnumValue &);

}