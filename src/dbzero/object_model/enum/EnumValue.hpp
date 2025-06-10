#pragma once

#include <string>
#include "EnumDef.hpp"
#include <dbzero/core/collections/pools/StringPools.hpp>
#include <dbzero/object_model/LangConfig.hpp>
#include <dbzero/core/serialization/Types.hpp>

namespace db0::object_model

{
    
    using LP_String = db0::LP_String;
    using ObjectSharedPtr = db0::object_model::LangConfig::ObjectSharedPtr;
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
    
    class [[gnu::packed]] o_enum_value_repr: public db0::o_base<o_enum_value_repr, 0, false>
    {
    protected:
        using self = o_enum_value_repr;
        using super_t = db0::o_base<o_enum_value_repr, 0, false>;
        friend super_t;

        o_enum_value_repr(const EnumDef &, const char *str_repr, const char *prefix_name = nullptr);

    public:
        const o_enum_def &enum_def() const;
        const o_string &str_repr() const;
        const o_nullable_string &prefix_name() const;

        static std::size_t measure(const EnumDef &, const char *str_repr, const char *prefix_name = nullptr); 
        
        template <typename T> static std::size_t safeSizeOf(T at)
        {
            return sizeOfMembers(at)
                (o_enum_def::type())
                (o_string::type())
                (o_nullable_string::type());
        }   
    };
    
    // EnumValue placeholder when EnumValue could not be created
    // e.g. due to read-only access
    struct EnumValueRepr
    {
        using LangConfig = db0::object_model::LangConfig;
        using LangToolkit = LangConfig::LangToolkit;
        
        // the associated type definition
        std::shared_ptr<EnumTypeDef> m_enum_type_def;
        // the string representation
        std::string m_str_repr;

        EnumValueRepr(std::shared_ptr<EnumTypeDef>, const std::string &str_repr);
        ~EnumValueRepr();
        
        const EnumTypeDef &getEnumTypeDef() const;
        
        static EnumValueRepr &makeNew(void *at, std::shared_ptr<EnumTypeDef>, const std::string &str_repr);

        bool operator==(const EnumValueRepr &other) const;        
        bool operator!=(const EnumValueRepr &other) const;

        void serialize(std::vector<std::byte> &buffer) const;

        // NOTE: deserialize will attempt to convert to EnumValue if possible
        static ObjectSharedPtr deserialize(db0::swine_ptr<Fixture> &, std::vector<std::byte>::const_iterator &iter,
            std::vector<std::byte>::const_iterator end);
    };
    
    struct EnumValue
    {   
        using LangConfig = db0::object_model::LangConfig;
        using ObjectPtr = LangConfig::ObjectPtr;
        using ObjectSharedPtr = LangConfig::ObjectSharedPtr;

        // the associated fixture (for context validation purposes)     
        db0::swine_ptr<Fixture> m_fixture;
        // the associated enum's UID (i.e. its address from the dedicated address pool)
        // Note that m_enum_id = 0 may be a vailid enum's UID
        std::uint32_t m_enum_uid = 0;
        LP_String m_value;
        // the string representation
        std::string m_str_repr;
        
        // get unique tag identifier (unique within its prefix)
        EnumValue_UID getUID() const;
        
        operator bool() const {
            return m_fixture && m_value;
        }

        bool operator==(const EnumValue &) const;
        bool operator!=(const EnumValue &) const;
        
        // get hash for persistent storage (computed from string representation)
        // this is to allow matching enum values from different prefixes
        std::int64_t getPermHash() const;

        void serialize(std::vector<std::byte> &buffer) const;
        
        static ObjectSharedPtr deserialize(Snapshot &, std::vector<std::byte>::const_iterator &iter,
            std::vector<std::byte>::const_iterator end);
    };
    
    struct [[gnu::packed]] o_enum_value: db0::o_fixed<o_enum_value>
    {
        const std::uint64_t m_fixture_uuid;
        const std::uint32_t m_enum_uid;
        LP_String m_value;

        o_enum_value(std::uint64_t fixture_uuid, std::uint32_t enum_uid, LP_String value);
        o_enum_value(const EnumValue &);
        
        EnumValue_UID getUID() const;
    };
    
} 

namespace std

{
    
    ostream &operator<<(ostream &, const db0::object_model::EnumValue &);

}