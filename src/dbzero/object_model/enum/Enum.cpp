#include "Enum.hpp"


namespace db0::object_model

{

    GC0_Define(Enum)

    o_enum::o_enum(Memspace &memspace)
        : m_values(memspace)
    {
    }

    Enum::Enum(db0::swine_ptr<Fixture> &fixture, const std::string &name, const std::vector<std::string> &values,
        const char *type_id)
        : super_t(fixture, *fixture)
        , m_fixture_uuid(fixture->getUUID())
        , m_uid(this->fetchUID())
        , m_string_pool(fixture->getLimitedStringPool())
        , m_values((*this)->m_values(*fixture))
    {
        for (auto &value: values) {
            m_values.insert(m_string_pool.add(value));
        }
        modify().m_name = m_string_pool.add(name);
        modify().m_values = m_values;
    }
    
    Enum::Enum(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_fixture_uuid(fixture->getUUID())
        , m_uid(this->fetchUID())
        , m_string_pool(fixture->getLimitedStringPool())
        , m_values((*this)->m_values(*fixture))
    {
    }

    Enum *Enum::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture, const std::string &name,
        const std::vector<std::string> &values, const char *type_id)
    {
        return new (at_ptr) Enum(fixture, name, values, type_id);
    }

    LP_String Enum::find(const char *value) const
    {
        assert(value);        
        decltype(LP_String::m_value) value_id;
        if (!m_string_pool.find(value, value_id)) {
            THROWF(db0::InputException) << "Enum value not found: " << value;
        }
        if (m_values.find(value_id) == m_values.end()) {
            THROWF(db0::InputException) << "Enum value not found: " << value;
        }
        return value_id;
    }
    
    std::uint32_t Enum::fetchUID() const
    {
        // return UID as relative address from the underlying SLOT
        auto result = this->getFixture()->makeRelative(this->getAddress(), SLOT_NUM);
        // relative address must not exceed SLOT size
        assert(result < std::numeric_limits<std::uint32_t>::max());
        return result;
    }
    
    EnumValue Enum::get(const char *str_value) const
    {
        assert(str_value);
        auto value = find(str_value);
        return { m_fixture_uuid, m_uid, value, std::string(str_value) };
    }
    
    EnumValue Enum::get(EnumValue_UID enum_value_uid) const
    {
        if (m_values.find(enum_value_uid.m_value) == m_values.end()) {
            THROWF(db0::InputException) << "Enum value not found by UID: " << enum_value_uid.asULong();
        }
        return { m_fixture_uuid, enum_value_uid.m_enum_uid, enum_value_uid.m_value, 
            m_string_pool.fetch(enum_value_uid.m_value) };
    }

    std::vector<EnumValue> Enum::getValues() const
    {
        std::vector<EnumValue> values;
        for (auto value: m_values) {
            values.push_back({ m_fixture_uuid, m_uid, value, m_string_pool.fetch(value) });
        }
        return values;
    }
    
    Enum::ObjectSharedPtr Enum::getLangValue(const char *value) const
    {
        auto it_cache = m_cache.find(value);
        if (it_cache == m_cache.end()) {
            auto lang_value = LangToolkit::unloadEnumValue(get(value));
            it_cache = m_cache.insert({value, lang_value}).first;
        }
        return it_cache->second.get();
    }

    Enum::ObjectSharedPtr Enum::getLangValue(EnumValue_UID value_uid) const {
        return getLangValue(get(value_uid).m_str_repr.c_str());
    }

    Enum::ObjectSharedPtr Enum::getLangValue(const EnumValue &enum_value) const {
        return getLangValue(enum_value.m_str_repr.c_str());
    }

    std::optional<std::string> getEnumKeyVariant(std::optional<std::string> type_id, std::optional<std::string> enum_name,
        std::optional<std::string> module_name, const std::vector<std::string> &values, int variant_id)
    {
        switch (variant_id) {
            case 0: {                
                if (type_id) {
                    return type_id;
                }
                return std::nullopt;
            }
            break;

            case 1: {
                // type & module name are required
                assert(enum_name && module_name);
                std::stringstream _str;
                _str << "enum:" << *enum_name << ".pkg:" << *module_name;
                return _str.str();                
            }
            break;

            case 2: {
                // variant 2. name + fields
                // std::stringstream _str;
                // _str << "cls:" << _class.getTypeName() << "." << db0::python::getTypeFields(lang_class);
                // return _str.str();
            }
            break;

            case 3: {
                // variant 3. module + fields
                // std::stringstream _str;
                // _str << "pkg:" << _class.getModuleName() << "." << db0::python::getTypeFields(lang_class);
                // return _str.str();
            }
            break;

            default: {
                assert(false);
                THROWF(db0::InputException) << "Invalid type name variant id: " << variant_id;
            }
            break;
        }
        return std::nullopt;
    }

}