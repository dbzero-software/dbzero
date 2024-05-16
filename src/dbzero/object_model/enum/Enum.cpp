#include "Enum.hpp"


namespace db0::object_model

{

    GC0_Define(Enum)

    o_enum::o_enum(Memspace &memspace)
        : m_values(memspace)
    {
    }

    Enum::Enum(db0::swine_ptr<Fixture> &fixture, const std::vector<std::string> &values)
        : super_t(fixture, *fixture)
        , m_string_pool(fixture->getLimitedStringPool())
        , m_values((*this)->m_values(*fixture))
    {        
        for (auto &value: values) {
            m_values.insert(m_string_pool.add(value));
        }
    }
    
    Enum::Enum(db0::swine_ptr<Fixture> &fixture, std::uint64_t address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_string_pool(fixture->getLimitedStringPool())
        , m_values((*this)->m_values(*fixture))
    {
    }

    Enum *Enum::makeNew(void *at_ptr, db0::swine_ptr<Fixture> &fixture, const std::vector<std::string> &values) {
        return new (at_ptr) Enum(fixture, values);
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

}