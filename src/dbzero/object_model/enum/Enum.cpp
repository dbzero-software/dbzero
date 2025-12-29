// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "Enum.hpp"

namespace db0::object_model

{

    GC0_Define(Enum)
    
    o_enum::o_enum(Memspace &memspace)
        : m_values(memspace)
        , m_ordered_values(memspace)
    {
    }
    
    Enum::Enum(db0::swine_ptr<Fixture> &fixture, const std::string &name, const std::string &module_name, 
        const std::vector<std::string> &values, const char *type_id)
        : super_t(fixture, *fixture)
        , m_fixture_uuid(fixture->getUUID())
        , m_uid(this->fetchUID())
        , m_string_pool_ref(fixture->getLimitedStringPool())
        , m_values((*this)->m_values(*fixture))
        , m_ordered_values((*this)->m_ordered_values(*fixture))
        , m_enum_def(name, module_name, values, type_id)
    {
        for (auto &value: values) {
            auto value_ref = m_string_pool_ref.addRef(value);
            m_values.insert(value_ref);
            m_ordered_values.push_back(value_ref);
        }
        modify().m_name = m_string_pool_ref.addRef(name);
        modify().m_module_name = m_string_pool_ref.addRef(module_name);
        if (type_id) {
            modify().m_type_id = m_string_pool_ref.addRef(type_id);
        }
        modify().m_values = m_values;
        modify().m_ordered_values = m_ordered_values;
    }
    
    Enum::Enum(db0::swine_ptr<Fixture> &fixture, Address address)
        : super_t(super_t::tag_from_address(), fixture, address)
        , m_fixture_uuid(fixture->getUUID())
        , m_uid(this->fetchUID())
        , m_string_pool_ref(fixture->getLimitedStringPool())
        , m_values((*this)->m_values(*fixture))
        , m_ordered_values((*this)->m_ordered_values(*fixture))
        , m_enum_def(makeEnumDef())
    {
    }
    
    Enum::~Enum()
    {
        // unregister needs to be called before destruction of members
        unregister();
    }

    LP_String Enum::tryFind(const char *value) const
    {
        assert(value);
        decltype(LP_String::m_value) value_id;
        db0::swine_ptr<Fixture> lock;
        if (!getStringPool(lock).find(value, value_id)) {
            return LP_String();
        }
        if (m_values.find(value_id) == m_values.end()) {
            return LP_String();
        }
        return value_id;
    }

    LP_String Enum::find(const char *value) const
    {
        assert(value);
        decltype(LP_String::m_value) value_id;
        db0::swine_ptr<Fixture> lock;
        if (!getStringPool(lock).find(value, value_id)) {
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
    
    const EnumFullDef &Enum::getEnumDef() const {
        return m_enum_def;
    }
    
    EnumValue Enum::tryGet(const char *str_value) const
    {
        assert(str_value);
        auto value = tryFind(str_value);
        if (!value) {
            return {};
        }
        return { this->getFixture(), m_uid, value, std::string(str_value) };
    }

    EnumValue Enum::get(const char *str_value) const
    {
        assert(str_value);
        auto value = find(str_value);
        return { this->getFixture(), m_uid, value, std::string(str_value) };
    }

    EnumValue Enum::get(EnumValue_UID enum_value_uid) const
    {
        if (m_values.find(enum_value_uid.m_value) == m_values.end()) {
            THROWF(db0::InputException) << "Enum value not found by UID: " << enum_value_uid.asULong();
        }
        auto fixture = this->getFixture();
        return { db0::weak_swine_ptr<Fixture>(fixture), enum_value_uid.m_enum_uid, enum_value_uid.m_value,
            getStringPool(fixture).fetch(enum_value_uid.m_value) 
        };
    }
    
    std::vector<EnumValue> Enum::getValues() const
    {
        std::vector<EnumValue> values;
        auto fixture = this->getFixture();
        for (auto value: m_ordered_values) {
            values.push_back({ db0::weak_swine_ptr<Fixture>(fixture), m_uid, value, getStringPool(fixture).fetch(value) });
        }
        return values;
    }
    
    Enum::ObjectSharedPtr Enum::getLangValue(const char *value) const
    {
        // NOTE: m_cache must NOT be cleared during the lifetime of the process
        // there's a strong assumption that each enum value is represented by the same native object
        auto it_cache = m_cache.find(value);
        if (it_cache == m_cache.end()) {
            auto lang_value = LangToolkit::makeEnumValue(get(value));
            it_cache = m_cache.insert({value, lang_value}).first;
        }
        return it_cache->second.get();
    }
    
    Enum::ObjectSharedPtr Enum::tryGetLangValue(const char *value) const
    {
        auto it_cache = m_cache.find(value);
        if (it_cache != m_cache.end()) {
            return it_cache->second.get();    
        }
        
        auto enum_value = tryGet(value);
        if (!enum_value) {
            return nullptr;
        }
        auto lang_value = LangToolkit::makeEnumValue(enum_value);
        it_cache = m_cache.insert({value, lang_value}).first;
        return it_cache->second.get();
    }
    
    Enum::ObjectSharedPtr Enum::getLangValue(EnumValue_UID value_uid) const {
        return getLangValue(get(value_uid).m_str_repr.c_str());
    }

    Enum::ObjectSharedPtr Enum::getLangValue(const EnumValue &enum_value) const {
        return getLangValue(enum_value.m_str_repr.c_str());
    }
    
    Enum::ObjectSharedPtr Enum::getLangValue(unsigned int at) const
    {        
        // NOTE: m_ord_cache must NOT be cleared during the lifetime of the process
        // there's a strong assumption that each enum value is represented by the same native object
        assert(at < m_ordered_values.size());
        auto it_cache = m_ord_cache.find(at);
        if (it_cache == m_ord_cache.end()) {
            // retieve by name
            db0::swine_ptr<Fixture> lock;
            auto lang_value = getLangValue(getStringPool(lock).fetch(m_ordered_values[at]).c_str());
            it_cache = m_ord_cache.insert({at, lang_value}).first;
        }
        return it_cache->second.get();
    }
    
    std::optional<std::string> getEnumKeyVariant(std::optional<std::string> type_id, std::optional<std::string> enum_name,
        std::optional<std::string> module_name, std::uint32_t hash, int variant_id)
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
                if (enum_name && module_name) {
                    std::stringstream _str;
                    _str << "enum:" << *enum_name << ".pkg:" << *module_name;
                    return _str.str();                
                }
            }
            break;

            case 2: {
                // variant 2. name + values (hash)
                if (enum_name && hash) {
                    std::stringstream _str;
                    _str << "enum:" << *enum_name << "#:" << hash;
                    return _str.str();
                }
            }
            break;
            
            case 3: {
                // variant 3. module + values (hash)
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
    
    std::string Enum::getName() const
    {
        db0::swine_ptr<Fixture> lock;
        return getStringPool(lock).fetch((*this)->m_name);
    }
    
    std::string Enum::getModuleName() const 
    {
        db0::swine_ptr<Fixture> lock;
        return getStringPool(lock).fetch((*this)->m_module_name);
    }

    std::optional<std::string> Enum::getTypeID() const 
    {
        if ((*this)->m_type_id) {
            db0::swine_ptr<Fixture> lock;
            return getStringPool(lock).fetch((*this)->m_type_id);
        }
        return std::nullopt;
    }

    void Enum::detach() const 
    {
        m_values.detach();
        m_ordered_values.detach();
        super_t::detach();
    }

    void Enum::commit() const
    {
        m_values.commit();
        m_ordered_values.commit();
        super_t::commit();
    }

    std::size_t Enum::size() const {
        return m_values.size();
    }

    EnumFullDef Enum::makeEnumDef() const
    {
        std::vector<std::string> values;
        db0::swine_ptr<Fixture> lock;
        for (auto value: m_ordered_values) {
            values.push_back(getStringPool(lock).fetch(value));
        }
        return { getName(), getModuleName(), values, getTypeID() };
    }
    
    const RC_LimitedStringPool &Enum::getStringPool(db0::swine_ptr<Fixture> &lock) const
    {
        // must lock the Fixture while accessing the string pool
        if (!lock) {
            lock = this->getFixture();
        }
        return m_string_pool_ref;
    }
    
    UniqueAddress Enum::getUniqueAddress() const {
        return { this->getAddress(), UniqueAddress::INSTANCE_ID_MAX };
    }

}

namespace std 

{

    ostream &operator<<(ostream &os, const db0::object_model::Enum &enum_)
    {
        os << "Enum: " << enum_.getName() << " (" << enum_.getModuleName() << ")";
        return os;
    }

}