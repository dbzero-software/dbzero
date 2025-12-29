// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "EnumValue.hpp"
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>
#include <dbzero/object_model/enum/Enum.hpp>
#include <dbzero/workspace/Snapshot.hpp>
#include <dbzero/core/utils/hash_func.hpp>

namespace db0::object_model

{

    o_enum_value_repr::o_enum_value_repr(const EnumDef &enum_def, const char *str_repr, const char *prefix_name)
    {
        arrangeMembers()
            (o_enum_def::type(), enum_def)
            (o_string::type(), str_repr)
            (o_nullable_string::type(), prefix_name);
    }

    std::size_t o_enum_value_repr::measure(const EnumDef &enum_def, const char *str_repr, const char *prefix_name)
    {
        return measureMembers()
            (o_enum_def::type(), enum_def)
            (o_string::type(), str_repr)
            (o_nullable_string::type(), prefix_name);            
    }

    const o_enum_def &o_enum_value_repr::enum_def() const {
        return this->getDynFirst(o_enum_def::type());
    }

    const o_string &o_enum_value_repr::str_repr() const {
        return this->getDynAfter(this->enum_def(), o_string::type());   
    }

    const o_nullable_string &o_enum_value_repr::prefix_name() const {
        return this->getDynAfter(this->str_repr(), o_nullable_string::type());
    }

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
    
    bool EnumValueRepr::operator==(const EnumValueRepr &other) const {
        return m_enum_type_def == other.m_enum_type_def && m_str_repr == other.m_str_repr;
    }
    
    bool EnumValueRepr::operator!=(const EnumValueRepr &other) const {
        return m_enum_type_def != other.m_enum_type_def || m_str_repr != other.m_str_repr;
    }
    
    void EnumValueRepr::serialize(std::vector<std::byte> &buffer) const
    {
        db0::serial::emplaceBack<o_enum_value_repr>(buffer, m_enum_type_def->m_enum_def, m_str_repr.c_str(),
            m_enum_type_def->getPrefixNamePtr());
        // stop byte (sentinel)
        db0::serial::write<std::uint8_t>(buffer, 0);    
    }
    
    ObjectSharedPtr EnumValueRepr::deserialize(db0::swine_ptr<Fixture> &fixture, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end)
    {        
        auto &enum_value_repr = db0::serial::pop<o_enum_value_repr>(iter, end);
        auto sentinel = db0::serial::read<std::uint8_t>(iter, end);
        if (sentinel != 0) {
            THROWF(db0::InputException) << "Invalid sentinel byte for EnumValue deserialization";
        }
                
        auto &enum_factory = fixture->get<db0::object_model::EnumFactory>();
        const char *prefix_name = nullptr;
        std::string str_prefix_name;
        if (!enum_value_repr.prefix_name().isNull()) {
            str_prefix_name = enum_value_repr.prefix_name().extract();
            prefix_name = str_prefix_name.c_str();
        }
        
        auto enum_def = enum_value_repr.enum_def().get();
        auto _enum = enum_factory.tryGetExistingEnum(enum_def, prefix_name);
        auto str_repr = enum_value_repr.str_repr().extract();
        if (_enum) {            
            return _enum->getLangValue(str_repr.c_str());
        } else {
            // enum does not exist (yet)? 
            // we must deserialize as enum value representation
            auto &type_manager = LangToolkit::getTypeManager();
            return LangToolkit::makeEnumValueRepr(type_manager.findEnumTypeDef(enum_def), str_repr.c_str());
        }
    }
    
    std::int64_t EnumValueRepr::getPermHash() const {
        return std::hash<std::string>{}(m_str_repr);
    }
    
    bool EnumValue::operator==(const EnumValue &other) const
    {
        using EnumFactory = db0::object_model::EnumFactory;
        if (db0::is_same(m_fixture, other.m_fixture)) {
            // compare values from the same prefix
            return m_enum_uid == other.m_enum_uid && m_value == other.m_value;
        } else {
            // First, compare if the enum definitions match
            auto enum_ = m_fixture.safe_lock()->get<EnumFactory>().getEnumByUID(m_enum_uid);
            auto other_enum = other.m_fixture.safe_lock()->get<EnumFactory>().getEnumByUID(other.m_enum_uid);
            if (enum_->getEnumDef() != other_enum->getEnumDef()) {
                return false;
            }
            
            // compare string representations if definitions match
            return m_str_repr == other.m_str_repr;
        }
    }
    
    bool EnumValue::operator!=(const EnumValue &other) const {
        return !(*this == other);
    }
    
    std::int64_t EnumValue::getPermHash() const {
        return murmurhash64A(m_str_repr.c_str(), m_str_repr.size());
    }
    
    void EnumValue::serialize(std::vector<std::byte> &buffer) const
    {        
        // NOTE: both enum value + enum def needs to be serialized
        // for fallback resolution in case the client has no access to a specific prefix
        // but has a reference to the enum type in its scope
        db0::serial::emplaceBack<o_enum_value>(buffer, *this);
        auto &enum_factory = m_fixture.safe_lock()->get<db0::object_model::EnumFactory>();
        auto _enum = enum_factory.getEnumByUID(m_enum_uid);
        _enum->getEnumDef().serialize(buffer);
        // stop byte (sentinel)
        db0::serial::write<std::uint8_t>(buffer, 0);
    }
    
    EnumValue::ObjectSharedPtr EnumValue::deserialize(Snapshot &workspace, std::vector<std::byte>::const_iterator &iter,
        std::vector<std::byte>::const_iterator end)
    {
        auto &enum_value = db0::serial::pop<o_enum_value>(iter, end);
        const auto &enum_def = db0::serial::pop<o_enum_def>(iter, end);
        auto sentinel = db0::serial::read<std::uint8_t>(iter, end);
        if (sentinel != 0) {
            THROWF(db0::InputException) << "Invalid sentinel byte for EnumValue deserialization";
        }
        
        auto fixture = workspace.tryGetFixture(enum_value.m_fixture_uuid);
        if (fixture) {
            auto &enum_factory = fixture->get<db0::object_model::EnumFactory>();
            auto _enum = enum_factory.getEnumByUID(enum_value.m_enum_uid);
            return _enum->getLangValue(enum_value.getUID());
        } else {
            // if unable to resolve the destination fixture, resolve as EnumValueRepr
            auto &type_manager = LangToolkit::getTypeManager();
            return LangToolkit::makeEnumValueRepr(
                type_manager.findEnumTypeDef(enum_def.get()), enum_value.str_repr().extract().c_str()
            );
        }
    }
    
    o_enum_value::o_enum_value(std::uint64_t fixture_uuid, std::uint32_t enum_uid, LP_String value, const std::string &str_repr)
        : m_fixture_uuid(fixture_uuid)
        , m_enum_uid(enum_uid)
        , m_value(value)
    {
        arrangeMembers()
            (db0::o_string::type(), str_repr);        
    }
    
    o_enum_value::o_enum_value(const EnumValue &enum_value)
        : o_enum_value(enum_value.m_fixture.safe_lock()->getUUID(), enum_value.m_enum_uid, 
            enum_value.m_value, enum_value.m_str_repr)
    {
    }

    const o_string &o_enum_value::str_repr() const {
        return this->getDynFirst(db0::o_string::type());
    }

    EnumValue_UID o_enum_value::getUID() const {
        return { m_enum_uid, m_value };
    }
    
    std::size_t o_enum_value::measure(std::uint64_t fixture_uuid, std::uint32_t enum_uid, LP_String value, 
        const std::string &str_repr)
    {
        return measureMembers()
            (db0::o_string::type(), str_repr);
    }
    
    std::size_t o_enum_value::measure(const EnumValue &enum_value)
    {
        return measureMembers()
            (db0::o_string::type(), enum_value.m_str_repr);
    }

} 

namespace std

{

    ostream &operator<<(ostream &os, const db0::object_model::EnumValue &enum_value) {
        return os << "EnumValue(" << enum_value.m_str_repr << ")";
    }
    
}