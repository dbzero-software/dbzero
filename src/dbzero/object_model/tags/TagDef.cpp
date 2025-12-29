// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "TagDef.hpp"
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/class/Class.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0::object_model

{
    
    TagDef::TagDef(TypeObjectPtr type, type_as_tag)
        : m_is_type_tag(true)
        , m_object(reinterpret_cast<ObjectPtr>(type))
    {
    }
    
    TagDef::TagDef(std::uint64_t fixture_uuid, Address address, ObjectPtr obj_ptr)
        : m_fixture_uuid(fixture_uuid)
        , m_address(address)
        , m_object(obj_ptr)
    {
    }
    
    bool TagDef::operator==(const TagDef &other) const {
        return this->m_address == other.m_address && this->m_fixture_uuid == other.m_fixture_uuid;
    }

    bool TagDef::operator!=(const TagDef &other) const {
        return !(*this == other);
    }
    
    Address TagDef::getAddress(ClassFactory &class_factory) const
    {
        if (!m_address.isValid()) {
            return getLongAddress(class_factory).second;
        }
        return m_address;
    }
    
    std::pair<std::uint64_t, Address> TagDef::getLongAddress(ClassFactory &class_factory) const
    {
        if (!m_address.isValid()) {
            assert(m_object.get() != nullptr);
            auto lang_type = reinterpret_cast<TypeObjectPtr>(m_object.get());
            auto type = class_factory.tryGetExistingType(lang_type);
            // try creating the dbzero class when type is accessed for the first time
            if (!type) {
                FixtureLock fixture(class_factory.getFixture());
                type = class_factory.getOrCreateType(lang_type);
            }
            // NOTE: here we use the address of the member's vector to distinguish from auto-assigned type tags (types)
            m_address = type->getMembersMatrix().getAddress();
            m_fixture_uuid = type->getFixture()->getUUID();
        }
        // returns fixture UUID + Address
        return { m_fixture_uuid, m_address };
    }
    
    std::uint64_t TagDef::tryGetFixtureUUID() const {
        // NOTE: fixture UUID not available for types as tags
        return m_fixture_uuid;
    }

    std::uint64_t TagDef::getHash() const
    {
        if (m_is_type_tag) {
            return static_cast<std::uint64_t>(reinterpret_cast<std::uintptr_t>(m_object.get()));
        }
        return m_address.getOffset() ^ m_fixture_uuid;
    }   

}
