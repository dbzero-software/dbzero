// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "PyEnumType.hpp"
#include "PyEnum.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>

namespace db0::python

{
    
    PyEnumData::PyEnumData(const EnumFullDef &enum_def, const char *prefix_name)
        : m_enum_type_def(std::make_shared<EnumTypeDef>(enum_def, prefix_name))
        , m_fixture_uuid(tryGetFixtureUUID(prefix_name))
    {
    }
    
    bool PyEnumData::exists() const
    {
        assert(m_enum_type_def);
        auto fixture_uuid = tryGetFixtureUUID();
        if (!fixture_uuid) {
            // failed to resolve the fixture UUID
            return false;
        }

        if (m_enum_cache.find(fixture_uuid) != m_enum_cache.end()) {
            return true;
        }

        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(fixture_uuid, AccessType::READ_ONLY);
        const auto &enum_factory = fixture->get<EnumFactory>();
        return enum_factory.tryGetExistingEnum(*m_enum_type_def) != nullptr;
    }

    const Enum &PyEnumData::get() const
    {        
        auto fixture_uuid = getFixtureUUID();
        auto it = m_enum_cache.find(fixture_uuid);
        if (it != m_enum_cache.end()) {
            return *it->second;
        }

        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(fixture_uuid, AccessType::READ_ONLY);
        const auto &enum_factory = fixture->get<EnumFactory>();
        // use empty module name since it's unknown
        auto enum_ptr = enum_factory.getExistingEnum(*m_enum_type_def);
        // popluate enum's value cache
        for (auto &value: enum_ptr->getValues()) {
            enum_ptr->getLangValue(value);
        }

        m_enum_cache[fixture_uuid] = enum_ptr;
        return *enum_ptr;
    }

    Enum *PyEnumData::tryCreate()
    {
        auto fixture_uuid = getFixtureUUID();
        auto it = m_enum_cache.find(fixture_uuid);
        if (it != m_enum_cache.end()) {
            return it->second.get();
        }

        // either get a specific or current prefix (must already be opened)
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().tryGetFixture(fixture_uuid);
        if (!fixture) {
            return nullptr;
        }

        auto &enum_factory = fixture->get<EnumFactory>();
        // use empty module name since it's unknown
        auto enum_ptr = enum_factory.tryGetOrCreateEnum(*m_enum_type_def);
        if (enum_ptr) {
            // popluate enum's value cache
            for (auto &value: enum_ptr->getValues()) {
                enum_ptr->getLangValue(value);
            }
            m_enum_cache[fixture_uuid] = enum_ptr;
        }
        return enum_ptr.get();
    }
    
    Enum &PyEnumData::create()
    {
        auto enum_ptr = tryCreate();
        if (!enum_ptr) {
            THROWF(db0::InputException) << "Unable to create enum: " << *m_enum_type_def;
        }
        return *enum_ptr;
    }
    
    void PyEnumData::close()
    {
        m_enum_cache.clear();
        m_fixture_uuid = std::nullopt;        
    }
        
    bool PyEnumData::hasValue(const char *value) const
    {
        assert(value);
        // check if enum value exists in the definition
        const auto &enum_def = m_enum_type_def->m_enum_def;
        return (std::find(enum_def.m_values.begin(), enum_def.m_values.end(), value) != enum_def.m_values.end());
    }
    
    const std::vector<std::string> &PyEnumData::getValueDefs() const {
        return m_enum_type_def->m_enum_def.m_values;
    }

    std::size_t PyEnumData::size() const {
        return m_enum_type_def->m_enum_def.m_values.size();
    }
    
    std::uint64_t PyEnumData::getFixtureUUID() const
    {
        if (!m_fixture_uuid) {
            m_fixture_uuid = tryGetFixtureUUID(m_enum_type_def->getPrefixNamePtr());
        }
        
        if (!m_fixture_uuid) {
            auto prefix_name_ptr = m_enum_type_def->getPrefixNamePtr();
            if (prefix_name_ptr) {
                // make the error message more informative
                THROWF(db0::PrefixNotFoundException) << "Prefix does not exist or unable to open: " << prefix_name_ptr;
            }
            THROWF(db0::InputException) << "Unable to resolve the scope of: " << *m_enum_type_def;
        }

        auto fixture_uuid = *m_fixture_uuid;
        if (!fixture_uuid) {
            // retrieve the current fixture UUID
            if (!PyToolkit::getPyWorkspace().hasWorkspace()) {
                THROWF(db0::InputException) << "Unable to resolve the scope of: " << *m_enum_type_def;
            }
            auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
            return workspace.getCurrentFixture()->getUUID();
        }
        return fixture_uuid;
    }

    std::uint64_t PyEnumData::tryGetFixtureUUID() const
    {
        if (!m_fixture_uuid) {
            m_fixture_uuid = tryGetFixtureUUID(m_enum_type_def->getPrefixNamePtr());        
        }
        
        if (!m_fixture_uuid) {
            return 0;
        }
        
        auto fixture_uuid = *m_fixture_uuid;
        if (!fixture_uuid) {
            // retrieve the current fixture UUID
            if (!PyToolkit::getPyWorkspace().hasWorkspace()) {
                return 0;
            }
            auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
            return workspace.getCurrentFixture()->getUUID();
        }
        return fixture_uuid;
    }

    std::optional<std::uint64_t> PyEnumData::tryGetFixtureUUID(const char *prefix_name)
    {
        if (prefix_name) {
            if (!PyToolkit::getPyWorkspace().hasWorkspace()) {
                return std::nullopt;
            }
            
            auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
            auto fixture = workspace.tryGetFixture(prefix_name, AccessType::READ_ONLY);
            if (!fixture) {
                return std::nullopt;
            }
            return fixture->getUUID();
        } else {
            // fixture UUID = 0 represents the default fixture
            return 0;
        }
    }
    
    std::optional<std::string> getEnumKeyVariant(const EnumDef &enum_def, int variant_id)
    {
        return db0::object_model::getEnumKeyVariant(enum_def.m_type_id,
            enum_def.m_name, enum_def.m_module_name, enum_def.m_hash, variant_id
        );
    }
    
    std::optional<std::string> getEnumKeyVariant(const PyEnumData &enum_data, int variant_id) {
        return getEnumKeyVariant(enum_data.m_enum_type_def->m_enum_def, variant_id);
    }

}