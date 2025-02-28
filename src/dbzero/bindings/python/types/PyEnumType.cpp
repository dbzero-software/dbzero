#include "PyEnumType.hpp"
#include "PyEnum.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/PrefixName.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>

namespace db0::python

{
    
    PyEnumData::PyEnumData(const EnumDef &enum_def, const char *type_id, const char *prefix_name)
        : m_enum_type_def(std::make_shared<EnumTypeDef>(enum_def, type_id, prefix_name))
    {
    }

    bool PyEnumData::exists() const
    {
        assert(m_enum_type_def);
        using EnumFactory = db0::object_model::EnumFactory;
        if (m_enum_ptr) {
            return true;
        }
        
        if (!PyToolkit::getPyWorkspace().hasWorkspace()) {
            // unable to check without a workspace
            return false;
        }

        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        std::uint64_t fixture_uuid = 0;
        if (m_enum_type_def->hasPrefix()) {
            if (!workspace.hasFixture(m_enum_type_def->getPrefixName())) {
                return false;
            }
            fixture_uuid = workspace.getFixture(
                m_enum_type_def->getPrefixName().c_str(), AccessType::READ_ONLY)->getUUID();
        }
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(fixture_uuid, AccessType::READ_ONLY);
        const auto &enum_factory = fixture->get<EnumFactory>();
        return enum_factory.tryGetExistingEnum(*m_enum_type_def) != nullptr;
    }
    
    const Enum &PyEnumData::get() const
    {
        using EnumFactory = db0::object_model::EnumFactory;
        if (!m_enum_ptr) {
            std::uint64_t fixture_uuid = 0;
            if (m_enum_type_def->hasPrefix()) {
                auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(
                    m_enum_type_def->getPrefixName().c_str(), 
                    AccessType::READ_ONLY
                );
                fixture_uuid = fixture->getUUID();
            }
            auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(fixture_uuid, AccessType::READ_ONLY);
            const auto &enum_factory = fixture->get<EnumFactory>();
            // use empty module name since it's unknown
            m_enum_ptr = enum_factory.getExistingEnum(*m_enum_type_def);
            // popluate enum's value cache
            for (auto &value: m_enum_ptr->getValues()) {
                m_enum_ptr->getLangValue(value);
            }
        }
        assert(m_enum_ptr);
        return *m_enum_ptr;
    }
    
    Enum *PyEnumData::tryCreate()
    {
        using EnumFactory = db0::object_model::EnumFactory;
        if (!m_enum_ptr) {
            std::uint64_t fixture_uuid = 0;
            auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
            if (m_enum_type_def->hasPrefix()) {
                // try retrieving an already opened fixture / prefix
                auto fixture = workspace.tryGetFixture(m_enum_type_def->getPrefixName().c_str());
                if (!fixture) {
                    return nullptr;
                }
                fixture_uuid = fixture->getUUID();
            }
            // either get a specific or current prefix (must already be opened)
            auto fixture = workspace.tryGetFixture(fixture_uuid);
            if (!fixture) {
                return nullptr;
            }
            auto &enum_factory = fixture->get<EnumFactory>();
            // use empty module name since it's unknown
            m_enum_ptr = enum_factory.tryGetOrCreateEnum(*m_enum_type_def);
            if (m_enum_ptr) {
                // popluate enum's value cache
                for (auto &value: m_enum_ptr->getValues()) {
                    m_enum_ptr->getLangValue(value);
                }
            }
        }
        return m_enum_ptr.get();
    }
    
    Enum &PyEnumData::create()
    {
        auto enum_ptr = tryCreate();
        if (!enum_ptr) {
            THROWF(db0::InputException) << "Unable to create enum: " << *m_enum_type_def;
        }
        return *enum_ptr;
    }
    
    void PyEnumData::close() {
        m_enum_ptr = nullptr;
    }
    
    void PyEnumData::makeNew(void *at_ptr, const EnumDef &enum_def, const char *type_id, const char *prefix_name) {
        new(at_ptr) PyEnumData(enum_def, type_id, prefix_name);
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
    
}