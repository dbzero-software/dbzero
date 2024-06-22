#include "PyEnumType.hpp"
#include "PyEnum.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>

namespace db0::python

{

    PyEnumData::PyEnumData(const EnumDef &enum_def, const char *type_id, const char *prefix_name)
        : m_enum_def(enum_def)
        , m_type_id(type_id ? std::optional<std::string>(type_id) : std::nullopt)
        , m_prefix_name(prefix_name ? std::optional<std::string>(prefix_name) : std::nullopt)
    {
    }

    bool PyEnumData::exists() const
    {
        using EnumFactory = db0::object_model::EnumFactory;
        if (m_enum_ptr) {
            return true;
        }
        
        auto &workspace = PyToolkit::getPyWorkspace().getWorkspace();
        std::uint64_t fixture_uuid = 0;
        if (m_prefix_name) {
            if (!workspace.hasFixture(*m_prefix_name)) {
                return false;
            }    
            fixture_uuid = workspace.getFixture((*m_prefix_name).c_str(), AccessType::READ_ONLY)->getUUID();            
        }
        auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(fixture_uuid, AccessType::READ_ONLY);
        const auto &enum_factory = fixture->get<EnumFactory>();
        return enum_factory.tryGetExistingEnum(m_enum_def, m_type_id ? m_type_id->c_str() : nullptr) != nullptr;
    }

    const Enum &PyEnumData::get() const
    {
        using EnumFactory = db0::object_model::EnumFactory;
        if (!m_enum_ptr) {
            std::uint64_t fixture_uuid = 0;
            if (m_prefix_name) {
                auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture((*m_prefix_name).c_str(), AccessType::READ_ONLY);
                fixture_uuid = fixture->getUUID();
            }
            auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(fixture_uuid, AccessType::READ_ONLY);
            const auto &enum_factory = fixture->get<EnumFactory>();
            // use empty module name since it's unknown
            m_enum_ptr = enum_factory.getExistingEnum(m_enum_def, m_type_id ? m_type_id->c_str() : nullptr);
            // popluate enum's value cache
            for (auto &value: m_enum_ptr->getValues()) {
                m_enum_ptr->getLangValue(value);
            }
        }
        assert(m_enum_ptr);
        return *m_enum_ptr;
    }
    
    Enum &PyEnumData::create()
    {
        using EnumFactory = db0::object_model::EnumFactory;
        if (!m_enum_ptr) {
            std::uint64_t fixture_uuid = 0;
            if (m_prefix_name) {
                auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture((*m_prefix_name).c_str());
                fixture_uuid = fixture->getUUID();
            }
            auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getFixture(fixture_uuid, AccessType::READ_ONLY);
            auto &enum_factory = fixture->get<EnumFactory>();
            // use empty module name since it's unknown
            m_enum_ptr = enum_factory.getOrCreateEnum(m_enum_def, m_type_id ? m_type_id->c_str() : nullptr);
            // popluate enum's value cache
            for (auto &value: m_enum_ptr->getValues()) {
                m_enum_ptr->getLangValue(value);
            }
        }
        assert(m_enum_ptr);
        return *m_enum_ptr;
    }    
    
    void PyEnumData::close() {
        m_enum_ptr = nullptr;
    }

    void PyEnumData::makeNew(void *at_ptr, const EnumDef &enum_def, const char *type_id, const char *prefix_name) {
        new(at_ptr) PyEnumData(enum_def, type_id, prefix_name);
    }
    
}