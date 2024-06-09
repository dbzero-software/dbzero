#include "PyEnumType.hpp"
#include "PyEnum.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>

namespace db0::python

{

    PyEnumData::PyEnumData(const EnumDef &enum_def, const char *type_id)
        : m_enum_def(enum_def)
        , m_type_id(type_id ? std::optional<std::string>(type_id) : std::nullopt)
    {
    }

    Enum &PyEnumData::operator*()
    {
        using EnumFactory = db0::object_model::EnumFactory;
        if (!m_enum_ptr) {
            auto fixture = PyToolkit::getPyWorkspace().getWorkspace().getMutableFixture();        
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
    
    Enum *PyEnumData::operator->() {
        return &**this;
    }

    void PyEnumData::close() {
        m_enum_ptr = nullptr;
    }

    void PyEnumData::makeNew(void *at_ptr, const EnumDef &enum_def, const char *type_id) {
        new(at_ptr) PyEnumData(enum_def, type_id);
    }
    
}