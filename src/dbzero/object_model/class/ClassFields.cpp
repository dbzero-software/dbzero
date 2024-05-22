#include "ClassFields.hpp"
#include "ClassFactory.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/Fixture.hpp>

namespace db0::object_model

{
    
    ClassFields::ClassFields(TypeObjectPtr lang_type)
        : m_lang_type(lang_type)
    {
    }
    
    void ClassFields::init(TypeObjectPtr lang_type) {
        m_lang_type = lang_type;
    }
    
    FieldDef ClassFields::get(const char *field_name) const
    {
        if (!m_type) {
            auto fixture = LangToolkit::getPyWorkspace().getWorkspace().getMutableFixture();
            auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
            // find py type associated DBZero class with the ClassFactory
            m_type = class_factory.getOrCreateType(m_lang_type.get(), nullptr);
        }
        
        return { m_type->getUID(), m_type->get(m_type->findField(field_name)) };
    }

}