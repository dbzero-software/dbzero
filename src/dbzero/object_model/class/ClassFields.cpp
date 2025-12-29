// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ClassFields.hpp"
#include "ClassFactory.hpp"
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/workspace/Fixture.hpp>
#include <dbzero/object_model/ObjectBase.hpp>

namespace db0::object_model

{
    
    ClassFields::ClassFields(TypeObjectPtr lang_type)
        : m_lang_type(lang_type)
    {
        if (!LangToolkit::isAnyMemoType(lang_type)) {
            THROWF(db0::InputException) << "Expected Memo type object";
        }
    }
    
    void ClassFields::init(TypeObjectPtr lang_type)
    {
        if (!LangToolkit::isAnyMemoType(lang_type)) {
            THROWF(db0::InputException) << "Expected Memo type object";
        }
        m_lang_type = lang_type;
    }
    
    FieldDef ClassFields::get(const char *field_name) const
    {
        if (!m_type) {
            auto fixture = LangToolkit::getPyWorkspace().getWorkspace().getFixture(
                LangToolkit::getFixtureUUID(m_lang_type.get()), AccessType::READ_ONLY
            );
            auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
            // find py type associated dbzero class with the ClassFactory
            m_type = class_factory.getOrCreateType(m_lang_type.get());
        }
        
        auto field_id = std::get<0>(m_type->findField(field_name)).primary().first;
        return { m_type->getUID(), m_type->getMember(field_id) };
    }
    
}