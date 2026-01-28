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

    const std::shared_ptr<Class>& ClassFields::get_type() const
    {
        if (!m_type) {
            auto fixture = LangToolkit::getPyWorkspace().getWorkspace().getFixture(
                LangToolkit::getFixtureUUID(m_lang_type.get()), AccessType::READ_ONLY
            );
            auto &class_factory = fixture->get<db0::object_model::ClassFactory>();
            // find py type associated dbzero class with the ClassFactory
            m_type = class_factory.getOrCreateType(m_lang_type.get());
        }
        return m_type;
    }
    
    FieldDef ClassFields::get(const char *field_name) const
    {
        auto &type = get_type();
        return {type->getUID(), type->getMember(field_name)};
    }

    std::optional<FieldDef> ClassFields::try_get(const char *field_name) const
    {
        auto &type = get_type();
        auto member = type->tryGetMember(field_name);
        if (!member) {
            return std::nullopt;
        }
        return std::make_optional<FieldDef>({type->getUID(), *member});
    }

}