// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>
#include "Class.hpp"
#include <dbzero/object_model/LangConfig.hpp>

namespace db0::object_model

{
    
    struct FieldDef
    {
        std::uint32_t m_class_uid;
        Class::Member m_member;
    };
    
    // This is accessor class for class field definitions
    class ClassFields
    {
    public:
        using Member = Class::Member;
        using LangToolkit = LangConfig::LangToolkit;
        using TypeObjectPtr = LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = LangToolkit::TypeObjectSharedPtr;

        ClassFields() = default;
        
        /**
         * @param lang_type must be a Memo type object
         */
        ClassFields(TypeObjectPtr lang_type);
        
        // deferred initialization
        void init(TypeObjectPtr lang_type);

        // Get existing member by name or throw exception
        FieldDef get(const char *field_name) const;
        
    private:
        TypeObjectSharedPtr m_lang_type;
        // cached DBZ class
        mutable std::shared_ptr<Class> m_type;
    };
    
}