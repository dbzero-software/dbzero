#pragma once

#include <memory>
#include "Class.hpp"
#include <dbzero/object_model/config.hpp>

namespace db0::object_model

{
    
    // This is accessor class for class field definitions
    class ClassFields
    {
    public:
        using Member = Class::Member;
        using LangToolkit = Config::LangToolkit;
        using TypeObjectPtr = LangToolkit::TypeObjectPtr;
        using TypeObjectSharedPtr = LangToolkit::TypeObjectSharedPtr;

        ClassFields() = default;
        ClassFields(TypeObjectPtr lang_type);
        
        // deferred initialization
        void init(TypeObjectPtr lang_type);

        // Get existing member by name or throw exception
        Member get(const char *field_name) const;
        
    private:
        TypeObjectSharedPtr m_lang_type;
        // cached DBZ class
        mutable std::shared_ptr<Class> m_type;
    };
    
}