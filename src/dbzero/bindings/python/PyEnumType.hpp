#pragma once

#include "PyWrapper.hpp"
#include <dbzero/object_model/enum/EnumDef.hpp>
#include <optional>

namespace db0::object_model {
    class Enum;    
}

namespace db0::python

{

    using Enum = db0::object_model::Enum;
    using EnumDef = db0::object_model::EnumDef;

    // must store EnumDef for deferred creation
    struct PyEnumData
    {
        EnumDef m_enum_def;
        std::optional<std::string> m_type_id;
        std::shared_ptr<Enum> m_enum_ptr;

        PyEnumData(const EnumDef &enum_def, const char *type_id);

        // when first accessed, tries pulling existing or creating a new enum in the current fixture
        Enum &operator*();
        Enum *operator->();

        void close();
        
        static void makeNew(void *at_ptr, const EnumDef &enum_def, const char *type_id = nullptr);
    };
    
    using PyEnum = PyWrapper<PyEnumData>;

}