#pragma once

#include "PyWrapper.hpp"
#include "PyTypes.hpp"
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
        std::optional<std::string> m_prefix_name;
        mutable std::shared_ptr<Enum> m_enum_ptr;

        PyEnumData(const EnumDef &enum_def, const char *type_id, const char *prefix_name);

        // tryCreate may fail if enum if first accessed and prefix is not opened for read/write
        Enum *tryCreate();
        // when first accessed, tries pulling existing or creating a new enum in the current fixture
        Enum &create();

        // get existing enum
        const Enum &get() const;        

        void close();

        // check if the underlying enum instance exists
        bool exists() const;

        bool hasValue(const char *value) const;
                
        static void makeNew(void *at_ptr, const EnumDef &enum_def, const char *type_id, 
            const char *prefix_name);
    };
    
    using PyEnum = PyWrapper<PyEnumData, false>;

}