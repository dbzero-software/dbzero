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
    using EnumTypeDef = db0::object_model::EnumTypeDef;
    
    // must store EnumDef for deferred creation
    struct PyEnumData
    {
        // shared_ptr to be able to associated this element with EnumValueRepre elemenst
        std::shared_ptr<EnumTypeDef> m_enum_type_def;
        // lazily created enum instance
        mutable std::shared_ptr<Enum> m_enum_ptr;
        
        PyEnumData(const EnumDef &, const char *type_id, const char *prefix_name);
        
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
        
        // get values from their definitions
        const std::vector<std::string> &getValueDefs() const;
        
        static void makeNew(void *at_ptr, const EnumDef &enum_def, const char *type_id, 
            const char *prefix_name);
    };
    
    using PyEnum = PyWrapper<PyEnumData, false>;

}