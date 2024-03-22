#pragma once

#include <dbzero/bindings/python/PyToolkit.hpp> 

namespace db0::object_model

{

    // Language-specific configuration
    struct Config
    {
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;
    };

}
