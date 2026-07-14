// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/bindings/python/PyToolkit.hpp> 

namespace db0::object_model

{

    // Language-specific configuration
    struct LangConfig
    {
        using LangToolkit = db0::python::PyToolkit;
        using ObjectPtr = LangToolkit::ObjectPtr;
        using ObjectSharedPtr = LangToolkit::ObjectSharedPtr;
    };

}
