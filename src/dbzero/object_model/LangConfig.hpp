// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

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
