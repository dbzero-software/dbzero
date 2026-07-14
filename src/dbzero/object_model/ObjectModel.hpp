// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <functional>
#include <dbzero/core/memory/swine_ptr.hpp>

namespace db0 

{ 
        
    class Fixture;
    
}

namespace db0::object_model

{

    /**
     * Retrieve the model specific fixture initializer.
    */
    std::function<void(db0::swine_ptr<Fixture> &, bool is_new, bool is_read_only, bool is_snapshot)> initializer();
    
}