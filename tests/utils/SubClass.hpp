// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/object_model/class/Class.hpp>

namespace tests

{
    
    using namespace db0;
    using namespace db0::object_model;

    // this is to expose protected constructor for tests
    class SubClass: public Class
    {
    public:
        SubClass(db0::swine_ptr<Fixture> &fixture, const std::string &name, std::optional<std::string> module_name,
            const char *type_id, const char *prefix_name, const std::vector<std::string> &init_vars, ClassFlags flags,
            std::shared_ptr<Class> base_class);
    };
    
    // constructs a mocked type
    std::shared_ptr<Class> getTestClass(db0::swine_ptr<Fixture> &fixture);
    
}


