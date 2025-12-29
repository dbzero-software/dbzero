// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SubClass.hpp"

namespace tests

{

    SubClass::SubClass(db0::swine_ptr<Fixture> &fixture, const std::string &name, std::optional<std::string> module_name,
        const char *type_id, const char *prefix_name, const std::vector<std::string> &init_vars, ClassFlags flags,
        std::shared_ptr<Class> base_class)
        : Class(fixture, name, module_name, type_id, prefix_name, init_vars, flags, base_class)
    {
        // to prevent premature removal
        this->incRef(false);
        this->incRef(false);
    }
    
    std::shared_ptr<Class> getTestClass(db0::swine_ptr<Fixture> &fixture)
    {
        return std::shared_ptr<Class>(new SubClass(
            fixture, "TestObject", std::nullopt, "test.object", "test_prefix", {}, ClassFlags(), nullptr)
        );
    }

}