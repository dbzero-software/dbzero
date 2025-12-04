// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <string>
#include <sstream>

namespace db0

{

    template <typename T> std::string to_string(const T &value)
    {
        std::ostringstream oss;
        oss << value;
        return oss.str();
    } 

}
