// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "string_compare.hpp"

namespace db0

{

    bool iequals(const std::string &a, const std::string &b)
    {
        return std::equal(a.begin(), a.end(), b.begin(), b.end(), [](char a, char b) {
            return tolower(a) == tolower(b);
        });
    }

}
