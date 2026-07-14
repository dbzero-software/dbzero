// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "conversions.hpp"

namespace db0

{

    std::optional<std::string> getOptionalString(const char *str)
    {
        if (!str) {
            return std::nullopt;
        }
        return std::string(str);
    }

}