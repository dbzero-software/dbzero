// SPDX-License-Identifier: AGPL-3.0-or-later
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