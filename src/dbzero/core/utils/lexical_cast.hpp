// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <iostream>

namespace db0

{

    template <typename OutputT, typename InputT> OutputT lexical_cast(const InputT &input) {
        std::stringstream ss;
        ss << input;
        OutputT output;
        ss >> output;
        return output;
    }

    // identity cast
    template <typename OutputT> OutputT lexical_cast(const OutputT &input) {
        return input;
    }

}