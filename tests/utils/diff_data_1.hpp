// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>

namespace db0::tests

{

    // page number / state number / storage page number
    std::vector<std::tuple<int, int, int> > getDiffIndexData1();
    
}