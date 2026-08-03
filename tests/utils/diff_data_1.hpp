// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <vector>

namespace db0::tests

{

    // page number / state number / storage page number
    std::vector<std::tuple<int, int, int> > getDiffIndexData1();
    
}