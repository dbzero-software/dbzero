// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#pragma once

#include <vector>

namespace db0::tests

{

    // op-code, realm_id, capacity, slab id
    std::vector<std::tuple<int, int, int, int> > getCPData();
    
}