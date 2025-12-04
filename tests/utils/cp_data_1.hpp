// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <vector>

namespace db0::tests

{

    // op-code, realm_id, capacity, slab id
    std::vector<std::tuple<int, int, int, int> > getCPData();
    
}