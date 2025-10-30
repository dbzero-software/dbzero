#pragma once

#include <vector>

namespace db0::tests

{

    // op-code, realm_id, capacity, slab id
    std::vector<std::tuple<int, int, int, int> > getCPData();
    
}