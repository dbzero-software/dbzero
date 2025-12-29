// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "uuid.hpp"
#include <random>

namespace db0

{
    
    std::uint64_t make_UUID()
    {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<std::uint64_t> dis;        
        return dis(gen);
    }
    
}