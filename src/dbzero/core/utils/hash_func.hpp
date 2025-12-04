// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
#include <cstdint>
#include <cstring>
#include <iostream>

namespace db0

{

    std::uint64_t murmurhash64A(const void* key, size_t len, std::uint64_t seed = 0);

}