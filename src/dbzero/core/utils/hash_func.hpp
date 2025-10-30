#pragma once
#include <cstdint>
#include <cstring>
#include <iostream>

namespace db0

{

    std::uint64_t murmurhash64A(const void* key, size_t len, std::uint64_t seed = 0);

}