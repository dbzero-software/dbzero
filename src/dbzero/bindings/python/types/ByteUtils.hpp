#pragma once
#include <cstdint>

void set_bytes(std::uint64_t &number, int start_byte, int n_bytes, std::uint64_t value);

std::uint64_t get_bytes(std::uint64_t &number, int start_byte, int n_bytes);
