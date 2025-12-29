// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ByteUtils.hpp"

void set_bytes(std::uint64_t &number, int start_byte, int n_bytes, std::uint64_t value){
    value = value & (0xFFFFFFFFFFFFFFFF >> (64 - n_bytes));
    number = number & ~(0xFFFFFFFFFFFFFFFF >> (64 - n_bytes) << start_byte);
    number = number | (value << start_byte);
}

std::uint64_t get_bytes(std::uint64_t &number, int start_byte, int n_bytes){
    return (number >> start_byte) & (0xFFFFFFFFFFFFFFFF >> (64 - n_bytes));
}