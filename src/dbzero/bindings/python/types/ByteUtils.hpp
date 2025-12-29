// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once
#include <cstdint>

void set_bytes(std::uint64_t &number, int start_byte, int n_bytes, std::uint64_t value);

std::uint64_t get_bytes(std::uint64_t &number, int start_byte, int n_bytes);
