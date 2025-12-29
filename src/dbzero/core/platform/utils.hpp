// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

// Define PACKED macro that works across compilers
#if defined(_MSC_VER)  // Microsoft Visual C++
    #define PACKED_STRUCT(definition) __pragma(pack(push, 1)) definition __pragma(pack(pop))
#elif defined(__GNUC__) || defined(__clang__)  // GCC, Clang
    #define PACKED_STRUCT(definition) definition __attribute__((__packed__))
#else
    #error "Compiler not supported"
#endif