// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

/**
 * Cross-platform compiler attribute support
 * 
 * This header provides macros for commonly used compiler-specific attributes
 * that need to work across different platforms and compilers.
 */

// Cross-platform packed attribute support
#if defined(_MSC_VER)
    // MSVC uses pragma pack
    #define DB0_PACKED_BEGIN __pragma(pack(push, 1))
    #define DB0_PACKED_END __pragma(pack(pop))
    #define DB0_PACKED_ATTR
#elif defined(__GNUC__) || defined(__clang__)
    // GCC and Clang support [[gnu::packed]]
    #define DB0_PACKED_BEGIN
    #define DB0_PACKED_END
    #define DB0_PACKED_ATTR [[gnu::packed]]
#else
    // Fallback for other compilers
    #define DB0_PACKED_BEGIN
    #define DB0_PACKED_END
    #define DB0_PACKED_ATTR
    #warning "Packed attribute not supported on this compiler"
#endif

// Alternative macro for single-line usage (legacy compatibility)
#if defined(_MSC_VER)
    #define DB0_PACKED(declaration) \
        __pragma(pack(push, 1)) \
        declaration \
        __pragma(pack(pop))
#elif defined(__GNUC__) || defined(__clang__)
    #define DB0_PACKED(declaration) declaration __attribute__((packed))
#else
    #define DB0_PACKED(declaration) declaration
    #warning "Packed attribute not supported on this compiler"
#endif

// Additional compiler-specific attributes can be added here as needed
// For example:
// - Force inline
// - Alignment
// - Deprecated warnings
// etc.

#if defined(_MSC_VER)
    #define DB0_FORCE_INLINE __forceinline
#elif defined(__GNUC__) || defined(__clang__)
    #define DB0_FORCE_INLINE __attribute__((always_inline)) inline
#else
    #define DB0_FORCE_INLINE inline
#endif