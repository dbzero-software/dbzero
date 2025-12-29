// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <chrono>

namespace db0

{

    class ProgressiveMutexDuration {
        std::chrono::microseconds m_duration;

    public:

        typedef std::chrono::microseconds time_type;

        struct microseconds{};
        struct milliseconds{};

    public:
        inline ProgressiveMutexDuration(int64_t, microseconds);
        inline ProgressiveMutexDuration(int64_t, milliseconds);

        inline const std::chrono::microseconds &get() const;
        inline std::chrono::microseconds &get();
    };
    
    inline ProgressiveMutexDuration::ProgressiveMutexDuration(int64_t value, microseconds)
        : m_duration(value)
    {}

    inline ProgressiveMutexDuration::ProgressiveMutexDuration(int64_t value, milliseconds)
        : m_duration(value * 1000)
    {}

    inline const std::chrono::microseconds &ProgressiveMutexDuration::get() const {
        return m_duration;
    }

    inline std::chrono::microseconds &ProgressiveMutexDuration::get() {
        return m_duration;
    }

}