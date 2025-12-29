// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ChronoMeter.hpp"

namespace db0

{

    ChronoMeter::ChronoMeter(bool is_paused)
        : m_paused(is_paused)
        , m_start_time(std::chrono::high_resolution_clock::now())
    {
    }

    double ChronoMeter::getSeconds() const
    {
        double result = m_total;
        if (!m_paused)
        {
            auto t = std::chrono::high_resolution_clock::now();
            result += (double)std::chrono::duration_cast<std::chrono::nanoseconds>(t - m_start_time).count()
                    / (double)NANOSECONDS_PER_SECOND;
        }
        return result;
    }

    void ChronoMeter::pause()
    {
        if (!m_paused)
        {
            auto t = std::chrono::high_resolution_clock::now();
            m_total += (double)std::chrono::duration_cast<std::chrono::nanoseconds>(t - m_start_time).count()
                    / (double)NANOSECONDS_PER_SECOND;
            m_paused = true;
        }
    }

    void ChronoMeter::start()
    {
        if (m_paused)
        {
            m_start_time = std::chrono::high_resolution_clock::now();
            m_paused = false;
        }
    }

    ChronoMeter& ChronoMeter::operator+=(const ChronoMeter&other)
    {
        m_total += other.m_total;
        return *this;
    }
    
}