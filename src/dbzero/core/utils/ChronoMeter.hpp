// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <chrono>

namespace db0 

{

	//defined as double to avoid conversion
	static const double NANOSECONDS_PER_SECOND      = 1000000000;
	static const double NANOSECONDS_PER_MILLISECOND = 1000000;
	static const double MICROSECONDS_PER_SECOND     = 1000000;
	static const double SECONDS_PER_MINUTE          = 60;
	static const double MILLISECONDS_PER_SECOND     = 1000;
	
	/**
	 * Measure elapsed time in seconds
	 */
	class ChronoMeter
	{
		double m_total = 0.0;
		bool m_paused;
		std::chrono::high_resolution_clock::time_point m_start_time;
	public :
		ChronoMeter(bool is_paused = false);

		/**
         * @return number of seconds elapsed from time of this object construction (excluding time when paused)
         */
		double getSeconds() const;

		/**
         * Pause timer
         */
		void pause();

		/**
         * Restart timer after it has been paused
         */
		void start();

		/**
         * Add to total time measured by some other instance
         */
		ChronoMeter &operator+=(const ChronoMeter&);
	};
    
}
