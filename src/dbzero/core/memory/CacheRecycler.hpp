// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <deque>
#include <functional>
#include <optional>
#include <atomic>
#include <chrono>
#include <dbzero/core/memory/ResourceLock.hpp>
#include <dbzero/core/utils/FixedList.hpp>

namespace db0

{

	class CacheRecycler
    {
	public:
		static constexpr std::size_t DEFAULT_FLUSH_SIZE = 256u << 20;
		static constexpr std::int64_t INITIAL_FLUSH_DELAY_NS = 1'000; // 1us
		static constexpr std::int64_t MAX_FLUSH_DELAY_NS = 1'000'000'000; // 1 second
		
		/**
		 * Holds resource locks and recycles based on LRU policy
         * 
		 * @param capactity cahe capacity as the number of bytes
		 * @param flush_size recommended number of bytes to be released in single operation
		 * @param flush_dirty function to request releasing specific number of bytes from dirty locks (i.e. converting to non-dirty)
		 * @param flush_callback to be notified on each flush operation (with indication if sufficient space was released)
		 */
		CacheRecycler(std::size_t capacity, const std::atomic<std::size_t> &dirty_meter, std::optional<std::size_t> flush_size = {},
			std::function<void(std::size_t limit)> flush_dirty = {},
			std::function<bool(bool threshold_reached)> flush_callback = {});

		void update(std::shared_ptr<ResourceLock> res_lock);
        
		/**
		 * Release specified resource from cache
		 */
        void release(ResourceLock &, std::unique_lock<std::mutex> &);

		/**
		 * Release all managed resources
		 * Note that only locks with no active references are released
		 */
		void clear();
		
        /**
         * Change cache capacity at runtime
         * @param new_capacity as byte size
         */
        void resize(std::size_t new_capacity);

		void setFlushSize(unsigned int);

        /**
         * Acquire lock of the entire instance
        */
        std::unique_lock<std::mutex> lock() const {
            return std::unique_lock<std::mutex>(m_mutex);
        }
		
		/**
		 * Get current cache utilization
		*/
		std::size_t size() const;
		
		// @return current cache size with a by-priority breakdown
		std::vector<std::size_t> getDetailedSize() const;

		std::size_t getCapacity() const;
		
		/**
		 * Execute f over each stored resource lock
		*/
		void forEach(std::function<void(std::shared_ptr<ResourceLock>)>) const;
		
	private:
        using list_t = db0::FixedList<std::shared_ptr<ResourceLock> >;
        using iterator = list_t::iterator;
		
		// total cache capacity
		std::size_t m_capacity;
		// buffers for priority cache (#0) and secondary cache (#1)
		std::array<list_t, 2> m_res_bufs;
		std::array<std::size_t, 2> m_current_size = {0, 0};
		const std::atomic<std::size_t> &m_dirty_meter;
		// number of locks to be flushed at once
		std::size_t m_flush_size;
		mutable std::mutex m_mutex;
		std::function<void(std::size_t limit)> m_flush_dirty;
		std::function<bool(bool)> m_flush_callback;
		std::pair<bool, bool> m_last_flush_callback_result = {true, false};
		
		// Flush rate limiting
		std::chrono::high_resolution_clock::time_point m_next_flush_time{};
		std::chrono::nanoseconds m_current_flush_delay{0};
		
		void resize(std::unique_lock<std::mutex> &, std::size_t new_size, int priority);        
		/**
         * Adjusts cache size after updates, collect locks to unlock (can be unlocked off main thread)
         * @param released_locks locks to be released
		 * @param release_size total number of bytes to be released
		 * @return number of bytes actually released
         */
        std::size_t adjustSize(std::unique_lock<std::mutex> &, list_t &res_buf, std::size_t release_size);
		void adjustSize(std::unique_lock<std::mutex> &, std::size_t release_size);
		void updateSize(std::unique_lock<std::mutex> &, int priority, std::size_t expected_size);
		// update overall size
		void updateSize(std::unique_lock<std::mutex> &, std::size_t expected_size);

		inline std::size_t getCurrentSize() const {
			return m_current_size[0] + m_current_size[1];
		}

		std::pair<bool, bool> _flush(std::unique_lock<std::mutex> &, int priority);
	};

}