#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <deque>
#include <functional>
#include <optional>
#include <atomic>
#include <dbzero/core/memory/ResourceLock.hpp>
#include <dbzero/core/utils/FixedList.hpp>

namespace db0

{

	class CacheRecycler
    {
	public :
		static constexpr std::size_t DEFAULT_FLUSH_SIZE = 128 << 20u;
		
		/**
		 * Holds resource locks and recycles based on LRU policy
         * 
		 * @param size as the number of bytes
		 * @param flush_size recommended number of bytes to be released in single operation
		 * @param flush_dirty function to request releasing specific number of bytes from dirty locks (i.e. converting to non-dirty)
		 * @param flush_callback to be notified on each flush operation (with indication if sufficient space was released)
		 */
		CacheRecycler(std::size_t size, const std::atomic<std::size_t> &dirty_meter, std::optional<std::size_t> flush_size = {},
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
         * Modify cache size
         * @param new_size as byte count
         */
        void resize(std::size_t new_size);

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

		std::size_t getCapacity() const;
		
		/**
		 * Execute f over each stored resource lock
		*/
		void forEach(std::function<void(std::shared_ptr<ResourceLock>)>) const;
		
	private :
        using list_t = db0::FixedList<std::shared_ptr<ResourceLock> >;
        using iterator = list_t::iterator;

		list_t m_res_buf;
		std::size_t m_current_size = 0;
		// cache capacity as number of bytes
		std::size_t m_capacity;
		const std::atomic<std::size_t> &m_dirty_meter;
		// number of locks to be flushed at once
		std::size_t m_flush_size;
		mutable std::mutex m_mutex;
		std::function<void(std::size_t limit)> m_flush_dirty;
		std::function<bool(bool)> m_flush_callback;
		std::pair<bool, bool> m_last_flush_callback_result = {true, false};
		
        /**
         * Adjusts cache size after updates, collect locks to unlock (can be unlocked off main thread)
         * @param released_locks locks to be released
		 * @param release_size total number of bytes to be released
         */
        void adjustSize(std::unique_lock<std::mutex> &, std::size_t release_size);		
		void updateSize(std::unique_lock<std::mutex> &, std::size_t expected_size);
	};

}