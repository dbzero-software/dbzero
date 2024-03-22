#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <deque>
#include <functional>
#include <dbzero/core/memory/ResourceLock.hpp>
#include <dbzero/core/utils/FixedList.hpp>

namespace db0

{

	class CacheRecycler
    {
	public :

		/**
		 * Holds resource locks and recycles based on LRU policy
         * 
		 * @param size as the number of bytes
		 * @param flush_size recommended number of bytes to be released in single operation (0 for default)
		 */
		CacheRecycler(std::size_t size, unsigned int flush_size = 0);

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
         * @param new_size new nubmer of cached pages
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
		// number of locks to be flushed at once
		unsigned int m_flush_size;        
		mutable std::mutex m_mutex;

        /**
         * Adjusts cache size after updates, collect locks to unlock (can be unlocked off main thread)
         * @param released_locks locks to be released
		 * @param release_size total number of bytes to be released
         */
        void adjustSize(std::unique_lock<std::mutex> &, std::vector<std::shared_ptr<ResourceLock> > &released_locks, 
			std::size_t release_size);

		void updateSize(std::unique_lock<std::mutex> &, std::size_t expected_size);
	};

}